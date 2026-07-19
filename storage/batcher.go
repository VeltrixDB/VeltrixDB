package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// batchFlushBytes — flush when accumulated payload reaches 2 MB.
	// At 400-500K writes/s (128B values) this fills in ~5ms, matching batchFlushDur.
	// Larger batches mean fewer MultiPut calls → fewer VLogBatcher.Flush() calls
	// → fewer fdatasyncs per second per disk.
	batchFlushBytes = 2 << 20

	// batchFlushCount — flush when the batch accumulates 4096 requests.
	// At 400K writes/s with 8 NVMe disks: 4096 entries / 8 disks = 512 VLog
	// Stage calls per disk per Flush → single fdatasync covers 512 values.
	// CGO pool pre-sized to match (see batchBufPool below).
	batchFlushCount = 4096

	// batchFlushDur — 15 ms collection window.  At the target write rate
	// (~14 K writes/s/disk × 8 disks) the timer is the primary flush trigger
	// and collects ~200 entries per batch, reducing CGO transitions and
	// fdatasyncs by 3× vs the old 5 ms window.  The byte/count ceiling remains
	// the early-flush safety valve under burst traffic.
	// P99 ≈ batchFlushDur + WALFlushWindowMs + fdatasync ≈ 15+5+0.2 = ~20 ms.
	batchFlushDur = 15 * time.Millisecond

	// batchChanDepth — at 500K writes/s the channel fills in ~130 ms at this
	// depth.  65536 entries gives the batcher goroutine ~130 ms of burst headroom
	// before the synchronous fallback path is triggered.
	batchChanDepth = 65536
)

// writeReq is a single queued write (fire-and-forget).
type writeReq struct {
	key   string
	value []byte
	ttl   int32
}

// batchBuffer is a reusable accumulator obtained from batchBufPool.
// reset() clears it without releasing the underlying slice capacity.
type batchBuffer struct {
	reqs  []MultiPutRequest
	bytes int // total key+value payload bytes accumulated so far
}

// batchBufPool eliminates per-flush heap allocation for the request slice.
// Pre-sized to batchFlushCount so the slice never grows on the heap.
var batchBufPool = sync.Pool{
	New: func() any {
		return &batchBuffer{reqs: make([]MultiPutRequest, 0, batchFlushCount)}
	},
}

// WriteBatcher accumulates individual Put requests into vectorized batches and
// flushes them asynchronously via engine.MultiPut (Go path) or the CGO batch
// engine (Linux+CGO path).
//
// Flush is triggered when:
//   - Accumulated payload reaches batchFlushBytes (128 KB), OR
//   - batchFlushDur (500 µs) elapses since the buffer was last empty.
//
// Client calls return as soon as the request is enqueued (< 1 µs on an
// unsaturated channel).  WAL persistence and index update happen in the
// batcher goroutine — this is a fire-and-forget durability model where
// acknowledgement is implicit once the WAL group-commit fsyncs the entry.
//
// If the internal channel is saturated, the write is executed synchronously
// (engine.Put) so no entry is ever silently dropped; the Dropped counter
// records how often this fallback path was taken.
type WriteBatcher struct {
	engine  *StorageEngine
	ch      chan writeReq
	stopCh  chan struct{}
	wg      sync.WaitGroup
	Dropped atomic.Uint64 // synchronous fallbacks due to channel saturation
}

func newWriteBatcher(se *StorageEngine) *WriteBatcher {
	wb := &WriteBatcher{
		engine: se,
		ch:     make(chan writeReq, batchChanDepth),
		stopCh: make(chan struct{}),
	}
	wb.wg.Add(1)
	go wb.run()
	return wb
}

// Enqueue queues a write and returns immediately.
// Falls back to synchronous engine.Put only when the channel is full.
func (wb *WriteBatcher) Enqueue(key string, value []byte, ttl int32) {
	select {
	case wb.ch <- writeReq{key: key, value: value, ttl: ttl}:
	default:
		wb.Dropped.Add(1)
		_ = wb.engine.Put(key, value, ttl)
	}
}

// Stop drains remaining requests, flushes them, and waits for the goroutine
// to exit.  Must be called exactly once during engine shutdown.
func (wb *WriteBatcher) Stop() {
	close(wb.stopCh)
	wb.wg.Wait()
}

func (wb *WriteBatcher) run() {
	defer wb.wg.Done()

	timer := time.NewTimer(batchFlushDur)
	defer timer.Stop()

	buf := batchBufPool.Get().(*batchBuffer)

	for {
		select {
		// ── New write request ────────────────────────────────────────────
		case req := <-wb.ch:
			buf.reqs = append(buf.reqs, MultiPutRequest{
				Key: req.key, Value: req.value, TTL: req.ttl,
			})
			buf.bytes += len(req.key) + len(req.value)

			// Non-blocking drain: pull every request already in the channel
			// without waiting for new arrivals.  Stop when the byte or count
			// ceiling is reached to bound the CGO call payload to 1024 entries.
		drainLoop:
			for buf.bytes < batchFlushBytes && len(buf.reqs) < batchFlushCount {
				select {
				case r := <-wb.ch:
					buf.reqs = append(buf.reqs, MultiPutRequest{
						Key: r.key, Value: r.value, TTL: r.ttl,
					})
					buf.bytes += len(r.key) + len(r.value)
				default:
					break drainLoop
				}
			}

			// Flush when either the byte or the entry-count ceiling is hit.
			if buf.bytes >= batchFlushBytes || len(buf.reqs) >= batchFlushCount {
				wb.flush(buf)
				buf = batchBufPool.Get().(*batchBuffer)
				resetBatchTimer(timer, batchFlushDur)
			}

		// ── Time-triggered flush ─────────────────────────────────────────
		case <-timer.C:
			if len(buf.reqs) > 0 {
				wb.flush(buf)
				buf = batchBufPool.Get().(*batchBuffer)
			}
			timer.Reset(batchFlushDur)

		// ── Shutdown ─────────────────────────────────────────────────────
		case <-wb.stopCh:
			// Drain any requests enqueued between the last flush and Stop().
		shutdownDrain:
			for {
				select {
				case req := <-wb.ch:
					buf.reqs = append(buf.reqs, MultiPutRequest{
						Key: req.key, Value: req.value, TTL: req.ttl,
					})
				default:
					break shutdownDrain
				}
			}
			if len(buf.reqs) > 0 {
				wb.flush(buf)
			} else {
				batchBufPool.Put(buf.reset())
			}
			return
		}
	}
}

// flush sends buf to the storage engine and returns the buffer to the pool.
//
// CGO path (linux, CGO_ENABLED=1):
//
//	The C++ batch engine receives the full request slice via direct pointer
//	passing (zero extra allocation).  It groups by shard and processes each
//	shard group in a dedicated OS thread — up to NumCPU shards in parallel.
//	After the CGO call returns, MultiPut still runs for WAL persistence and
//	Go shardedIndex update (correctness path — both paths are always executed).
//
// Pure-Go path (macOS / CGO_ENABLED=0):
//
//	Only MultiPut runs; behaviour is identical to the current engine.
func (wb *WriteBatcher) flush(buf *batchBuffer) {
	if wb.engine.cgoBatch != nil {
		wb.engine.cgoBatch.batchPutViaCGO(buf.reqs)
	}
	wb.engine.MultiPut(buf.reqs)
	batchBufPool.Put(buf.reset())
}

func (b *batchBuffer) reset() *batchBuffer {
	b.reqs = b.reqs[:0]
	b.bytes = 0
	return b
}

// resetBatchTimer stops the timer safely (draining the channel if needed) and
// resets it to d.  This is the documented correct pattern for time.Timer.Reset.
func resetBatchTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
