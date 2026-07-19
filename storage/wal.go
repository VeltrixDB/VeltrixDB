package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// walRespPool avoids a heap allocation per WAL append call by pooling the
// buffered error-response channels.  At 500K writes/s this eliminates
// 500K channel allocations per second and the associated GC pressure.
var walRespPool = sync.Pool{
	New: func() any { c := make(chan error, 1); return &c },
}

// WriteAheadLog provides crash durability via group-commit fdatasync.
//
// Group commit (windowed mode, flushWindow > 0):
//
//	When the first WAL entry arrives the flusher starts a timer for
//	flushWindow. Every subsequent entry that arrives before the timer fires
//	is added to the same batch. When the timer fires (or maxBatch is reached)
//	the flusher writes all entries in one sequential pass and calls fdatasync
//	exactly once, then unblocks every waiting caller simultaneously.
//
// Group commit (immediate mode, flushWindow == 0):
//
//	Legacy behaviour: drain the channel once, flush immediately. Still
//	batches concurrent writers — just doesn't wait for stragglers.
//
// Either way: N concurrent writes → 1 fdatasync instead of N.
//
// Flush window sizing:
//
//	On Linux NVMe, fdatasync costs ~0.2–0.5 ms. A 2 ms window means P99
//	write latency ≈ window + fdatasync = ~2.2 ms while amortising the sync
//	across all writers that arrive during the window.
//
//	On macOS (F_FULLFSYNC ≈ 7–10 ms), the same 2 ms window reduces
//	effective latency from one-fsync-per-write (~10 ms each) to one fsync
//	per window period — measured P99 drops from ~122 ms to ~10 ms at low
//	concurrency and sub-5 ms at ≥8 concurrent writers.
type WriteAheadLog struct {
	file           *os.File
	walPath        string        // absolute path to wal.log (used for truncation on clean close)
	appendCh       chan *walItem
	doneCh         chan struct{}
	walFlushes     *atomic.Uint64 // pointer into StorageMetrics
	bytesWritten   atomic.Uint64
	entriesWritten atomic.Uint64
	// durableBytes is the byte offset of wal.log covered by a completed
	// fdatasync. Because the flusher writes whole entries and syncs whole
	// batches, this offset always lands on an entry boundary — the WAL
	// archiver (pitr.go) uses it as a safe copy boundary. Initialised to the
	// existing file size at open (pre-existing content is durable by
	// definition) and advanced by the flusher AFTER each successful fdatasync.
	durableBytes atomic.Int64
	flushWindow    time.Duration // 0 = flush immediately after channel drain
	maxBatch       int           // max entries per flush (safety cap)
	diskIdx        int           // for log prefixes
}

type walItem struct {
	data []byte
	resp chan error
}

// newWriteAheadLog opens (or creates) the WAL file in walDir and starts the
// flusher goroutine. flushWindow controls the group-commit window duration;
// 0 means flush immediately after draining available channel entries.
// maxBatch caps how many entries accumulate before an early forced flush.
func newWriteAheadLog(
	walDir string,
	walFlushes *atomic.Uint64,
	flushWindow time.Duration,
	maxBatch int,
	diskIdx int,
) (*WriteAheadLog, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("wal dir %s: %w", walDir, err)
	}
	walPath := filepath.Join(walDir, "wal.log")
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	if maxBatch <= 0 {
		maxBatch = 1024
	}

	wal := &WriteAheadLog{
		file:        file,
		walPath:     walPath,
		appendCh:    make(chan *walItem, 4096),
		doneCh:      make(chan struct{}),
		walFlushes:  walFlushes,
		flushWindow: flushWindow,
		maxBatch:    maxBatch,
		diskIdx:     diskIdx,
	}
	// Seed the durable offset with the pre-existing file size (crash-recovery
	// content or a prior checkpoint) so the archiver can copy it too.
	if fi, err := file.Stat(); err == nil {
		wal.durableBytes.Store(fi.Size())
	}

	go wal.flusher()
	return wal, nil
}

// append serialises entry, sends it to the flusher, and blocks until the
// group fdatasync that covers this entry has completed.
func (wal *WriteAheadLog) append(entry *WALEntry) error {
	rp := wal.beginAppend(entry)
	err := <-*rp
	walRespPool.Put(rp)
	return err
}

// beginAppend sends entry to the flusher and returns the response channel
// without blocking.  The caller must read one error value from *rp and then
// call walRespPool.Put(rp).  This lets the caller overlap WAL and VLog
// fdatasyncs: submit both, then wait for both.
func (wal *WriteAheadLog) beginAppend(entry *WALEntry) *chan error {
	data := wal.serialize(entry)
	rp := walRespPool.Get().(*chan error)
	wal.appendCh <- &walItem{data: data, resp: *rp}
	return rp
}

// appendAll sends all entries to the WAL channel without blocking, then waits
// for all responses at once.  Because all N items are enqueued before any
// response is awaited, the WAL flusher sees all N items in a single drain and
// covers them with ONE fdatasync — regardless of the flush window.
//
// This is the correct path for MultiPut: N sequential wal.append calls would
// produce N fdatasyncs; appendAll produces 1.
func (wal *WriteAheadLog) appendAll(entries []*WALEntry) []error {
	if len(entries) == 0 {
		return nil
	}
	type pending struct {
		respPtr *chan error
	}
	ps := make([]pending, len(entries))
	for i, e := range entries {
		rp := walRespPool.Get().(*chan error)
		ps[i] = pending{respPtr: rp}
		wal.appendCh <- &walItem{data: wal.serialize(e), resp: *rp}
	}
	errs := make([]error, len(entries))
	for i, p := range ps {
		errs[i] = <-(*p.respPtr)
		walRespPool.Put(p.respPtr)
	}
	return errs
}

// serializeBufPool avoids per-call allocation for the WAL header line.
// The line is: timestamp|tombstone|key|valueLen|crc32hex|version\n
// For a 16-byte key + typical field widths this is ~60-80 bytes.
var serializeBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 128); return &b }}

func (wal *WriteAheadLog) serialize(entry *WALEntry) []byte {
	bufPtr := serializeBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	// Header: timestamp|tombstone|key|valueLen|crc32hex|version|vlogOffset|packed\n
	// 8-field format. vlogOffset=0 means value bytes follow on the next line
	// (non-KV-sep mode or old-format compatibility). vlogOffset>0 means the
	// value is already durable in the VLog at that offset — no value bytes here.
	// packed='1' means the VLog record at vlogOffset shares its 4 KB block
	// with other records (block packing); packed='0' means it owns a full
	// 4 KB block (legacy unpacked layout). Replay restores FlagPacked from
	// this field. Old 7-field WAL records are still parsed (packed defaults
	// to false).
	buf = strconv.AppendInt(buf, entry.Timestamp, 10)
	buf = append(buf, '|')
	if entry.IsTombstone {
		buf = append(buf, '1')
	} else {
		buf = append(buf, '0')
	}
	buf = append(buf, '|')
	buf = append(buf, entry.Key...)
	buf = append(buf, '|')
	buf = strconv.AppendUint(buf, uint64(entry.ValueLen), 10)
	buf = append(buf, '|')
	buf = strconv.AppendUint(buf, uint64(entry.Checksum), 16)
	buf = append(buf, '|')
	buf = strconv.AppendUint(buf, entry.Version, 10)
	buf = append(buf, '|')
	buf = strconv.AppendInt(buf, entry.VLogOffset, 10)
	buf = append(buf, '|')
	if entry.Packed {
		buf = append(buf, '1')
	} else {
		buf = append(buf, '0')
	}
	buf = append(buf, '\n')

	var result []byte
	// Write value bytes only when the value is NOT stored in VLog (VLogOffset==0)
	// and the entry carries value data (non-tombstone with a non-empty payload).
	if !entry.IsTombstone && len(entry.Value) > 0 && entry.VLogOffset == 0 {
		result = make([]byte, len(buf)+len(entry.Value)+1)
		copy(result, buf)
		copy(result[len(buf):], entry.Value)
		result[len(buf)+len(entry.Value)] = '\n'
	} else {
		result = make([]byte, len(buf))
		copy(result, buf)
	}

	*bufPtr = buf
	serializeBufPool.Put(bufPtr)
	return result
}

// flusher is the single goroutine that owns file I/O for this WAL.
//
// Windowed path (flushWindow > 0):
//   - On the first item of a new batch, start a timer for flushWindow.
//   - Drain all immediately available items into the batch.
//   - If maxBatch is reached before the timer fires, flush early.
//   - When the timer fires, flush whatever has accumulated.
//
// Immediate path (flushWindow == 0):
//   - Same drain-then-flush as the original implementation.
//   - Still batches concurrent writers; just no deliberate wait.
func (wal *WriteAheadLog) flusher() {
	pending := make([]*walItem, 0, 1024)

	flush := func() {
		if len(pending) == 0 {
			return
		}

		var writeErr error
		var batchBytes int64
		for _, item := range pending {
			if writeErr != nil {
				break
			}
			if _, err := wal.file.Write(item.data); err != nil {
				writeErr = err
				continue
			}
			wal.bytesWritten.Add(uint64(len(item.data)))
			wal.entriesWritten.Add(1)
			batchBytes += int64(len(item.data))
		}

		if writeErr == nil {
			writeErr = fdatasync(int(wal.file.Fd()))
		}

		if writeErr == nil && wal.walFlushes != nil {
			wal.walFlushes.Add(1)
		}
		if writeErr == nil {
			// Advance the safe archive boundary only after the fdatasync that
			// covers this batch has completed (single atomic add — never blocks
			// the group-commit hot path).
			wal.durableBytes.Add(batchBytes)
		}
		for _, item := range pending {
			item.resp <- writeErr
		}
		pending = pending[:0]
	}

	// drain drains all immediately available items from appendCh into pending.
	// Returns true if maxBatch was reached (signals an early flush is needed).
	drain := func() bool {
	drainLoop:
		for len(pending) < wal.maxBatch {
			select {
			case item := <-wal.appendCh:
				pending = append(pending, item)
			default:
				break drainLoop
			}
		}
		return len(pending) >= wal.maxBatch
	}

	var (
		timer  *time.Timer
		timerC <-chan time.Time
	)

	startTimer := func() {
		if wal.flushWindow > 0 && timer == nil {
			timer = time.NewTimer(wal.flushWindow)
			timerC = timer.C
		}
	}
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}

	for {
		select {
		case item := <-wal.appendCh:
			pending = append(pending, item)
			// Start window timer on the first entry of a new batch.
			startTimer()
			// Drain whatever is already in the channel without blocking.
			full := drain()

			// Flush immediately if: no window configured, or max batch reached.
			if wal.flushWindow == 0 || full {
				stopTimer()
				flush()
			}
			// Otherwise wait for the timer to fire (more writers may arrive).

		case <-timerC:
			// Window expired — flush everything that accumulated.
			timer = nil
			timerC = nil
			flush()

		case <-wal.doneCh:
			// Shutdown: stop timer, drain channel, flush final batch.
			stopTimer()
		drainFinal:
			for {
				select {
				case item := <-wal.appendCh:
					pending = append(pending, item)
				default:
					break drainFinal
				}
			}
			flush()
			return
		}
	}
}

// durableOffset returns the number of bytes of wal.log covered by a completed
// fdatasync. Everything below this offset consists of whole, durable entries —
// the WAL archiver reads [archived, durableOffset) with an independent
// read-only file handle. See pitr.go.
func (wal *WriteAheadLog) durableOffset() int64 {
	return wal.durableBytes.Load()
}

// GetStats returns cumulative WAL I/O counters for observability.
// bytesWritten is raw serialized bytes flushed to the file (includes WAL framing).
// entriesWritten is the total number of WALEntry records written.
func (wal *WriteAheadLog) GetStats() (bytesWritten, entriesWritten uint64) {
	return wal.bytesWritten.Load(), wal.entriesWritten.Load()
}

func (wal *WriteAheadLog) close() error {
	close(wal.doneCh)
	return wal.file.Close()
}

// checkpoint writes a compacted WAL (one record per live key) via an atomic
// rename so keys survive a clean restart.  Must be called after close().
func (wal *WriteAheadLog) checkpoint(index *shardedIndex, numDisks int, kvSep bool, version uint64) error {
	return writeWALCheckpoint(wal.walPath, index, wal.diskIdx, numDisks, kvSep, version)
}
