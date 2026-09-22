package storage

import (
	"fmt"
	"log"
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
//	NOTE: on macOS this is plain fsync(2) (fdatasync_other.go), measured at
//	~0.02 ms because Darwin's fsync returns at the drive cache without
//	flushing it. F_FULLFSYNC, which would flush it, costs ~3 ms and is NOT
//	called anywhere — so a macOS build is not power-loss safe, and macOS
//	write timings do not predict Linux. The window still reduces P99 at low
//	concurrency and sub-5 ms at ≥8 concurrent writers.
type WriteAheadLog struct {
	file     *os.File
	walPath  string // absolute path to wal.log (used for truncation on clean close)
	appendCh chan *walItem
	doneCh   chan struct{}
	// flusherDone is closed by the flusher goroutine as it returns. close()
	// waits on it before closing the file: the flusher's shutdown branch
	// writes and fdatasyncs one final batch, so closing the fd first races
	// with it and would fail those writes with "file already closed".
	flusherDone    chan struct{}
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
	flushWindow  time.Duration // 0 = flush immediately after channel drain
	maxBatch     int           // max entries per flush (safety cap)
	diskIdx      int           // for log prefixes
}

type walItem struct {
	data []byte
	// rec is the pooled backing buffer for data. The flusher returns it to
	// walRecPool once it has copied the bytes into the batch buffer, which is
	// the last read of them.
	rec  *[]byte
	resp chan error
}

// walItemPool reuses the per-entry envelope. One walItem was allocated for
// every entry in every batch; at 1024-entry MultiPut that is 1024 allocations
// per call for objects that die as soon as the flusher answers their waiter.
// The flusher returns them after the response send, which is the last read.
var walItemPool = &sync.Pool{New: func() any { return &walItem{} }}

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
		flusherDone: make(chan struct{}),
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
	rec := wal.serialize(entry)
	rp := walRespPool.Get().(*chan error)
	item := walItemPool.Get().(*walItem)
	item.rec = rec
	item.data = *rec
	item.resp = *rp
	wal.appendCh <- item
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
		item := walItemPool.Get().(*walItem)
		item.rec = wal.serialize(e)
		item.data = *item.rec
		item.resp = *rp
		wal.appendCh <- item
	}
	errs := make([]error, len(entries))
	for i, p := range ps {
		errs[i] = <-(*p.respPtr)
		walRespPool.Put(p.respPtr)
	}
	return errs
}

// walRecPool holds one serialized WAL record. The record is built directly in
// this buffer and handed to the flusher, which copies it into the batch buffer
// and returns it here. Previously serialize borrowed a scratch buffer, then
// allocated a second one to return — one guaranteed allocation per entry, i.e.
// 1024 per 1024-entry MultiPut.
//
// 160 B covers a 10-field header for a ~16 B key without growing.
var walRecPool = sync.Pool{New: func() any { b := make([]byte, 0, 160); return &b }}

// serialize renders entry into a pooled buffer and returns that buffer. The
// caller must hand the pointer to the flusher, which owns returning it.
func (wal *WriteAheadLog) serialize(entry *WALEntry) *[]byte {
	bufPtr := walRecPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	// Header: timestamp|tombstone|key|valueLen|crc32hex|version|vlogOffset|packed|diskLen|xflags\n
	// 10-field format. vlogOffset=0 means value bytes follow on the next line
	// (non-KV-sep mode or old-format compatibility). vlogOffset>0 means the
	// value is already durable in the VLog at that offset — no value bytes here.
	// packed='1' means the VLog record at vlogOffset shares its 4 KB block
	// with other records (block packing); packed='0' means it owns a full
	// 4 KB block (legacy unpacked layout). Replay restores FlagPacked from
	// this field. Old 7-field WAL records are still parsed (packed defaults
	// to false).
	//
	// diskLen is the on-disk blob length (post-compression, post-encryption)
	// and xflags (hex) carries FlagCompressed|FlagEncrypted. valueLen stays
	// the PLAINTEXT length. Replay needs all three to rebuild an IndexEntry
	// whose ValueSize matches what ReadValue must pull off disk and whose
	// flags tell the read path to decrypt/decompress. Records written before
	// these fields existed parse with diskLen=valueLen and xflags=0, which is
	// correct for them: they were written by a build that stored values
	// untransformed on this path.
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
	// Fields 9-10 are emitted ONLY when they carry information, i.e. when a
	// transform actually applied. An untransformed record has
	// diskLen == valueLen and xflags == 0, which is exactly what the parser
	// defaults to for a shorter record, so omitting them is lossless.
	//
	// This is deliberate rollback insurance. The 10-field form cannot be read
	// correctly by a pre-fix binary, so emitting it unconditionally would make
	// every record written after an upgrade un-rollback-able. Emitting it only
	// for transformed records shrinks that blast radius to exactly the records
	// an older binary would mishandle anyway (it is the bug being fixed), and
	// leaves deployments with compression and encryption off fully
	// rollback-compatible.
	diskLen := entry.DiskValueLen
	if diskLen == 0 {
		diskLen = entry.ValueLen // no transform applied — on-disk == plaintext
	}
	if entry.XformFlags != 0 || diskLen != entry.ValueLen {
		buf = append(buf, '|')
		buf = strconv.AppendUint(buf, uint64(diskLen), 10)
		buf = append(buf, '|')
		buf = strconv.AppendUint(buf, uint64(entry.XformFlags), 16)
	}
	buf = append(buf, '\n')

	// Write value bytes only when the value is NOT stored in VLog (VLogOffset==0)
	// and the entry carries value data (non-tombstone with a non-empty payload).
	if !entry.IsTombstone && len(entry.Value) > 0 && entry.VLogOffset == 0 {
		buf = append(buf, entry.Value...)
		buf = append(buf, '\n')
	}

	*bufPtr = buf
	return bufPtr
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
	defer close(wal.flusherDone)
	pending := make([]*walItem, 0, 1024)

	// batch is the coalescing buffer: every record in a group-commit batch is
	// concatenated here and written with ONE write(2).
	//
	// This used to be one write(2) per entry, with the single fdatasync after
	// them. Group commit therefore amortised the sync but not the writes, and
	// at 8 concurrent 1024-key MultiPuts that left ~8K syscalls per batch on
	// the table. Profiling the concurrent batch path measured syscall.write at
	// 27% of total CPU against syscall.Fsync at 1.2% — the sync was never the
	// expensive half.
	//
	// Durability is unchanged: identical bytes, identical order, still exactly
	// one fdatasync covering the batch before any waiter is answered.
	batch := make([]byte, 0, 64<<10)

	flush := func() {
		if len(pending) == 0 {
			return
		}

		batch = batch[:0]
		for _, item := range pending {
			batch = append(batch, item.data...)
		}

		var writeErr error
		batchBytes := int64(len(batch))
		if len(batch) > 0 {
			if _, err := wal.file.Write(batch); err != nil {
				writeErr = err
			}
		}
		if writeErr == nil {
			wal.bytesWritten.Add(uint64(batchBytes))
			wal.entriesWritten.Add(uint64(len(pending)))
		}

		if writeErr == nil {
			writeErr = fdatasync(int(wal.file.Fd()))
		}

		// Give the record buffers back now that their bytes are in `batch`.
		for _, item := range pending {
			if item.rec != nil {
				walRecPool.Put(item.rec)
				item.rec = nil
			}
		}

		// A batch that grew far beyond the steady-state size is not worth
		// holding onto between flushes.
		if cap(batch) > 4<<20 {
			batch = make([]byte, 0, 64<<10)
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
			// Last read of item — the waiter only takes the value from the
			// channel, never the envelope. Safe to recycle.
			item.data = nil
			item.resp = nil
			walItemPool.Put(item)
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
	// Bounded for the same reason as VLog.close — see closeFlusherGrace.
	// Unbounded, a flusher stuck writing to a full response channel would
	// deadlock shutdown permanently.
	select {
	case <-wal.flusherDone: // final batch written and fdatasync'd
	case <-time.After(closeFlusherGrace):
		log.Printf("[wal] disk=%d flusher did not exit within %v — closing fd anyway; "+
			"the final batch may be lost", wal.diskIdx, closeFlusherGrace)
	}
	return wal.file.Close()
}

// checkpoint writes a compacted WAL (one record per live key) via an atomic
// rename so keys survive a clean restart.  Must be called after close().
func (wal *WriteAheadLog) checkpoint(index *shardedIndex, numDisks int, kvSep bool, version uint64) error {
	return writeWALCheckpoint(wal.walPath, index, wal.diskIdx, numDisks, kvSep, version)
}
