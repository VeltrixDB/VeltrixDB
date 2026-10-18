package storage

// wal_checkpoint_live.go — checkpointing a WAL that is open and serving
// writes (CLAUDE.md invariant 21).
//
// # The bug this replaces
//
// StorageEngine.Checkpoint (admin POST /checkpoint, every online backup,
// CheckpointManager) used the shutdown checkpoint as is: write the compacted
// file, rename it over wal.log. The flusher kept its fd on the old inode, so
// every write acknowledged after the checkpoint went to an unlinked file and
// was gone after a crash restart — only a clean Close, which checkpoints from
// the index again, hid it.
//
// # The design
//
// The checkpoint is the old file's prefix replaced by a compacted image of
// the index, with the old file's suffix carried over byte for byte:
//
//	new wal.log = [ one record per live key at version V, the raft marker ]
//	            + old wal.log [fenceOff, end)
//
// 1. Fence (in the flusher, between batches): fenceOff = the old file's
//    length; flip the write epoch. Every write registers in the current
//    epoch (enterWrite) BEFORE it is given a version or enqueues its WAL
//    record, and leaves it (exitWrite) AFTER it has updated the index. So a
//    record below fenceOff belongs to a write of the old epoch.
// 2. Drain: wait until no write of the old epoch is in flight. Now every
//    record below fenceOff has reached the index (writes update the index
//    only after their record is durable, so without this wait a record could
//    be in the old prefix and not yet in the index the build reads).
// 3. Build the compacted records from the index into wal.log.ckpt. Writes
//    continue meanwhile; their records go to the old file, past fenceOff.
// 4. Copy the old file's [fenceOff, durable) into wal.log.ckpt and
//    fdatasync it — still without blocking writes.
// 5. Pause (in the flusher, between batches): copy the rest of the tail,
//    fdatasync wal.log.ckpt, rename it over wal.log, fsync the directory and
//    make the new file the flusher's write handle. Writes resume into it.
//
// Replay of the new file gives, per key, the index state at the build, then
// every record written after the fence in its original order — the same
// result as replaying the whole old file, because the index at the build
// already reflects every record below the fence (step 2) and replay applies
// a record only over an entry that is not newer (replayPut / replayMark-
// Tombstone compare write timestamps).
//
// # Crash safety
//
// The rename is the commit point. Before it wal.log is the old file,
// untouched and complete: the flusher never writes anything but appends, and
// at the rename it is paused, so every acknowledged record is in the old file
// and in the tail copied into the new one. A crash before the rename leaves
// wal.log.ckpt, which nothing reads. After the rename wal.log holds the
// compacted image plus the complete tail; the directory is fsync'd before
// the first write is acknowledged into the new file, so a write acknowledged
// after the checkpoint can never be in a file the directory forgets.
//
// # Cost
//
// The build (O(keys)), most of the tail copy and the fdatasync of both run
// with writes flowing; the only stall is step 5 — the tail written since
// step 4, an fdatasync of just that, a rename and a directory fsync —
// measured per checkpoint in
// LastCheckpointStats. Group commit and the one-write(2)-per-batch flusher
// (invariants 41–42) are unchanged: the pause is one more case in the
// flusher's select, taken between batches.
//
// Multi-disk: every disk's WAL is checkpointed on its own, one after the
// other, at the same version V. Raft mode: V is the version of the newest
// applied-index marker and every disk's checkpoint carries that marker
// (raft_applied.go), so the checkpoint cuts exactly where that marker cut
// before — records above it stay above it.

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Steps of a runtime checkpoint, as passed to the test hook. Fenced, built
// and tail-copied run while the flusher serves writes; synced, renamed and
// swapped run inside the flusher's pause.
const (
	ckptStepFenced     = "fenced"
	ckptStepBuilt      = "built"
	ckptStepTailCopied = "tail-copied"
	ckptStepSynced     = "synced"
	ckptStepRenamed    = "renamed"
	ckptStepSwapped    = "swapped"
)

// walDrainTimeout bounds the wait for in-flight writes at the fence. A write
// in flight is waiting on its own fdatasync, so this is never reached by a
// healthy disk; on expiry the checkpoint fails and wal.log is untouched.
var walDrainTimeout = 30 * time.Second

var errWALClosed = errors.New("wal: closed")

// walCtl is a function the flusher runs between two batches.
type walCtl struct {
	fn   func() error
	done chan error
}

// walSwap records one runtime checkpoint's file swap for the PITR archiver:
// the replaced file ended at oldEnd; the new file's first newStart bytes are
// the compacted records and the carried tail (both already archived from the
// old file, or not archivable); reader is a read handle on the new file.
type walSwap struct {
	oldEnd   int64
	newStart int64
	reader   *os.File
}

// walCkptStats describes the last runtime checkpoint of one WAL.
type walCkptStats struct {
	build     time.Duration // compacted-record build, writes flowing
	stall     time.Duration // the flusher pause (step 5)
	tailBytes int64         // bytes carried from the old file
	newLen    int64         // new wal.log length at the swap
}

// enterWrite registers a write in the current epoch. Call it before the
// write takes a version or enqueues a WAL record on this WAL, and call
// exitWrite with the result after the write has updated the index (also on
// failure). Two atomic loads and one add when no checkpoint is running.
func (wal *WriteAheadLog) enterWrite() uint64 {
	for {
		e := wal.wepoch.Load()
		wal.wactive[e&1].Add(1)
		if wal.wepoch.Load() == e {
			return e
		}
		// A fence flipped the epoch in between: count in the new one, so the
		// fence never waits for a write whose record is after it.
		wal.wactive[e&1].Add(-1)
	}
}

func (wal *WriteAheadLog) exitWrite(e uint64) { wal.wactive[e&1].Add(-1) }

// waitWriters waits until no write registered in epoch e is in flight.
func (wal *WriteAheadLog) waitWriters(e uint64) error {
	slot := &wal.wactive[e&1]
	deadline := time.Now().Add(walDrainTimeout)
	for spins := 0; slot.Load() > 0; spins++ {
		if spins < 64 {
			runtime.Gosched()
			continue
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wal disk=%d: %d writes still in flight after %v", wal.diskIdx, slot.Load(), walDrainTimeout)
		}
		time.Sleep(50 * time.Microsecond)
	}
	return nil
}

// inFlusher runs fn in the flusher goroutine between two batches and returns
// its error. Writes queue (and their callers wait) while fn runs.
func (wal *WriteAheadLog) inFlusher(fn func() error) error {
	c := walCtl{fn: fn, done: make(chan error, 1)}
	select {
	case wal.ctlCh <- c:
	case <-wal.flusherDone:
		return errWALClosed
	}
	return <-c.done
}

// liveCkptResult is what checkpointLive reports for one disk.
type liveCkptResult struct {
	// fenceVersion: every write with a version ≤ it is in the compacted
	// records (it entered the epoch the fence drained).
	fenceVersion uint64
	// ckptLen is the length of the compacted records (the new file's prefix
	// before the carried tail); snap, when asked for, is a read handle on the
	// new file, so [0, ckptLen) can be copied after later checkpoints.
	ckptLen int64
	snap    *os.File
	stats   walCkptStats
}

// checkpointLive checkpoints this WAL while it serves writes — see the file
// comment. version is the version of every compacted record; extra (the raft
// marker) is written after them; curVersion reads the engine's version
// counter. On error before the rename wal.log is untouched.
func (wal *WriteAheadLog) checkpointLive(index *shardedIndex, numDisks int, kvSep bool, version uint64, extra []*WALEntry, curVersion func() uint64, wantSnap bool) (res liveCkptResult, err error) {
	wal.ckptMu.Lock()
	defer wal.ckptMu.Unlock()
	hook := func(step string) error {
		if wal.ckptHook == nil {
			return nil
		}
		return wal.ckptHook(wal.diskIdx, step)
	}

	// 1. Fence.
	var fenceOff int64
	var oldEpoch uint64
	if err := wal.inFlusher(func() error {
		res.fenceVersion = curVersion() // before the flip: see liveCkptResult
		oldEpoch = wal.wepoch.Add(1) - 1
		fi, err := wal.file.Stat()
		if err != nil {
			return err
		}
		fenceOff = fi.Size()
		return nil
	}); err != nil {
		return res, fmt.Errorf("wal checkpoint fence: %w", err)
	}
	// 2. Drain the writes that may have a record below the fence.
	if err := wal.waitWriters(oldEpoch); err != nil {
		return res, fmt.Errorf("wal checkpoint: %w", err)
	}
	if err := hook(ckptStepFenced); err != nil {
		return res, err
	}

	// 3. Build.
	tmpPath := wal.walPath + ".ckpt"
	f, err := os.Create(tmpPath)
	if err != nil {
		return res, fmt.Errorf("wal checkpoint create: %w", err)
	}
	swapped := false
	defer func() {
		if !swapped {
			f.Close()
			os.Remove(tmpPath)
			if res.snap != nil {
				res.snap.Close()
				res.snap = nil
			}
		}
	}()
	if wantSnap {
		if res.snap, err = os.Open(tmpPath); err != nil {
			return res, fmt.Errorf("wal checkpoint snapshot handle: %w", err)
		}
	}
	t0 := time.Now()
	if err := writeCheckpointRecords(f, index, wal.diskIdx, numDisks, kvSep, version, wal.legacyText, wal.dropTTL, wal.keepTombstone, extra...); err != nil {
		return res, fmt.Errorf("wal checkpoint write: %w", err)
	}
	res.stats.build = time.Since(t0)
	if res.ckptLen, err = f.Seek(0, io.SeekCurrent); err != nil {
		return res, fmt.Errorf("wal checkpoint: %w", err)
	}
	if err := hook(ckptStepBuilt); err != nil {
		return res, err
	}

	// 4. Carry the tail written so far, writes still flowing. Only a
	// checkpoint (holding ckptMu) replaces wal.log, so this is the file the
	// flusher appends to; bytes below durableOffset are written and final.
	src, err := os.Open(wal.walPath)
	if err != nil {
		return res, fmt.Errorf("wal checkpoint: open wal: %w", err)
	}
	defer src.Close()
	copied := fenceOff
	if d := wal.durableOffset(); d > copied {
		if err := copyWALRange(f, src, copied, d); err != nil {
			return res, fmt.Errorf("wal checkpoint tail: %w", err)
		}
		copied = d
	}
	// Sync the compacted records and the tail so far now, outside the pause:
	// the pause's fdatasync then covers only the tail written since.
	if err := f.Sync(); err != nil {
		return res, fmt.Errorf("wal checkpoint sync: %w", err)
	}
	if err := hook(ckptStepTailCopied); err != nil {
		return res, err
	}

	// 5. The swap, between two batches.
	var dirErr error
	err = wal.inFlusher(func() error {
		start := time.Now()
		defer func() { res.stats.stall = time.Since(start) }()
		fi, err := wal.file.Stat()
		if err != nil {
			return err
		}
		end := fi.Size()
		if end > copied {
			if err := copyWALRange(f, src, copied, end); err != nil {
				return fmt.Errorf("tail: %w", err)
			}
		}
		res.stats.tailBytes = end - fenceOff
		if err := f.Sync(); err != nil {
			return err
		}
		if err := hook(ckptStepSynced); err != nil {
			return err
		}
		newLen, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if err := os.Rename(tmpPath, wal.walPath); err != nil {
			return err
		}
		// Committed: wal.log is the new file, so it must become the write
		// handle whatever happens next — appending to the old inode is the
		// bug this file exists to fix.
		swapped = true
		_ = hook(ckptStepRenamed)
		// The rename must be durable before a write is acknowledged into the
		// new file. A failure is reported, but the swap stands.
		dirErr = fsyncDir(filepath.Dir(wal.walPath))
		old := wal.file
		wal.file = f // positioned at newLen; only the flusher writes it
		wal.publishSwap(end, newLen)
		old.Close()
		res.stats.newLen = newLen
		_ = hook(ckptStepSwapped)
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("wal checkpoint swap: %w", err)
	}
	st := res.stats
	wal.lastCkpt.Store(&st)
	if dirErr != nil {
		log.Printf("[wal] disk=%d checkpoint: fsync of %s failed after the rename: %v — a power cut may revert wal.log to the pre-checkpoint file and lose writes acknowledged since",
			wal.diskIdx, filepath.Dir(wal.walPath), dirErr)
		return res, fmt.Errorf("wal checkpoint: fsync dir: %w", dirErr)
	}
	return res, nil
}

// copyWALRange appends src[from, to) to dst.
func copyWALRange(dst io.Writer, src *os.File, from, to int64) error {
	n, err := io.Copy(dst, io.NewSectionReader(src, from, to-from))
	if err != nil {
		return err
	}
	if n != to-from {
		return fmt.Errorf("short copy: %d of %d bytes at offset %d", n, to-from, from)
	}
	return nil
}

// fsyncDir makes a rename in dir durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// publishSwap records a swap for the archiver (flusher goroutine only).
func (wal *WriteAheadLog) publishSwap(oldEnd, newStart int64) {
	wal.swapMu.Lock()
	defer wal.swapMu.Unlock()
	wal.fileGen.Add(1) // odd: durableBytes is being moved to the new file
	wal.durableBytes.Store(newStart)
	if wal.swapWatch {
		r, err := os.Open(wal.walPath)
		if err != nil {
			log.Printf("[wal] disk=%d checkpoint: cannot open the new wal.log for the WAL archiver: %v", wal.diskIdx, err)
			r = nil
		}
		wal.swaps = append(wal.swaps, walSwap{oldEnd: oldEnd, newStart: newStart, reader: r})
	}
	wal.fileGen.Add(1)
}

// ── PITR archiver interface (pitr.go) ────────────────────────────────────────

// openFollower opens a read handle on the current wal.log for the archiver
// and starts recording swaps. It returns the swap count the handle is at.
func (wal *WriteAheadLog) openFollower() (*os.File, uint64, error) {
	wal.ckptMu.Lock() // no swap between the open and the count
	defer wal.ckptMu.Unlock()
	r, err := os.Open(wal.walPath)
	if err != nil {
		return nil, 0, err
	}
	wal.swapMu.Lock()
	wal.swapWatch = true
	gen := wal.fileGen.Load() / 2
	wal.swapMu.Unlock()
	return r, gen, nil
}

// durableSnapshot returns the swap count and the durable offset of the file
// that count names, read consistently.
func (wal *WriteAheadLog) durableSnapshot() (gen uint64, durable int64) {
	for {
		g := wal.fileGen.Load()
		if g&1 == 1 {
			runtime.Gosched()
			continue
		}
		d := wal.durableBytes.Load()
		if wal.fileGen.Load() == g {
			return g / 2, d
		}
	}
}

// peekSwap returns the oldest swap the archiver has not consumed.
func (wal *WriteAheadLog) peekSwap() (walSwap, bool) {
	wal.swapMu.Lock()
	defer wal.swapMu.Unlock()
	if len(wal.swaps) == 0 {
		return walSwap{}, false
	}
	return wal.swaps[0], true
}

// dropSwap consumes the oldest swap (its reader now belongs to the caller).
func (wal *WriteAheadLog) dropSwap() {
	wal.swapMu.Lock()
	defer wal.swapMu.Unlock()
	if len(wal.swaps) > 0 {
		wal.swaps[0] = walSwap{}
		wal.swaps = wal.swaps[1:]
	}
}

// stopFollowing stops recording swaps and closes unconsumed handles.
func (wal *WriteAheadLog) stopFollowing() {
	wal.swapMu.Lock()
	defer wal.swapMu.Unlock()
	wal.swapWatch = false
	for _, s := range wal.swaps {
		if s.reader != nil {
			s.reader.Close()
		}
	}
	wal.swaps = nil
}

// ── Engine level ─────────────────────────────────────────────────────────────

// CheckpointStats describes the last runtime checkpoint (Checkpoint or an
// online backup) across all disks.
type CheckpointStats struct {
	Disks int
	// Build is the longest per-disk compacted-record build; writes flow
	// during it (only the shard being read is briefly read-locked).
	Build time.Duration
	// MaxStall is the longest per-disk flusher pause: the time writes to
	// that disk queued behind the swap (tail copy + fdatasync + rename +
	// directory fsync).
	MaxStall time.Duration
	// TailBytes is the WAL written during the checkpoint and carried over.
	TailBytes int64
}

// LastCheckpointStats reports the most recent runtime checkpoint of each
// disk (zero before the first one).
func (se *StorageEngine) LastCheckpointStats() CheckpointStats {
	var cs CheckpointStats
	for _, w := range se.wals {
		st := w.lastCkpt.Load()
		if st == nil {
			continue
		}
		cs.Disks++
		if st.build > cs.Build {
			cs.Build = st.build
		}
		if st.stall > cs.MaxStall {
			cs.MaxStall = st.stall
		}
		cs.TailBytes += st.tailBytes
	}
	return cs
}

// checkpointVersion is the version every checkpoint record is written at and
// the records the checkpoint carries after its keys.
//
// Raft mode (an applied-index marker exists): the newest marker's own
// version, and that marker. The checkpoint then cuts exactly where the
// marker did: records written after the marker keep versions above it, in
// the carried tail, and stay cut at a crash until a later marker covers them;
// and the version is never inside a dead range (raft_applied.go) — a
// checkpoint written at a dead range's upper bound, which se.version is right
// after a cutting open, had every record dropped at the next open.
//
// Otherwise: a fresh version, above every record already in any WAL.
func (se *StorageEngine) checkpointVersion() (uint64, []*WALEntry) {
	if v, rec, ok := se.applied.checkpointCut(); ok {
		return v, []*WALEntry{rec}
	}
	return se.version.Add(1), nil
}

// liveCheckpoint is a runtime checkpoint of every disk.
type liveCheckpoint struct {
	disks []liveCkptResult
	// fenceVersion: every write with a version ≤ it is in the compacted
	// records of its disk (the minimum over the disks' fences).
	fenceVersion uint64
}

func (lc *liveCheckpoint) close() {
	for i := range lc.disks {
		if lc.disks[i].snap != nil {
			lc.disks[i].snap.Close()
			lc.disks[i].snap = nil
		}
	}
}

// checkpointAll checkpoints every disk's WAL while the engine serves writes.
// wantSnap keeps a read handle on each new file (the backup copies from it).
// Disks are done one after the other; on error the disks already done keep
// their new file and the rest their old one — both are complete.
func (se *StorageEngine) checkpointAll(wantSnap bool) (*liveCheckpoint, error) {
	version, extra := se.checkpointVersion()
	numDisks := len(se.wals)
	kvSep := se.config.KeyValueSeparation
	lc := &liveCheckpoint{disks: make([]liveCkptResult, numDisks)}
	for i, w := range se.wals {
		r, err := w.checkpointLive(se.index, numDisks, kvSep, version, extra, se.version.Load, wantSnap)
		if err != nil {
			lc.close()
			return nil, fmt.Errorf("disk %d: %w", i, err)
		}
		lc.disks[i] = r
		if i == 0 || r.fenceVersion < lc.fenceVersion {
			lc.fenceVersion = r.fenceVersion
		}
	}
	st := se.LastCheckpointStats()
	log.Printf("[checkpoint] complete  disks=%d  keys=%d  version=%d  build=%s  max_stall=%s  tail=%dB",
		numDisks, se.index.size(), version, st.Build.Round(time.Microsecond), st.MaxStall.Round(time.Microsecond), st.TailBytes)
	return lc, nil
}

// checkpointForBackup is Checkpoint for an online backup: it runs through the
// raft gate like Checkpoint and keeps a read handle on each new wal.log.
func (se *StorageEngine) checkpointForBackup() (*liveCheckpoint, error) {
	if gate := se.checkpointGate(); gate != nil {
		var lc *liveCheckpoint
		var err error
		gate(func() { lc, err = se.checkpointAll(true) })
		return lc, err
	}
	return se.checkpointAll(true)
}

// setCheckpointTestHook installs fn as every WAL's checkpoint step hook
// (nil removes it).
func (se *StorageEngine) setCheckpointTestHook(fn func(diskIdx int, step string) error) {
	for _, w := range se.wals {
		w.ckptMu.Lock()
		w.ckptHook = fn
		w.ckptMu.Unlock()
	}
}
