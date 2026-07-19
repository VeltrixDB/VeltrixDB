package storage

// vlog.go — Append-only Value Log (WiscKey Key-Value Separation).
//
// One VLog per NVMe disk.  Values are written sequentially to vlog_active.dat,
// completely bypassing the compaction-heavy segment writer.  The IndexEntry
// fields DiskOffset / SegmentID / ValueSize are reinterpreted as a ValuePointer
// into the VLog when KeyValueSeparation is enabled:
//
//	IndexEntry.DiskOffset  → byte offset of the VLog record header
//	IndexEntry.SegmentID   → disk index (which VLog file)
//	IndexEntry.ValueSize   → unpadded value length in bytes
//
// Record format (24-byte header, sector-aligned total for O_DIRECT compatibility):
//
//	Offset  Size  Field
//	  0      4    Magic (0x564C5402 = "VLT\x02")
//	  4      4    ValLen  (value bytes, not including header or padding)
//	  8      4    CRC32C  of value bytes
//	 12      4    Reserved (future: compression flags / schema)
//	 16      8    WriteTimestampUs  (int64, µs since Unix epoch)
//	─── 24 bytes ───
//	 24    (ValLen)  Value bytes
//	 24+V   pad      Zero-bytes to the next 512-byte sector boundary
//
// Write path: Append() serialises concurrent callers via mu, assembles an
// aligned buffer via the shared ioPool, writes via WriteAt, then fdatasyncs.
//
// Read path: ReadValue() issues a single pread via ReadAt.  No locking
// required — ReadAt (pread64) is safe alongside concurrent WriteAt calls.
//
// GC path: when GCRatio() exceeds DefragThreshold, the Defragmenter rewrites
// the VLog: it iterates live index entries, copies their values to a new file,
// updates IndexEntry.DiskOffset atomically, then atomically replaces the file.

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// vhdrOff* are byte offsets within a VLog record header.
const (
	vhdrOffMagic   = 0
	vhdrOffValLen  = 4
	vhdrOffCRC     = 8
	// offset 12: Reserved (4 B)
	vhdrOffWriteUs = 16
)

// diskLatencySampleEvery is the 1-in-N sample rate for per-disk write/read
// latency EWMA updates. 32 keeps CAS pressure low at multi-M ops/sec while
// still giving the slow-disk detector enough signal (≤16 µs sampling lag at
// 2 M/s vs 5× detection threshold at ms scale).
const (
	diskLatencySampleEvery uint64 = 32
	diskLatencySampleMask  uint64 = diskLatencySampleEvery - 1
)

// updateLatencyEWMA does an α=1/8 EWMA update under a CAS loop on the supplied
// atomic. Identical formula to the read-EWMA used for admission control.
func updateLatencyEWMA(slot *atomic.Int64, sampleNs int64) {
	for {
		old := slot.Load()
		next := old - (old >> 3) + (sampleNs >> 3)
		if slot.CompareAndSwap(old, next) {
			return
		}
	}
}

// vlogFlushReq is a single Append() request sent to the group-commit flusher.
type vlogFlushReq struct {
	offset     int64      // byte offset reserved for this record (already written)
	alignedLen int        // bytes written (for totalBytes/liveBytes accounting)
	resp       chan error  // flusher writes the fdatasync result here
}

// vlogRespPool reuses resp channels to avoid make(chan error,1) on every Append.
var vlogRespPool = sync.Pool{New: func() any { ch := make(chan error, 1); return &ch }}

// VLog is an append-only value log on a single NVMe disk.
//
// All records are sector-aligned (512 bytes) so the C++ io_uring O_DIRECT
// reader (vlog.cpp VLogContext) can access them without alignment faults.
// Go-side writes use buffered I/O; the internal group-commit flusher
// amortises fdatasync across all concurrent Append callers — identical to
// the WAL group-commit pattern.  The io_uring storage bridge
// (cgo_bridge_storage_bridge_linux.go) replaces N sequential pwrite calls
// with a single io_uring_submit per batch.
//
// Backing: VLog is normally a regular file (vlog_active.dat) inside diskPath.
// When the engine is constructed with a non-empty RawVLogDevices entry for
// this disk index, the file points at a raw block device (/dev/nvmeXnY)
// instead. In raw mode `rawDevicePath` holds the device node path and GC
// reclaim uses BLKDISCARD; everything else (offsets, alignment, magic) is
// identical so engine code is backing-agnostic.
type VLog struct {
	diskIdx        int
	diskPath       string
	rawDevicePath  string // empty when running on a regular file backing

	mu   sync.Mutex   // serialises WriteAt + end advance only (not fdatasync)
	file *os.File
	end  atomic.Int64 // next free byte offset (always sector-aligned)

	// liveBytes / totalBytes track the GC ratio.
	// liveBytes is decremented by MarkDead when a key is overwritten or deleted.
	liveBytes  atomic.Int64
	totalBytes atomic.Int64

	// writeBytes / readBytes track raw value payload throughput (unpadded, no headers).
	writeBytes atomic.Int64
	readBytes  atomic.Int64

	// Per-disk latency EWMA (α=1/8) in nanoseconds.  Updated by Append + ReadValue
	// at the diskLatencySampleEvery cadence (default 1-in-32) so the CAS loop on
	// these atomics does not pingpong across cores at multi-M-ops/sec throughput.
	// Read by GetSlowDisks() to flag a disk as slow when its EWMA is > 5× the
	// median across all disks — surfaces failing or contended NVMe early without
	// requiring kernel iostat scraping.
	writeLatencyEWMANs atomic.Int64
	readLatencyEWMANs  atomic.Int64
	writeLatencyCtr    atomic.Uint64
	readLatencyCtr     atomic.Uint64

	// punchWatermark is the highest byte offset up to which fallocate
	// PUNCH_HOLE has already been called.  GC advances this after relocating
	// all live entries above it, physically freeing dead disk blocks.
	punchWatermark atomic.Int64

	// bridge is the io_uring storage bridge used by VLogBatcher.Flush on Linux.
	// Nil on macOS dev builds and when CGO_ENABLED=0.
	bridge *cgoStorageBridge

	// Group-commit flusher — mirrors the WAL flusher pattern.
	// Append() sends a vlogFlushReq after WriteAt; the flusher drains pending
	// requests, calls one fdatasync, and replies to all waiters.
	flushCh chan *vlogFlushReq
	doneCh  chan struct{}
}

// newVLog opens (or creates) the VLog file for a single NVMe disk.
// flushWindow is the group-commit window duration (matches WALFlushWindowMs).
// openVLogFile is platform-specific (vlog_linux.go = O_DIRECT, vlog_other.go = buffered).
//
// rawDevicePath: when non-empty, opens that block device with O_DIRECT|O_EXCL
// and uses it as the VLog backing instead of a regular file. The first 4 KB
// of the device is reserved for a RawSuperblock (magic, version, vlog start).
// The diskPath directory is still required (and used) for WAL files and the
// punch watermark.
func newVLog(diskIdx int, diskPath, rawDevicePath string, flushWindow time.Duration) (*VLog, error) {
	if err := os.MkdirAll(diskPath, 0755); err != nil {
		return nil, fmt.Errorf("disk %d: mkdir %s: %w", diskIdx, diskPath, err)
	}

	var (
		file        *os.File
		err         error
		startOffset int64
	)

	if rawDevicePath != "" {
		// ── Raw block-device backing ────────────────────────────────────────
		file, err = openBlockDevice(rawDevicePath)
		if err != nil {
			return nil, fmt.Errorf("disk %d: open raw device %s: %w", diskIdx, rawDevicePath, err)
		}
		// Read or initialise the superblock.
		vlogStart, ok, err := readRawSuperblock(file)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("disk %d: superblock %s: %w", diskIdx, rawDevicePath, err)
		}
		if !ok {
			if err := writeRawSuperblock(file, time.Now().UnixMicro()); err != nil {
				file.Close()
				return nil, fmt.Errorf("disk %d: init superblock %s: %w", diskIdx, rawDevicePath, err)
			}
			vlogStart = RawSuperblockSize
		}
		// On a raw device Stat().Size() returns 0, so we cannot infer the
		// highest used VLog byte from the file system. We start with the
		// minimum (vlogStart, just past the superblock); the engine's startup
		// path then calls SetEndAtLeast(maxIndexOffset) AFTER WAL replay has
		// rebuilt the index, advancing vl.end past every live record. Until
		// that call lands, NO Put has been admitted yet (engine still in
		// constructor) — so the temporary low value is safe.
		//
		// Crash mid-write semantics: if a previous run wrote a record to VLog
		// (fdatasync done) but the WAL didn't fdatasync, the value isn't in
		// the index after replay → its bytes are correctly considered dead and
		// new writes can land on top. The client never received OK for that
		// write, so no durability contract is broken.
		startOffset = vlogStart
		if startOffset < int64(vlogBlockSize) {
			startOffset = int64(vlogBlockSize)
		}
	} else {
		// ── File-based backing (existing path) ──────────────────────────────
		vlogPath := filepath.Join(diskPath, "vlog_active.dat")
		file, err = openVLogFile(vlogPath)
		if err != nil {
			return nil, fmt.Errorf("disk %d: open vlog %s: %w", diskIdx, vlogPath, err)
		}

		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("disk %d: stat vlog: %w", diskIdx, err)
		}

		size := info.Size()
		// Step 1: remove any partial sector at the tail (incomplete crash-mid-write).
		if size%sectorSize != 0 {
			size = size &^ (sectorSize - 1)
			if err := file.Truncate(size); err != nil {
				file.Close()
				return nil, fmt.Errorf("disk %d: vlog truncate to sector boundary: %w", diskIdx, err)
			}
		}
		// Step 2: advance vl.end to the next sector boundary so that all NEW writes
		// start at a vlogBlockSize-aligned offset.  Existing records below this
		// boundary remain readable via the intra-block extraction in ReadValue.
		// The gap (at most vlogBlockSize-1 bytes) is treated as dead space; no
		// DiskOffset ever points into it.
		startOffset = (size + int64(vlogBlockSize) - 1) &^ (int64(vlogBlockSize) - 1)
		// Step 3: enforce a minimum startOffset of vlogBlockSize (4096) so that
		// DiskOffset=0 remains an unambiguous "not yet written" sentinel — matching
		// segment files where offset 0 is always the 64-byte segment header, never
		// a data record.  Without this, the first Append on a new empty VLog would
		// land at offset 0, and the DiskOffset>0 guard in engine.Get would silently
		// skip the VLog read for that key (returning not-found or falling through to
		// a wrong segment-file read).
		if startOffset < int64(vlogBlockSize) {
			startOffset = int64(vlogBlockSize)
		}
	}

	if flushWindow <= 0 {
		flushWindow = 10 * time.Millisecond
	}
	vl := &VLog{
		diskIdx:       diskIdx,
		diskPath:      diskPath,
		rawDevicePath: rawDevicePath,
		file:          file,
		flushCh:       make(chan *vlogFlushReq, 65536),
		doneCh:        make(chan struct{}),
	}
	vl.end.Store(startOffset)
	vl.totalBytes.Store(startOffset)
	vl.liveBytes.Store(startOffset)

	// Restore the punch watermark from the previous session.  The kernel
	// already freed the blocks (PUNCH_HOLE is durable); we only need to
	// update the in-memory watermark so punchDeadHead doesn't re-punch
	// the same range and so GCRatio accounting stays correct.
	if saved := loadVLogWatermark(diskPath); saved > 0 {
		vl.punchWatermark.Store(saved)
	}

	go vl.flusher(flushWindow)
	return vl, nil
}

// Append writes value to the VLog and returns the byte offset of the record.
// It is a thin wrapper over beginAppend that blocks until the group fdatasync
// covering this write completes.
func (vl *VLog) Append(value []byte) (int64, error) {
	offset, rp, err := vl.beginAppend(value)
	if err != nil {
		return 0, err
	}
	if err := <-*rp; err != nil {
		vlogRespPool.Put(rp)
		return 0, fmt.Errorf("disk %d vlog fdatasync: %w", vl.diskIdx, err)
	}
	vlogRespPool.Put(rp)
	return offset, nil
}

// beginAppend does the WriteAt and enqueues the fdatasync request, returning
// the reserved offset and the response channel without blocking on fdatasync.
// The caller must read one error value from *rp and call vlogRespPool.Put(rp).
//
// Lock-free write path: vl.end.Add() atomically reserves [offset, offset+alignedLen).
// Because alignedLen is always a multiple of vlogBlockSize (4096) and startOffset
// is a multiple of vlogBlockSize, every returned offset is 4K-aligned — no mutex
// needed to enforce alignment.  Concurrent pwrite64 (WriteAt) calls to
// non-overlapping ranges on the same file are defined safe by POSIX and the
// Linux kernel, including O_DIRECT.  This eliminates the N×WriteAt_latency
// serialization that previously capped VLog throughput to 1/WriteAt_time.
func (vl *VLog) beginAppend(value []byte) (int64, *chan error, error) {
	startNs := time.Now().UnixNano()

	rawLen := vlogHeaderBytes + len(value)
	alignedLen := (rawLen + vlogBlockSize - 1) &^ (vlogBlockSize - 1)

	buf := vlogIOPool.get(alignedLen)
	defer vlogIOPool.put(buf)

	crc := computeCRC32C(value)
	binary.LittleEndian.PutUint32(buf[vhdrOffMagic:], vlogMagic)
	binary.LittleEndian.PutUint32(buf[vhdrOffValLen:], uint32(len(value)))
	binary.LittleEndian.PutUint32(buf[vhdrOffCRC:], crc)
	binary.LittleEndian.PutUint64(buf[vhdrOffWriteUs:], uint64(time.Now().UnixMicro()))
	copy(buf[vlogHeaderBytes:], value)
	for i := vlogHeaderBytes + len(value); i < alignedLen; i++ {
		buf[i] = 0
	}

	// Atomically claim [offset, offset+alignedLen) — no mutex required.
	// alignedLen is always a multiple of vlogBlockSize so the offset stays
	// 4K-aligned for every caller.
	offset := vl.end.Add(int64(alignedLen)) - int64(alignedLen)

	if _, err := vl.file.WriteAt(buf, offset); err != nil {
		return 0, nil, fmt.Errorf("disk %d vlog writeat offset=%d: %w", vl.diskIdx, offset, err)
	}

	vl.totalBytes.Add(int64(alignedLen))
	vl.liveBytes.Add(int64(alignedLen))
	vl.writeBytes.Add(int64(len(value)))

	rp := vlogRespPool.Get().(*chan error)
	vl.flushCh <- &vlogFlushReq{offset: offset, alignedLen: alignedLen, resp: *rp}

	if vl.writeLatencyCtr.Add(1)&diskLatencySampleMask == 0 {
		updateLatencyEWMA(&vl.writeLatencyEWMANs, time.Now().UnixNano()-startNs)
	}
	return offset, rp, nil
}

// ReadValue reads the value stored at offset in the VLog.
//
// ReadAt (pread64) is used so concurrent Append calls need not hold any lock.
// Both the buffer pointer and read length must satisfy O_DIRECT alignment; the
// aligned buffer is obtained from ioPool as in Append.
//
// CRC32C is re-verified to guard against silent bit-rot between write and read.
func (vl *VLog) ReadValue(offset int64, valueLen uint32) ([]byte, error) {
	if valueLen == 0 {
		return nil, nil
	}

	startNs := time.Now().UnixNano()

	rawLen := vlogHeaderBytes + int(valueLen)

	// Round the read offset DOWN to the nearest 4 KB boundary so the pread is
	// O_DIRECT safe on both XFS (4096-byte fs block) and 4Kn NVMe drives.
	// For all records (written at vlogBlockSize=4096 alignment) intraBlock is
	// always 0.
	alignedOffset := offset &^ (int64(vlogBlockSize) - 1)
	intraBlock := int(offset - alignedOffset)
	readSize := (intraBlock + rawLen + vlogBlockSize - 1) &^ (vlogBlockSize - 1)

	buf := vlogIOPool.get(readSize)
	defer vlogIOPool.put(buf)

	if _, err := vl.file.ReadAt(buf[:readSize], alignedOffset); err != nil {
		return nil, fmt.Errorf("disk %d vlog readat offset=%d: %w", vl.diskIdx, offset, err)
	}

	// The header starts at intraBlock within the 4K-aligned buffer.
	hdr := buf[intraBlock:]
	magic := binary.LittleEndian.Uint32(hdr[vhdrOffMagic:])
	if magic != vlogMagic {
		return nil, fmt.Errorf("disk %d: vlog bad magic at offset %d (corruption?)", vl.diskIdx, offset)
	}

	storedCRC := binary.LittleEndian.Uint32(hdr[vhdrOffCRC:])
	payload := hdr[vlogHeaderBytes : vlogHeaderBytes+int(valueLen)]
	computed := computeCRC32C(payload)
	if computed != storedCRC {
		return nil, fmt.Errorf("disk %d: vlog CRC32C mismatch at offset %d (stored=%08x computed=%08x)",
			vl.diskIdx, offset, storedCRC, computed)
	}

	out := make([]byte, valueLen)
	copy(out, payload)
	vl.readBytes.Add(int64(valueLen))
	if vl.readLatencyCtr.Add(1)&diskLatencySampleMask == 0 {
		updateLatencyEWMA(&vl.readLatencyEWMANs, time.Now().UnixNano()-startNs)
	}
	return out, nil
}

// MarkDead decrements live byte accounting when a key is overwritten or deleted.
// Called by the engine's Put (overwrite) and Delete paths so the Defragmenter
// can accurately compute the GC ratio without scanning every record.
//
// packed: true when the record was written through the packed VLogBatcher path
// (multiple records share a 4 KB block). For packed records we subtract only
// the raw header+value bytes — the block itself stays "live" until every
// record inside it is dead. For unpacked records (legacy 4 KB-per-record path)
// we subtract the full aligned block.
//
// Mismatched packed flag: a permanent over- or under-count of liveBytes.
// Callers MUST source the flag from `entry.IsPacked()` on the IndexEntry that
// is being superseded — never guess from offset alignment (a packed record at
// block-start position 0 has a 4 K-aligned offset and would be misread).
func (vl *VLog) MarkDead(valueLen uint32, packed bool) {
	rawLen := int64(vlogHeaderBytes) + int64(valueLen)
	var dead int64
	if packed {
		dead = rawLen
	} else {
		dead = (rawLen + int64(vlogBlockSize) - 1) &^ int64(vlogBlockSize-1)
	}
	if vl.liveBytes.Add(-dead) < 0 {
		vl.liveBytes.Store(0)
	}
}

// GCRatio returns the fraction of VLog bytes that are dead (garbage).
// When this exceeds StorageConfig.DefragThreshold the Defragmenter should
// compact this VLog to reclaim space.
func (vl *VLog) GCRatio() float64 {
	total := vl.totalBytes.Load()
	if total == 0 {
		return 0
	}
	live := vl.liveBytes.Load()
	if live < 0 {
		live = 0
	}
	dead := total - live
	if dead < 0 {
		dead = 0
	}
	return float64(dead) / float64(total)
}

// vlogWatermarkPath returns the path of the on-disk punch-watermark file for
// a given disk directory.  The file stores 8 bytes (little-endian uint64) that
// record the highest byte offset up to which PUNCH_HOLE has been called.  On
// startup, NewStorageEngine reads this file and calls applyVLogEmergencyPunch
// before creating WAL files — critical when a previous GC pass freed blocks
// that would otherwise leave the disk "full" at next restart.
func vlogWatermarkPath(diskPath string) string {
	return filepath.Join(diskPath, ".vlog_punch_wm")
}

// loadVLogWatermark reads the saved punch watermark for diskPath.
// Returns 0 if the file is absent or unreadable.
func loadVLogWatermark(diskPath string) int64 {
	data, err := os.ReadFile(vlogWatermarkPath(diskPath))
	if err != nil || len(data) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(data))
}

// saveVLogWatermark atomically writes offset to the watermark file via a
// rename so readers always see a complete 8-byte value.
func saveVLogWatermark(diskPath string, offset int64) {
	tmp := vlogWatermarkPath(diskPath) + ".tmp"
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(offset))
	if err := os.WriteFile(tmp, buf[:], 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, vlogWatermarkPath(diskPath))
}

// applyVLogEmergencyPunch opens the VLog backing for diskPath and frees
// blocks below the saved watermark.  Called at engine startup — before WAL
// files are created — so blocks freed by GC in a previous session are
// immediately released even when the disk is reported "full".
//
// On a regular file: fallocate PUNCH_HOLE.
// On a raw block device (rawDevicePath set): BLKDISCARD over the watermark
//   range. Skipped entirely if rawDevicePath is set and we cannot grab
//   exclusive ownership before newVLog runs — newVLog itself will reapply.
//
// This is a best-effort operation: errors are silently ignored so a missing
// VLog or an unsupported filesystem never blocks engine startup.
func applyVLogEmergencyPunch(diskIdx int, diskPath, rawDevicePath string) {
	wm := loadVLogWatermark(diskPath)
	if wm <= 0 {
		return
	}
	if rawDevicePath != "" {
		// We deliberately skip the pre-newVLog discard for raw mode: holding
		// O_EXCL twice (here + in newVLog) would deadlock startup. The
		// punchWatermark is restored from disk in newVLog, and the next GC
		// pass will issue BLKDISCARD as part of normal operation. No data
		// loss — discard is a hint, not a durability requirement.
		return
	}
	vlogPath := filepath.Join(diskPath, "vlog_active.dat")
	f, err := os.OpenFile(vlogPath, os.O_RDWR, 0644)
	if err != nil {
		return // VLog doesn't exist yet — nothing to punch
	}
	defer f.Close()
	_ = vlogPunchHole(f, wm)
}

// punchDeadHead physically frees disk blocks for VLog bytes [0, minLiveOffset).
// On a regular file backing it uses fallocate PUNCH_HOLE; on a raw block
// device it issues BLKDISCARD (NVMe TRIM) over the same range, which frees
// flash erase blocks rather than just FS extents.  Call after a GC pass once
// all live entries have been relocated above minLiveOffset.  Idempotent:
// only advances the watermark.
//
// This is the only mechanism that actually shrinks disk usage — GC alone only
// moves data to the tail without ever releasing the blocks at the head.
func (vl *VLog) punchDeadHead(minLiveOffset int64) error {
	// Align down to vlogBlockSize so we don't punch a partial block.
	punchEnd := minLiveOffset &^ (int64(vlogBlockSize) - 1)
	wm := vl.punchWatermark.Load()
	if punchEnd <= wm {
		return nil // already punched up to here
	}

	var err error
	if vl.rawDevicePath != "" {
		// On raw devices we discard ONLY the new range [wm, punchEnd) instead
		// of [0, punchEnd) because BLKDISCARD on already-discarded ranges is
		// well-defined but wastes controller cycles. The kernel/FW reports
		// errors for unaligned ranges; both wm and punchEnd are vlogBlockSize-
		// aligned by construction.
		// Guard: never discard the superblock at offset 0–4096.
		startOff := wm
		if startOff < RawSuperblockSize {
			startOff = RawSuperblockSize
		}
		if punchEnd > startOff {
			err = blkDiscardRange(vl.file, startOff, punchEnd-startOff)
		}
	} else {
		err = vlogPunchHole(vl.file, punchEnd)
	}

	if err != nil {
		return err
	}
	vl.punchWatermark.Store(punchEnd)
	saveVLogWatermark(vl.diskPath, punchEnd)
	return nil
}

// Stats returns a snapshot of VLog metrics for this disk.
func (vl *VLog) Stats() VLogStats {
	live := vl.liveBytes.Load()
	if live < 0 {
		live = 0
	}
	return VLogStats{
		DiskIdx:           vl.diskIdx,
		Path:              vl.diskPath,
		FileBytes:         vl.end.Load(),
		LiveBytes:         live,
		GarbageRatio:      vl.GCRatio(),
		WriteBytes:        vl.writeBytes.Load(),
		ReadBytes:         vl.readBytes.Load(),
		WriteLatencyEWMAs: float64(vl.writeLatencyEWMANs.Load()) / 1e9,
		ReadLatencyEWMAs:  float64(vl.readLatencyEWMANs.Load()) / 1e9,
	}
}

// FileSize returns the current VLog file size in bytes.
func (vl *VLog) FileSize() int64 { return vl.end.Load() }

// SetEndAtLeast advances vl.end to at least minEnd (rounded up to the next
// vlogBlockSize boundary).  If vl.end is already ≥ minEnd it is unchanged —
// vl.end never retreats, only grows.  Also bumps totalBytes/liveBytes to keep
// GCRatio() accounting correct after the bump.
//
// Called by the engine startup path AFTER WAL replay to seed vl.end past the
// highest live VLog record — required in raw block-device mode where the
// kernel can't tell us how far prior writes went (Stat().Size()=0). On file
// mode this is a no-op because newVLog already initialised vl.end from the
// real file size.
func (vl *VLog) SetEndAtLeast(minEnd int64) {
	if minEnd <= 0 {
		return
	}
	// Round up to vlogBlockSize so the next Append's atomic add stays aligned.
	aligned := (minEnd + int64(vlogBlockSize) - 1) &^ (int64(vlogBlockSize) - 1)

	for {
		cur := vl.end.Load()
		if cur >= aligned {
			return // already past minEnd — never retreat
		}
		if vl.end.CompareAndSwap(cur, aligned) {
			// Adjust accounting: pretend the gap [cur, aligned) was already
			// written so GCRatio doesn't see an inflated dead ratio.
			delta := aligned - cur
			vl.totalBytes.Add(delta)
			vl.liveBytes.Add(delta)
			return
		}
	}
}

// DiskPath returns the directory path for this disk's VLog.
func (vl *VLog) DiskPath() string { return vl.diskPath }

func (vl *VLog) close() error {
	close(vl.doneCh)
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return vl.file.Close()
}

// flusher is the VLog group-commit goroutine.
//
// It mirrors the WAL flusher: drain all pending vlogFlushReqs, call one
// fdatasync, then reply to every waiter.  window is the maximum time to
// wait for more requests to arrive before flushing (matches WALFlushWindowMs).
func (vl *VLog) flusher(window time.Duration) {
	pending := make([]*vlogFlushReq, 0, 4096)

	flush := func() {
		if len(pending) == 0 {
			return
		}
		err := fdatasync(int(vl.file.Fd()))
		for _, req := range pending {
			req.resp <- err
		}
		pending = pending[:0]
	}

	drain := func() {
	drainLoop:
		for len(pending) < 4096 {
			select {
			case req := <-vl.flushCh:
				pending = append(pending, req)
			default:
				break drainLoop
			}
		}
	}

	var (
		timer  *time.Timer
		timerC <-chan time.Time
	)
	startTimer := func() {
		if window > 0 && timer == nil {
			timer = time.NewTimer(window)
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
		case req := <-vl.flushCh:
			pending = append(pending, req)
			startTimer()
			drain()
			if window == 0 || len(pending) >= 4096 {
				stopTimer()
				flush()
			}

		case <-timerC:
			timer = nil
			timerC = nil
			flush()

		case <-vl.doneCh:
			stopTimer()
		drainFinal:
			for {
				select {
				case req := <-vl.flushCh:
					pending = append(pending, req)
				default:
					break drainFinal
				}
			}
			flush()
			return
		}
	}
}

// ── Group-append batcher (amortises fdatasync across concurrent writes) ──────

// VLogBatcher buffers multiple Append calls and flushes them together under a
// single fdatasync, mirroring the WAL group-commit pattern.  Hot path for
// WriteBatcher.flush, GC compactor relocations, and any caller that wants to
// amortise fdatasync across N writes.
//
// ── Block packing (since 2026-05-09) ───────────────────────────────────────
// VLogBatcher packs multiple records into the same 4 KB block so 128 B-class
// values use ~152 B of disk each instead of a full 4 KB. Density gain: 26×
// for 128 B values vs. one-record-per-block.
//
// Layout of a packed 4 KB block, 26 × 152 B records:
//
//   [hdr|val][hdr|val][hdr|val]...[hdr|val][72 B internal pad]
//    24+128   24+128   24+128      24+128
//      pos 0     152      304        3800
//
// The IndexEntry.DiskOffset for each packed record is `blockOffset + pos`
// (NOT 4 KB-aligned). ReadValue already rounds DOWN to 4 KB and extracts at
// `intraBlock`, so the read path is unchanged. Each entry that goes through
// VLogBatcher gets FlagPacked set in IndexEntry.Flags by the caller.
//
// Records that don't fit (rawLen > vlogBlockSize) get their own dedicated
// block(s) with the legacy unpacked layout — uncommon for typical KV
// workloads.
//
// On Linux with CGO enabled, Flush routes writes through the io_uring
// storage bridge: N pwrites → 1 io_uring_submit. The bridge sees the same
// (offset, buf) pairs it always saw — a packed block is just a 4 KB write
// at a 4 KB-aligned offset. No bridge changes needed.
//
// Usage:
//
//	b := vl.NewBatcher()
//	off1, _ := b.Stage(val1)   // → e.g. blockOff + 0
//	off2, _ := b.Stage(val2)   // → e.g. blockOff + 152
//	off3, _ := b.Stage(val3)   // → e.g. blockOff + 304
//	b.Flush()                  // single fdatasync covers all 3
type VLogBatcher struct {
	vl     *VLog
	blocks []*packBlock
	// Total live bytes added by this batch (sum of header+value over all
	// records). Used to advance vl.liveBytes once on Flush.
	rawLive int64
}

// packBlock is a 4 KB buffer being filled with sequential records.
type packBlock struct {
	offset int64  // 4 KB-aligned offset in the VLog
	buf    []byte // exactly vlogBlockSize bytes (from vlogIOPool); for oversized records, a multiple of vlogBlockSize
	used   int    // bytes filled so far (header+value sum, no internal pad)
}

// stagedRecord is the thin record passed to the io_uring bridge. The bridge
// wants (offset, length, buffer-pointer) tuples — it doesn't care whether
// each tuple is one packed block or one oversized record's full extent. Kept
// as a separate type so the C++ bridge layer doesn't have to know about
// packBlock semantics.
type stagedRecord struct {
	offset    int64
	alignedSz int
	buf       []byte
}

// SetStorageBridge wires the io_uring bridge so that VLogBatcher.Flush
// routes writes through io_uring on Linux instead of N separate WriteAt calls.
// Must be called before any writes; nil disables the bridge (WriteAt fallback).
func (vl *VLog) SetStorageBridge(b *cgoStorageBridge) {
	vl.bridge = b
}

// NewBatcher creates a group-append batcher for this VLog.
func (vl *VLog) NewBatcher() *VLogBatcher {
	return &VLogBatcher{vl: vl}
}

// Stage assembles a record into the current packed block (or starts a new
// block if it doesn't fit) but does NOT write to disk yet. Returns the offset
// the record WILL occupy after Flush() — this offset is intra-block (not
// 4 KB-aligned) when the block is shared with other records.
//
// Callers MUST set IndexEntry.Flags |= FlagPacked on entries built from these
// offsets — without that flag, MarkDead later subtracts a full 4 KB instead
// of just header+value bytes (corrupting GCRatio accounting).
//
// Records larger than vlogBlockSize fall back to the unpacked path: their own
// 4 KB-aligned block sized to fit. The caller in that case should NOT set
// FlagPacked. Today this never happens because every key fits comfortably
// (header 24 B + value ≤ 4 K - 24 = 4072 B ⇒ values up to ~4 KB pack fine).
func (b *VLogBatcher) Stage(value []byte) (int64, error) {
	rawLen := vlogHeaderBytes + len(value)
	if rawLen > vlogBlockSize {
		return b.stageOversized(value)
	}

	// Try to fit into the last (open) block.
	var blk *packBlock
	if n := len(b.blocks); n > 0 {
		last := b.blocks[n-1]
		if last.used+rawLen <= vlogBlockSize {
			blk = last
		}
	}
	// No room — allocate a fresh 4 KB block.
	if blk == nil {
		offset := b.vl.end.Add(int64(vlogBlockSize)) - int64(vlogBlockSize)
		buf := vlogIOPool.get(vlogBlockSize)
		// Zero the unused tail explicitly so the on-disk block is well-defined
		// for any future packed Stage call against this same buffer (vlogIOPool
		// doesn't zero on get).
		for i := 0; i < vlogBlockSize; i++ {
			buf[i] = 0
		}
		blk = &packBlock{offset: offset, buf: buf, used: 0}
		b.blocks = append(b.blocks, blk)
	}

	// Serialize the record into the block at blk.used.
	pos := blk.used
	binary.LittleEndian.PutUint32(blk.buf[pos+vhdrOffMagic:], vlogMagic)
	binary.LittleEndian.PutUint32(blk.buf[pos+vhdrOffValLen:], uint32(len(value)))
	binary.LittleEndian.PutUint32(blk.buf[pos+vhdrOffCRC:], computeCRC32C(value))
	binary.LittleEndian.PutUint64(blk.buf[pos+vhdrOffWriteUs:], uint64(time.Now().UnixMicro()))
	copy(blk.buf[pos+vlogHeaderBytes:], value)
	blk.used += rawLen
	b.rawLive += int64(rawLen)
	return blk.offset + int64(pos), nil
}

// stageOversized handles a record whose raw size exceeds vlogBlockSize. It
// allocates a dedicated multi-block range and writes the record at the start
// — same encoding as the legacy single-record path. The caller should NOT
// set FlagPacked on the IndexEntry.
func (b *VLogBatcher) stageOversized(value []byte) (int64, error) {
	rawLen := vlogHeaderBytes + len(value)
	alignedLen := (rawLen + vlogBlockSize - 1) &^ (vlogBlockSize - 1)
	buf := vlogIOPool.get(alignedLen)
	binary.LittleEndian.PutUint32(buf[vhdrOffMagic:], vlogMagic)
	binary.LittleEndian.PutUint32(buf[vhdrOffValLen:], uint32(len(value)))
	binary.LittleEndian.PutUint32(buf[vhdrOffCRC:], computeCRC32C(value))
	binary.LittleEndian.PutUint64(buf[vhdrOffWriteUs:], uint64(time.Now().UnixMicro()))
	copy(buf[vlogHeaderBytes:], value)
	for i := vlogHeaderBytes + len(value); i < alignedLen; i++ {
		buf[i] = 0
	}
	offset := b.vl.end.Add(int64(alignedLen)) - int64(alignedLen)
	// Represent oversized records as a single "block" whose buf is alignedLen
	// (multiple of vlogBlockSize). The Flush loop writes blk.buf[:len(buf)]
	// not blk.buf[:vlogBlockSize], so this works for any size multiple.
	b.blocks = append(b.blocks, &packBlock{offset: offset, buf: buf, used: alignedLen})
	b.rawLive += int64(rawLen)
	return offset, nil
}

// Flush writes all staged blocks to disk in offset order and issues a single
// fdatasync. Returns block buffers to vlogIOPool. Not safe to call
// concurrently with another Flush on the same batcher.
//
// On Linux with the io_uring storage bridge wired (vl.bridge != nil), all
// blocks are dispatched in a single io_uring_submit call (linked SQEs +
// IOSQE_IO_LINK for ordering), then one fdatasync. Replaces N separate
// WriteAt syscalls with 1 io_uring_submit (zero-syscall on SQPOLL hot path).
func (b *VLogBatcher) Flush() error {
	if len(b.blocks) == 0 {
		return nil
	}
	defer b.release()

	fd := int(b.vl.file.Fd())

	// ── io_uring fast path (Linux + bridge wired) ──────────────────────────
	if ok := vlogFlushViaBridge(b, fd); ok {
		var blockTotal int64
		for _, blk := range b.blocks {
			blockTotal += int64(len(blk.buf))
		}
		b.vl.totalBytes.Add(blockTotal)
		b.vl.liveBytes.Add(b.rawLive)
		return nil
	}

	// ── Fallback: sequential WriteAt + one fdatasync ───────────────────────
	for _, blk := range b.blocks {
		if blk.offset%int64(vlogBlockSize) != 0 {
			return fmt.Errorf("disk %d vlog batch: block offset %d not 4K-aligned", b.vl.diskIdx, blk.offset)
		}
		if len(blk.buf)%vlogBlockSize != 0 {
			return fmt.Errorf("disk %d vlog batch: block buf len %d not 4K multiple", b.vl.diskIdx, len(blk.buf))
		}
		if _, err := b.vl.file.WriteAt(blk.buf, blk.offset); err != nil {
			return fmt.Errorf("disk %d vlog batch writeat: %w", b.vl.diskIdx, err)
		}
	}
	if err := fdatasync(fd); err != nil {
		return fmt.Errorf("disk %d vlog batch fdatasync: %w", b.vl.diskIdx, err)
	}

	var blockTotal int64
	for _, blk := range b.blocks {
		blockTotal += int64(len(blk.buf))
	}
	b.vl.totalBytes.Add(blockTotal)
	b.vl.liveBytes.Add(b.rawLive)
	return nil
}

func (b *VLogBatcher) release() {
	for _, blk := range b.blocks {
		vlogIOPool.put(blk.buf)
	}
	b.blocks = b.blocks[:0]
	b.rawLive = 0
}
