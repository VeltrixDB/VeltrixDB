package storage

// segment.go — Binary segment file writer + reader.
//
// One SegmentWriter is created per NVMe disk.  Shards map to disks via:
//
//	diskIdx = shardID % numDisks
//
// On-disk record layout (little-endian, 64-byte aligned header):
//
//	Offset  Size  Field
//	──────────────────────────────────────────────────────────────
//	 0       4    Magic (0x564C5401 = "VLT\x01")
//	 4       1    Flags  0x01=TOMBSTONE  0x02=COMPRESSED
//	 5       3    Reserved
//	 8       4    KeyLen  (bytes)
//	12       4    ValLen  (bytes)
//	16       4    CRC32C  of (key || value)
//	20       4    Reserved
//	24       8    WriteUs  (int64, µs since Unix epoch)
//	32       8    TTLUS    (int64, absolute µs; 0 = immortal)
//	40      24    Reserved (future: HLC / schema version / CRDT)
//	────────────────── total 64 bytes ───────────────────────────
//	64      KeyLen  key bytes
//	64+K    ValLen  value bytes
//	64+K+V  pad     zero-bytes to the next 512-byte sector boundary
//
// Records are sector-aligned so that the C++ io_uring O_DIRECT layer can
// read them without alignment faults.  The same format is used across both
// Go and C++ so future CGO integration is a drop-in.

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// segmentMagic: "VLT\x01" (VeltrixDB segment format version 1)
	segmentMagic = uint32(0x564C5401)

	recordHeaderBytes = 64
	sectorSize        = 512

	flagTombstoneRecord  = uint8(0x01)
	flagCompressedRecord = uint8(0x02)
)

// ── On-disk header offsets ─────────────────────────────────────────────────────

const (
	hdrOffMagic   = 0
	hdrOffFlags   = 4
	hdrOffKeyLen  = 8
	hdrOffValLen  = 12
	hdrOffCRC     = 16
	hdrOffWriteUs = 24
	hdrOffTTLUs   = 32
)

// SegmentWriter owns one segment file on one NVMe disk.  Multiple shards
// assigned to the same disk share a single SegmentWriter; writes are
// serialised by sw.mu so callers need no external synchronisation.
type SegmentWriter struct {
	diskIdx  int
	diskPath string

	mu   sync.Mutex
	file *os.File
	end  atomic.Int64 // next free byte offset in the file
}

// newSegmentWriter opens (or creates) the active segment file for a disk.
// On Linux the file is opened with O_DIRECT so all I/O bypasses the page cache.
// diskIdx is the zero-based disk index; diskPath is the mount point / directory.
func newSegmentWriter(diskIdx int, diskPath string) (*SegmentWriter, error) {
	if err := os.MkdirAll(diskPath, 0755); err != nil {
		return nil, fmt.Errorf("disk %d: mkdir %s: %w", diskIdx, diskPath, err)
	}

	segPath := filepath.Join(diskPath, "seg_active.dat")
	file, err := openSegmentFile(segPath) // O_DIRECT on Linux, plain on others
	if err != nil {
		return nil, fmt.Errorf("disk %d: open %s: %w", diskIdx, segPath, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("disk %d: stat: %w", diskIdx, err)
	}

	// Ensure existing file size is sector-aligned (safe after a clean shutdown).
	size := info.Size()
	if size%sectorSize != 0 {
		// Truncate down to the last complete sector — the partial record at the
		// end was never fdatasync'd (process crashed mid-write) and is invalid.
		size = size &^ (sectorSize - 1)
		if err := file.Truncate(size); err != nil {
			file.Close()
			return nil, fmt.Errorf("disk %d: truncate to sector boundary: %w", diskIdx, err)
		}
	}

	sw := &SegmentWriter{diskIdx: diskIdx, diskPath: diskPath, file: file}
	sw.end.Store(size)
	return sw, nil
}

// WriteRecord serialises one key+value record to disk and fdatasyncs.
// Returns the byte offset at which the record header starts.
//
// O_DIRECT requires that the write buffer is sector-aligned in both pointer
// and length, and that the file offset is sector-aligned.  We satisfy all
// three by assembling the entire record (header + key + value + padding) into
// one aligned buffer allocated by newAlignedBuf, then issuing a single WriteAt.
// WriteAt is used instead of Seek+Write to avoid corrupting seek state when
// concurrent ReadAt calls are in flight on the same file descriptor.
//
// Safe for concurrent callers — writes are serialised by sw.mu.
func (sw *SegmentWriter) WriteRecord(key string, value []byte, ttlUs int64, tombstone bool) (int64, error) {
	rawLen := recordHeaderBytes + len(key) + len(value)
	alignedLen := (rawLen + sectorSize - 1) &^ (sectorSize - 1)

	buf := ioPool.get(alignedLen)
	defer ioPool.put(buf)

	binary.LittleEndian.PutUint32(buf[hdrOffMagic:], segmentMagic)
	if tombstone {
		buf[hdrOffFlags] = flagTombstoneRecord
	}
	binary.LittleEndian.PutUint32(buf[hdrOffKeyLen:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[hdrOffValLen:], uint32(len(value)))

	crc := computeCRC32C(append([]byte(key), value...))
	binary.LittleEndian.PutUint32(buf[hdrOffCRC:], crc)

	binary.LittleEndian.PutUint64(buf[hdrOffWriteUs:], uint64(time.Now().UnixMicro()))
	if ttlUs > 0 {
		binary.LittleEndian.PutUint64(buf[hdrOffTTLUs:], uint64(ttlUs))
	}

	copy(buf[recordHeaderBytes:], key)
	copy(buf[recordHeaderBytes+len(key):], value)

	sw.mu.Lock()
	defer sw.mu.Unlock()

	startOffset := sw.end.Load()
	if _, err := sw.file.WriteAt(buf, startOffset); err != nil {
		return 0, fmt.Errorf("disk %d writeat offset=%d: %w", sw.diskIdx, startOffset, err)
	}

	if err := fdatasync(int(sw.file.Fd())); err != nil {
		return 0, fmt.Errorf("disk %d fdatasync: %w", sw.diskIdx, err)
	}

	sw.end.Add(int64(alignedLen))
	return startOffset, nil
}

// ReadValue reads the value of a record previously written at diskOffset.
// keyLen and valueLen come from the live IndexEntry — they avoid re-parsing the header.
//
// O_DIRECT requires that the read buffer is sector-aligned in both pointer and
// length, and that diskOffset is sector-aligned.  diskOffset is guaranteed
// sector-aligned because WriteRecord always writes sector-rounded records.
// We round the read length up to the next sector boundary and use newAlignedBuf.
//
// ReadAt is lock-free on Linux (pread64 syscall) — multiple goroutines can
// call ReadValue on the same SegmentWriter concurrently without contention.
func (sw *SegmentWriter) ReadValue(diskOffset int64, keyLen, valueLen uint32) ([]byte, error) {
	if valueLen == 0 {
		return nil, nil
	}

	needed := recordHeaderBytes + int(keyLen) + int(valueLen)
	readSize := (needed + sectorSize - 1) &^ (sectorSize - 1)

	buf := ioPool.get(readSize)
	defer ioPool.put(buf)

	if _, err := sw.file.ReadAt(buf[:readSize], diskOffset); err != nil {
		return nil, fmt.Errorf("disk %d readat %d: %w", sw.diskIdx, diskOffset, err)
	}

	magic := binary.LittleEndian.Uint32(buf[hdrOffMagic:])
	if magic != segmentMagic {
		return nil, fmt.Errorf("disk %d: bad magic at offset %d (corruption?)", sw.diskIdx, diskOffset)
	}

	storedCRC := binary.LittleEndian.Uint32(buf[hdrOffCRC:])
	payload := buf[recordHeaderBytes : recordHeaderBytes+int(keyLen)+int(valueLen)]
	computed := computeCRC32C(payload)
	if computed != storedCRC {
		return nil, fmt.Errorf("disk %d: CRC32C mismatch at offset %d (stored %08x computed %08x)",
			sw.diskIdx, diskOffset, storedCRC, computed)
	}

	out := make([]byte, valueLen)
	copy(out, payload[keyLen:])
	return out, nil
}

// DiskSize returns the current byte size of the segment file on this disk.
func (sw *SegmentWriter) DiskSize() int64 { return sw.end.Load() }

// DiskPath returns the directory path for this disk.
func (sw *SegmentWriter) DiskPath() string { return sw.diskPath }

func (sw *SegmentWriter) close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.file.Close()
}

// diskForShard maps a shard ID to a disk index using round-robin assignment:
//
//	shard 0 → disk 0,  shard 1 → disk 1,  …  shard N → disk N%numDisks
//
// This distributes the 256 shards evenly across all disks so every disk
// receives the same number of shards regardless of how many disks are present.
func diskForShard(shardID uint16, numDisks int) int {
	if numDisks <= 1 {
		return 0
	}
	return int(shardID) % numDisks
}
