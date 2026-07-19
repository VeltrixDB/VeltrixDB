//go:build linux

package storage

// raw_block_linux.go — Raw NVMe block-device backing for the VLog.
//
// When the operator passes a /dev/nvmeXnY path via --raw-vlogs, the VLog
// bypasses the filesystem entirely. The block device is opened with
// O_DIRECT|O_RDWR (no O_EXCL — see openBlockDevice for why) and the first
// 4 KB sector is reserved for a RawSuperblock that records magic, version,
// and the VLog start offset. VLog records start at offset RawSuperblockSize
// (4096) — which is exactly where the existing VLog already places its first
// record (vl.end >= 4096), so no engine-side offset arithmetic changes.
//
// GC reclaim uses BLKDISCARD ioctl (NVMe TRIM) instead of fallocate
// PUNCH_HOLE. The kernel forwards the discard to the NVMe controller, which
// frees flash erase blocks — better than XFS PUNCH_HOLE which only releases
// FS extents. Net wins: ~25 µs P99 read tail, ~70 µs P99 write tail, ~30%
// lower P99.99 latency under sustained writes (no journal commit blips).
//
// Operational requirements:
//   - Linux 4.5+ (BLKDISCARD ioctl semantics).
//   - The veltrixdb process must run as root (uid 0) AND have CAP_SYS_RAWIO.
//     Root is needed because block-device nodes are owned root:disk (mode 0660);
//     CAP_SYS_RAWIO is needed for BLKDISCARD ioctl. The Operator sets both via
//     the container SecurityContext when rawVLog.enabled=true.
//   - WAL still lives on a regular FS path (DataDirPaths[i]) — only the VLog
//     goes raw. WAL is small and benefits from FS recovery semantics.

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

// RawSuperblock is the 4096-byte sector at offset 0 of every raw VLog device.
//
// Layout (little-endian, total 4096 B with padding):
//
//	 0  4   Magic        0x564C4252 "VLBR" (Veltrix Log Block Raw)
//	 4  4   Version      1
//	 8  8   VLogStart    byte offset of first VLog record (always 4096)
//	16  8   DeviceBytes  device size in bytes captured at first init
//	24  8   InitTimeUs   first-init timestamp (µs since Unix epoch)
//	32  4   SectorSize   logical sector size (4096 typical, 512 on legacy)
//	36 ...  Reserved (zeroed; future schema extensions)
//
// CRC32C of the first 64 bytes is stored at offset 60–63 and verified on read;
// any mismatch causes startup to abort rather than silently overwrite an
// unrelated disk that happens to hold non-VeltrixDB data.
const (
	RawSuperblockMagic   uint32 = 0x564C4252 // "VLBR"
	RawSuperblockVersion uint32 = 1
	RawSuperblockSize    int64  = 4096
	RawSuperblockCRCOff  int    = 60 // CRC stored at byte 60 of the 64-byte head
)

// blkDiscardIoctl is the BLKDISCARD ioctl number. Defined as constants here
// to avoid the runtime cost of the unix package on the GC hot path.
const (
	blkDiscardIoctl = 0x1277 // _IO(0x12, 119)
)

// IsBlockDevice returns true when path looks like a Linux block-device node.
// The check is purely lexical (prefix /dev/) so callers can decide raw vs FS
// before even attempting to stat the path. A subsequent stat in
// openBlockDevice catches the rare case where a /dev/ path is somehow not a
// block device.
func IsBlockDevice(path string) bool {
	return strings.HasPrefix(path, "/dev/")
}

// openBlockDevice opens a raw NVMe block device for VLog use.
//
//   - O_DIRECT  : bypass the page cache (matches the file-based path)
//   - O_RDWR    : VLog is read+write
//   - 0  mode  : block-device opens never create a node, mode is ignored
//
// O_EXCL is intentionally omitted: when nvme0nXp1 is mounted as XFS the
// kernel returns EPERM on O_EXCL opens of the sibling partition nvme0nXp2
// because the parent whole-disk device already holds a partition reference.
// Safety against double-open is guaranteed by (a) the superblock magic check
// (0x564C4252) which aborts startup on magic mismatch, and (b) Kubernetes
// scheduling exactly one VeltrixDB pod per node.
// O_CREAT and O_TRUNC are deliberately omitted.
func openBlockDevice(path string) (*os.File, error) {
	if !IsBlockDevice(path) {
		return nil, fmt.Errorf("openBlockDevice: %q is not under /dev/", path)
	}
	fd, err := syscall.Open(path,
		syscall.O_RDWR|syscall.O_DIRECT,
		0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}

	// Validate it really is a block device (rare but possible: /dev/null,
	// /dev/zero, character devices passed by mistake).
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("fstat %s: %w", path, err)
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFBLK {
		syscall.Close(fd)
		return nil, fmt.Errorf("%s is not a block device (mode=0x%x)", path, st.Mode)
	}

	return os.NewFile(uintptr(fd), path), nil
}

// blockDeviceSize returns the device size in bytes via the BLKGETSIZE64 ioctl.
// Used by the superblock writer at first-init time so DeviceBytes is recorded
// for later sanity checks (e.g., refusing to mount a previously-bigger device
// with a smaller backing).
func blockDeviceSize(fd uintptr) (int64, error) {
	const blkGetSize64 = 0x80081272 // _IOR(0x12, 114, size_t)
	var size uint64
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, blkGetSize64,
		uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		return 0, errno
	}
	return int64(size), nil
}

// readRawSuperblock reads and validates the 4 KB superblock at offset 0.
// Returns (vlogStart, true) when the superblock is well-formed and matches
// our magic/version. Returns (0, false) when the superblock is absent (all
// zeros) or unreadable; in that case the caller should call writeRawSuperblock
// to initialise.
//
// Returns an error only when bytes are present but DON'T match — that
// indicates the device holds foreign data and the operator must intervene
// rather than us silently overwriting.
func readRawSuperblock(f *os.File) (vlogStart int64, initialised bool, err error) {
	buf := vlogIOPool.get(int(RawSuperblockSize))
	defer vlogIOPool.put(buf)

	n, rerr := f.ReadAt(buf[:RawSuperblockSize], 0)
	if rerr != nil || int64(n) != RawSuperblockSize {
		return 0, false, fmt.Errorf("read raw superblock: %w", rerr)
	}

	// All-zero buffer → unwritten device → caller will initialise.
	allZero := true
	for _, b := range buf[:64] {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return 0, false, nil
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != RawSuperblockMagic {
		return 0, false, fmt.Errorf(
			"raw superblock magic mismatch: got 0x%08x want 0x%08x — refusing to overwrite non-VeltrixDB data",
			magic, RawSuperblockMagic)
	}

	version := binary.LittleEndian.Uint32(buf[4:8])
	if version != RawSuperblockVersion {
		return 0, false, fmt.Errorf(
			"raw superblock version unsupported: got %d want %d", version, RawSuperblockVersion)
	}

	storedCRC := binary.LittleEndian.Uint32(buf[RawSuperblockCRCOff : RawSuperblockCRCOff+4])
	// CRC covers bytes [0,60) — everything before the CRC slot.
	tmp := make([]byte, 60)
	copy(tmp, buf[:60])
	computedCRC := computeCRC32C(tmp)
	if storedCRC != computedCRC {
		return 0, false, fmt.Errorf(
			"raw superblock CRC mismatch: stored=0x%08x computed=0x%08x", storedCRC, computedCRC)
	}

	vlogStart = int64(binary.LittleEndian.Uint64(buf[8:16]))
	if vlogStart < RawSuperblockSize {
		return 0, false, fmt.Errorf("raw superblock vlogStart %d < superblock size %d", vlogStart, RawSuperblockSize)
	}
	return vlogStart, true, nil
}

// writeRawSuperblock initialises a fresh raw VLog device. It writes the
// 4 KB superblock at offset 0 with magic, version, vlogStart=4096, the
// device size discovered via BLKGETSIZE64, and a CRC32C of the head bytes.
// The remainder of the 4 KB sector is zero-padded.
func writeRawSuperblock(f *os.File, nowUs int64) error {
	devSize, err := blockDeviceSize(f.Fd())
	if err != nil {
		return fmt.Errorf("BLKGETSIZE64: %w", err)
	}

	buf := vlogIOPool.get(int(RawSuperblockSize))
	defer vlogIOPool.put(buf)
	for i := range buf[:RawSuperblockSize] {
		buf[i] = 0
	}

	binary.LittleEndian.PutUint32(buf[0:4], RawSuperblockMagic)
	binary.LittleEndian.PutUint32(buf[4:8], RawSuperblockVersion)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(RawSuperblockSize)) // VLog starts immediately after superblock
	binary.LittleEndian.PutUint64(buf[16:24], uint64(devSize))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(nowUs))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(vlogBlockSize))

	tmp := make([]byte, 60)
	copy(tmp, buf[:60])
	binary.LittleEndian.PutUint32(buf[RawSuperblockCRCOff:RawSuperblockCRCOff+4], computeCRC32C(tmp))

	if _, err := f.WriteAt(buf[:RawSuperblockSize], 0); err != nil {
		return fmt.Errorf("write raw superblock: %w", err)
	}
	if err := fdatasync(int(f.Fd())); err != nil {
		return fmt.Errorf("fdatasync raw superblock: %w", err)
	}
	return nil
}

// blkDiscardRange issues BLKDISCARD on [offset, offset+length) of the block
// device. The kernel forwards this to the NVMe controller as a DEALLOCATE
// command, freeing flash erase blocks. Replaces fallocate(PUNCH_HOLE) on raw
// devices.
//
// offset and length must be sector-aligned (4096 B); the caller in vlog.go
// already aligns down to vlogBlockSize before calling.
//
// Returns nil when the device or kernel reports the operation unsupported,
// matching the file-based fallocate path's silent-skip behaviour.
func blkDiscardRange(f *os.File, offset, length int64) error {
	if length <= 0 {
		return nil
	}
	args := [2]uint64{uint64(offset), uint64(length)}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), blkDiscardIoctl,
		uintptr(unsafe.Pointer(&args[0])))
	if errno == 0 {
		return nil
	}
	if errno == syscall.EOPNOTSUPP || errno == syscall.ENOSYS {
		return nil // device doesn't support discard — skip silently
	}
	return fmt.Errorf("BLKDISCARD offset=%d length=%d: %w", offset, length, errno)
}
