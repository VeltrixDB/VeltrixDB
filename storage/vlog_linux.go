//go:build linux

package storage

import (
	"os"
	"syscall"
)

// openVLogFile opens the VLog file with O_DIRECT on Linux.
//
// O_DIRECT is safe here because:
//   - All new VLog records are 4096-byte (vlogBlockSize) aligned: Append() and
//     Stage() both round up to vlogBlockSize and allocate from vlogIOPool whose
//     buffers are posix_memalign(4096) aligned.
//   - ReadValue() rounds the file offset DOWN to the nearest 4 KB boundary and
//     reads a full vlogBlockSize multiple, satisfying O_DIRECT on both 512n and
//     4Kn NVMe drives.
//   - The gap between the last 512-byte-aligned record on disk and the new
//     4K-aligned startOffset is sealed on newVLog() startup (truncate to 512B
//     boundary, advance vl.end to next 4K).  No DiskOffset ever points into
//     that gap, so no O_DIRECT read will straddle it.
//
// WAL still uses buffered I/O (Invariant 6 — O_DIRECT + O_APPEND has kernel bugs).
func openVLogFile(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_DIRECT, 0644)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}
