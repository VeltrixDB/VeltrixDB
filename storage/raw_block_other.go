//go:build !linux

package storage

import (
	"fmt"
	"os"
)

// Mirror of constants from raw_block_linux.go so non-Linux callers (vlog.go's
// shared code path, tests) can reference RawSuperblockSize without the build
// failing. The non-Linux helpers below all return errors, so these constants
// are referenced for arithmetic only — they never gate real I/O.
const (
	RawSuperblockMagic   uint32 = 0x564C4252
	RawSuperblockVersion uint32 = 1
	RawSuperblockSize    int64  = 4096
	RawSuperblockCRCOff  int    = 60
)

// IsBlockDevice reports false on non-Linux platforms — raw block-device
// VLog backing is a Linux-only feature (BLKDISCARD ioctl, /dev/nvmeXnY paths).
// macOS dev builds always run the file-based VLog path.
func IsBlockDevice(_ string) bool { return false }

// openBlockDevice fails on non-Linux. The server startup code rejects
// RawVLogDevices before reaching this stub, but the function exists so
// non-Linux builds compile cleanly.
func openBlockDevice(path string) (*os.File, error) {
	return nil, fmt.Errorf("raw block-device VLog (%s) is Linux-only", path)
}

// readRawSuperblock / writeRawSuperblock / blkDiscardRange are Linux-only
// stubs returning errors so the engine reports a clear message if it ever
// reaches them on a non-Linux build.

func readRawSuperblock(_ *os.File) (int64, bool, error) {
	return 0, false, fmt.Errorf("raw superblock unsupported on non-Linux")
}

func writeRawSuperblock(_ *os.File, _ int64) error {
	return fmt.Errorf("raw superblock unsupported on non-Linux")
}

func blkDiscardRange(_ *os.File, _, _ int64) error {
	return fmt.Errorf("BLKDISCARD unsupported on non-Linux")
}
