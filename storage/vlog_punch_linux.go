//go:build linux

package storage

import (
	"os"
	"syscall"
)

// vlogPunchHole punches a hole in the VLog file from byte 0 to length,
// freeing the underlying disk blocks while keeping the file's logical size.
// This is the physical space reclamation step after GC has relocated all live
// entries above the hole range.
//
// Uses fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE).
// Supported on XFS (GKE default) and ext4.  Returns nil on EOPNOTSUPP so a
// filesystem that does not support hole-punching is silently skipped rather
// than failing GC.
func vlogPunchHole(f *os.File, length int64) error {
	const (
		fallocPunchHole  = 0x2
		fallocKeepSize   = 0x1
		fallocMode       = fallocPunchHole | fallocKeepSize
	)
	err := syscall.Fallocate(int(f.Fd()), fallocMode, 0, length)
	if err == syscall.EOPNOTSUPP || err == syscall.ENOSYS {
		return nil // filesystem doesn't support it — skip silently
	}
	return err
}
