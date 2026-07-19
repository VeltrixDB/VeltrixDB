//go:build linux

package storage

import "syscall"

// fdatasync flushes data to disk without updating inode metadata timestamps,
// reducing write overhead compared to a full fsync.
func fdatasync(fd int) error {
	return syscall.Fdatasync(fd)
}
