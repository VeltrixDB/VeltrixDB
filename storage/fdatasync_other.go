//go:build !linux

package storage

import "syscall"

// fdatasync falls back to a full fsync on non-Linux platforms.
func fdatasync(fd int) error {
	return syscall.Fsync(fd)
}
