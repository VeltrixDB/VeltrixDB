//go:build !linux

package storage

// lockDefragThread is a no-op on non-Linux platforms.
func lockDefragThread() {}
