//go:build !linux

package storage

import "os"

// vlogPunchHole is a no-op on non-Linux platforms (macOS dev builds).
func vlogPunchHole(f *os.File, length int64) error { return nil }
