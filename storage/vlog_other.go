//go:build !linux

package storage

import "os"

// openVLogFile opens the VLog file with buffered I/O on non-Linux platforms
// (macOS dev, Windows CI).  O_DIRECT is Linux-only; on macOS the equivalent
// would be F_NOCACHE via fcntl but it is not needed for correctness in dev.
func openVLogFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
}
