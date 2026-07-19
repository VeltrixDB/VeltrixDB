//go:build !linux

package storage

import (
	"os"
	"sync"
)

// openSegmentFile opens a segment file with standard flags on non-Linux
// platforms (macOS, Windows).  O_DIRECT is Linux-only; on other platforms
// the page cache stays active, which is acceptable for local development.
func openSegmentFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
}

// newAlignedBuf returns a plain slice on non-Linux platforms.  No alignment
// is needed because O_DIRECT is not in use.
func newAlignedBuf(size int) []byte {
	size = (size + sectorSize - 1) &^ (sectorSize - 1)
	return make([]byte, size)
}

var ioPool = &alignedBufPool{}

type alignedBufPool struct {
	p sync.Pool
}

func (ap *alignedBufPool) get(minSize int) []byte {
	minSize = (minSize + sectorSize - 1) &^ (sectorSize - 1)
	if v := ap.p.Get(); v != nil {
		b := v.([]byte)
		if cap(b) >= minSize {
			return b[:minSize]
		}
	}
	return newAlignedBuf(minSize)
}

func (ap *alignedBufPool) put(b []byte) {
	if b != nil {
		ap.p.Put(b[:cap(b)])
	}
}
