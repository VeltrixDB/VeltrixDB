//go:build linux

package storage

import (
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// openSegmentFile opens a segment file with O_DIRECT so all I/O bypasses the
// OS page cache entirely.  LIRS is the sole caching layer — no double buffering.
//
// O_DIRECT requirements (enforced by the kernel):
//   - I/O buffer pointer must be aligned to sectorSize (512 bytes)
//   - I/O length must be a multiple of sectorSize
//   - File offset must be a multiple of sectorSize
//
// All three are guaranteed by WriteRecord and ReadValue because every record is
// padded to the next 512-byte sector boundary before being written.
func openSegmentFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_DIRECT, 0644)
}

// newAlignedBuf allocates a byte slice whose first element is aligned to
// sectorSize.  Required for O_DIRECT: the kernel rejects I/O from unaligned
// buffers with EINVAL.
//
// Strategy: allocate (size + sectorSize) bytes, then slice from the first
// sectorSize-aligned address within that allocation.  The extra sectorSize
// bytes ensure we always have room to find an aligned start.
func newAlignedBuf(size int) []byte {
	// Round size up to the next sector boundary (I/O length requirement).
	size = (size + sectorSize - 1) &^ (sectorSize - 1)

	// Over-allocate so we can manually align the slice start.
	raw := make([]byte, size+sectorSize)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	offset := int((uintptr(sectorSize) - addr%uintptr(sectorSize)) % uintptr(sectorSize))

	// The sub-slice shares raw's backing array — GC keeps raw alive.
	return raw[offset : offset+size]
}

// ioPool is a pool of reusable sector-aligned byte slices.
// Avoids per-read/write allocation on the hot path.
// Callers MUST call ioPool.put(buf) after they no longer need the slice.
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
		// Pooled buffer is too small; allocate a fresh one (don't return old to pool).
	}
	return newAlignedBuf(minSize)
}

func (ap *alignedBufPool) put(b []byte) {
	if b != nil {
		ap.p.Put(b[:cap(b)])
	}
}
