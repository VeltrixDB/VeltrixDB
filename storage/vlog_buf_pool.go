package storage

import (
	"sync"
	"unsafe"
)

// vlogBlockSize is the I/O alignment unit for VLog files.
//
// Must be 4096 — not 512 — for two reasons that compound each other:
//
//  1. XFS (mkfs.xfs default block size = 4096): O_DIRECT on an XFS filesystem
//     requires all three of (buffer pointer, file offset, transfer length) to be
//     multiples of the filesystem block size (4096), regardless of the NVMe
//     drive's own logical sector size.  512-byte O_DIRECT writes return EINVAL.
//
//  2. 4Kn NVMe drives: some GKE local SSDs have a 4096-byte logical sector
//     (not 512n).  O_DIRECT on 4Kn hardware also requires 4096-byte alignment.
//
// Space cost: each VLog record is padded to one 4096-byte block even for small
// values (e.g. 128B value → 152B raw → padded to 4096B; 97% waste/record).
// Reducing this waste requires sub-block packing — write multiple records into
// one 4096-byte block and track intra-block byte offsets in DiskOffset.  That
// is a larger refactor than changing this constant.
const vlogBlockSize = 4096

// vlogIOPool is a pool of vlogBlockSize-aligned slices for VLog I/O.
// vlogIOPool is a pool of 4096-byte-aligned slices for VLog I/O.
// Separate from ioPool (512-byte aligned, for segment files) — VLog buffers
// must be 4096-byte aligned to satisfy O_DIRECT on XFS and 4Kn NVMe drives.
var vlogIOPool = &vlog4kBufPool{}

type vlog4kBufPool struct{ p sync.Pool }

// newAlignedBuf4K returns a byte slice whose first element is aligned to 4096
// bytes and whose length is rounded up to the next 4096-byte boundary.
// Uses the same over-allocation trick as newAlignedBuf in segment_linux.go but
// with 4096-byte granularity so the buffers satisfy O_DIRECT on XFS/4Kn drives.
func newAlignedBuf4K(size int) []byte {
	const align = vlogBlockSize
	size = (size + align - 1) &^ (align - 1)
	raw := make([]byte, size+align)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	offset := int((uintptr(align) - addr%uintptr(align)) % uintptr(align))
	return raw[offset : offset+size]
}

func (ap *vlog4kBufPool) get(minSize int) []byte {
	const align = vlogBlockSize
	minSize = (minSize + align - 1) &^ (align - 1)
	if v := ap.p.Get(); v != nil {
		b := v.([]byte)
		if cap(b) >= minSize {
			return b[:minSize]
		}
	}
	return newAlignedBuf4K(minSize)
}

// put stores b[:cap(b)] so the full aligned backing array is reused.
// Invariant: cap(b) starts at the 4096-byte-aligned address returned by
// newAlignedBuf4K; re-slicing with [:cap(b)] preserves that alignment.
func (ap *vlog4kBufPool) put(b []byte) {
	if b != nil {
		ap.p.Put(b[:cap(b)])
	}
}
