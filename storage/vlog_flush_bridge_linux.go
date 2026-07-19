//go:build linux && cgo && go1.21

package storage

// vlogFlushViaBridge submits all staged records through the io_uring storage
// bridge in a single io_uring_submit call, then calls fdatasync once for
// crash durability.
//
// Returns true when the bridge path succeeded (all records written).
// Returns false when the bridge is unavailable — caller falls back to WriteAt.
//
// Why still fdatasync after io_uring writes?
// ─────────────────────────────────────────
// O_DIRECT bypasses the page cache so data lands in the NVMe DMA buffer
// immediately.  However, NVMe drives have a volatile write cache (WC) that
// must be flushed to persistent media before a power failure is safe.
// fdatasync issues a FLUSH CACHE command that drains the NVMe WC.
//
// GKE local NVMe SSDs have power-loss protection (capacitors), so the WC is
// effectively persistent — but we do not rely on hardware PLP for
// correctness.  One fdatasync per batch (not per record) keeps the cost low.
//
// io_uring SQPOLL eliminates the io_uring_enter syscall for all SQEs.
// The single fdatasync that follows is ~0.2 ms on local NVMe, shared across
// every write in the batch.
func vlogFlushViaBridge(b *VLogBatcher, fd int) bool {
	br := b.vl.bridge
	if br == nil || br.handle == nil {
		return false
	}

	// Build the stagedRecord slice the bridge expects — one entry per packed
	// block (or per oversized-record block group). Each entry carries the
	// 4 KB-aligned offset and the full block-buffer length, so the bridge
	// issues N pwrites where N == len(b.blocks), not len(records-staged).
	staged := make([]stagedRecord, len(b.blocks))
	for i, blk := range b.blocks {
		staged[i] = stagedRecord{
			offset:    blk.offset,
			alignedSz: len(blk.buf),
			buf:       blk.buf,
		}
	}

	if br.submitVLogBatch(fd, b.vl.diskIdx, staged) != len(staged) {
		return false
	}

	if fdatasync(fd) != nil {
		return false
	}

	return true
}
