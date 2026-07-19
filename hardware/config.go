package hardware

import (
	"math/bits"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// AutoConfig derives a *storage.StorageConfig tuned to the detected hardware.
//
// Memory — the 80% rule:
//
//	combinedMB      = 80% of TotalRAMMB
//	MaxMemorySizeMB = 15% of combinedMB   (WAL / memtable headroom)
//	CacheMaxSizeMB  = 85% of combinedMB   (LIRS data + index blocks)
//
// Sharding:
//
//	Target = nearest power-of-2 ≥ cores×8, capped at 256.
//	The shard index uses FNV-1a(key) & 0xFF (const numShards=256 in shard.go),
//	so no config value can raise the effective count above 256 without a full
//	index migration (CLAUDE.md invariant 1).
//
// Compaction:
//
//	threads = cores/4, clamped to [2, 16].
//
// I/O:
//
//	SSTableMaxSizeMB = 512 for NVMe-only, 64 when any HDD is present.
func AutoConfig(p *Profile, diskPaths []string) *storage.StorageConfig {
	cfg := storage.DefaultStorageConfig()

	// ── Memory ───────────────────────────────────────────────────────────
	combined := p.TotalRAMMB * 80 / 100
	cfg.MaxMemorySizeMB = combined * 15 / 100
	cfg.CacheMaxSizeMB = uint32(combined * 85 / 100)

	// ── Sharding ─────────────────────────────────────────────────────────
	cfg.NumShards = nearestPow2Capped256(p.CPUCores * 8)

	// ── Compaction ───────────────────────────────────────────────────────
	threads := p.CPUCores / 4
	switch {
	case threads < 2:
		threads = 2
	case threads > 16:
		threads = 16
	}
	cfg.CompactionThreads = uint16(threads)
	cfg.CompactionInterval = 30 * time.Second

	// ── I/O strategy ─────────────────────────────────────────────────────
	cfg.SSTableMaxSizeMB = sstMaxSize(p.Disks)

	// DirtyFlushThreshold: memtable budget ÷ assumed average value size (256 B).
	// Backpressure kicks in at 3× the flush threshold (ScyllaDB-style soft stall).
	avgVal := uint64(256)
	cfg.DirtyFlushThreshold = int(cfg.MaxMemorySizeMB * 1024 * 1024 / avgVal)
	cfg.BackPressureThreshold = cfg.DirtyFlushThreshold * 3

	// ── Paths ─────────────────────────────────────────────────────────────
	if len(diskPaths) > 0 {
		cfg.DataDirPaths = diskPaths
	}

	return cfg
}

// nearestPow2Capped256 returns the smallest power-of-2 that is ≥ n, capped at 256.
// The bitmask & 0xFF in shard routing makes 256 the hard ceiling (CLAUDE.md invariant 1).
func nearestPow2Capped256(n int) int {
	if n < 1 {
		n = 1
	}
	p := 1 << bits.Len(uint(n-1))
	if p < 1 {
		p = 1
	}
	if p > 256 {
		return 256
	}
	return p
}

// sstMaxSize returns 512 MB for all-NVMe setups, 64 MB when any HDD is present.
func sstMaxSize(disks []DiskInfo) uint64 {
	for _, d := range disks {
		if d.Kind == DiskKindHDD {
			return 64
		}
	}
	return 512
}
