package storage

// testStorageConfig is the single place test engines get their config.
//
// Every helper in this package must build its config here. Three of them used
// to carry their own copy of this block, and the copies drifted: two ended up
// at 1<<18 bits/shard. The Bloom filter is allocated eagerly at
// bits/shard / 8 * numShards, and numShards is 8192, so 1<<18 is 256 MB per
// engine — not the 32 MB one of the comments claimed, which was arithmetic
// from back when numShards was 1024.
//
// The suite constructs ~105 engines. Under -race that difference took peak RSS
// from 1.1 GB to 8.1 GB, which is what pushed CI runners into swap and got the
// job killed at its 45-minute budget.
func testStorageConfig(dirs ...string) *StorageConfig {
	cfg := DefaultStorageConfig()

	switch len(dirs) {
	case 0:
		// Caller sets the directories itself.
	case 1:
		cfg.DataDirPath = dirs[0]
		cfg.DataDirPaths = nil
	default:
		cfg.DataDirPath = ""
		cfg.DataDirPaths = dirs
	}

	cfg.CacheMaxSizeMB = 16

	// 4 K bits/shard x 8192 shards = 4 MB per engine. Tests do not need a
	// production false-positive rate, and they pay this cost per engine.
	cfg.BloomFilterShardBits = 1 << 12

	// Tests write sequentially, so the 15 ms production group-commit window
	// has nothing to batch and is pure added latency.
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1

	// One goroutine per VLog doing real I/O, in every engine, for no benefit
	// to a test that lives for a second.
	cfg.ScrubEnabled = false

	return cfg
}
