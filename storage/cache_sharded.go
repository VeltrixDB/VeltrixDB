package storage

import "math/bits"

// Sharded LIRS cache.
//
// # Why
//
// LIRSCache guards its whole state — the S stack, the Q queue and the index
// map — with a single sync.Mutex, and Get must take it exclusively because a
// read mutates the LIRS state machine (access() reorders S and Q). That makes
// the cache a global serialisation point on the hottest path in the engine.
//
// The index is deliberately split 8192 ways so reads on different keys never
// contend. Routing every one of those reads through one cache mutex threw that
// away. A CPU profile of BenchmarkGet_CacheHit on an 18-core machine put
// 21.7% of total runtime inside sync.(*Mutex).Lock, of which 98.98% was
// LIRSCache.Get — i.e. the cache lock alone, not the work under it.
//
// # How
//
// Partition into N independent LIRSCache instances, each with its own mutex
// and its own 1/N slice of the byte budget, selected by key hash. Contention
// drops by roughly N; each shard runs the unmodified, already-tested LIRS
// state machine.
//
// # The trade-off, stated plainly
//
// Per-shard budgets are not a global budget. A skewed key distribution can
// evict from a hot shard while a cold shard still has room, so the effective
// hit rate is slightly below a perfect global LIRS at the same total size.
// With a 64-bit hash and ≥256 keys per shard the imbalance is small (the
// standard balls-in-bins result), and it buys near-linear read scalability.
// This is the same trade every sharded cache makes (Caffeine, groupcache,
// bigcache).
//
// Shard selection uses the HIGH bits of the FNV-1a hash. The index shard mask
// is the low 13 bits, so taking high bits keeps cache placement statistically
// independent of index placement rather than aliasing onto it.

const (
	// maxCacheShards caps the split. Past this, per-shard budgets get small
	// enough that eviction imbalance starts to outweigh the contention win.
	maxCacheShards = 256

	// minBytesPerCacheShard keeps each shard big enough to hold a useful
	// working set. Below this the split is counterproductive, so the shard
	// count is reduced (see cacheShardCountFor) — a 1 MB cache stays
	// single-shard rather than becoming 256 × 4 KB.
	minBytesPerCacheShard = 1 << 20 // 1 MB
)

// cacheShardCountFor picks a power-of-two shard count that keeps each shard at
// or above minBytesPerCacheShard, capped at maxCacheShards.
func cacheShardCountFor(totalBytes uint64) int {
	if totalBytes < minBytesPerCacheShard*2 {
		return 1
	}
	n := totalBytes / minBytesPerCacheShard
	if n > maxCacheShards {
		n = maxCacheShards
	}
	// Round DOWN to a power of two so per-shard bytes stay >= the minimum.
	shift := bits.Len64(n) - 1
	return 1 << shift
}

// shardedLIRSCache routes each key to one of N independent LIRSCache shards.
type shardedLIRSCache struct {
	shards []*LIRSCache
	shift  uint // 64 - log2(len(shards)); selects the top bits of the hash
}

// NewShardedLIRSCache builds a LIRS cache split across enough shards to keep
// per-shard mutex contention off the read path.
//
// maxSizeMB is the TOTAL budget across all shards, so callers size it exactly
// as they would a single LIRSCache. Falls back to a plain single LIRSCache
// when the budget is too small to split usefully.
func NewShardedLIRSCache(maxSizeMB uint32, lirRatio float64) Cache {
	totalBytes := uint64(maxSizeMB) * 1024 * 1024
	n := cacheShardCountFor(totalBytes)
	if n <= 1 {
		return NewLIRSCache(maxSizeMB, lirRatio)
	}

	perShardMB := maxSizeMB / uint32(n)
	if perShardMB == 0 {
		perShardMB = 1
	}
	c := &shardedLIRSCache{
		shards: make([]*LIRSCache, n),
		shift:  uint(64 - (bits.Len64(uint64(n)) - 1)),
	}
	for i := range c.shards {
		c.shards[i] = NewLIRSCache(perShardMB, lirRatio).(*LIRSCache)
	}
	return c
}

// shardFor selects a shard from the high bits of the key hash. The index uses
// the low 13 bits, so high bits keep the two placements independent.
func (c *shardedLIRSCache) shardFor(key string) *LIRSCache {
	return c.shards[fnv64a(key)>>c.shift]
}

func (c *shardedLIRSCache) Get(key string) ([]byte, bool) { return c.shardFor(key).Get(key) }

// getHashed is Get for callers that already hold fnv64a(key), so the read
// path hashes once rather than once per subsystem.
func (c *shardedLIRSCache) getHashed(key string, h uint64) ([]byte, bool) {
	return c.shards[h>>c.shift].Get(key)
}
func (c *shardedLIRSCache) Put(key string, value []byte) { c.shardFor(key).Put(key, value) }

func (c *shardedLIRSCache) PutIfPresent(key string, value []byte) bool {
	return c.shardFor(key).PutIfPresent(key, value)
}
func (c *shardedLIRSCache) Evict(key string) { c.shardFor(key).Evict(key) }

func (c *shardedLIRSCache) Size() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += s.Size()
	}
	return total
}

func (c *shardedLIRSCache) Stats() CacheStats {
	var agg CacheStats
	for _, s := range c.shards {
		// Read the counters directly rather than calling s.Stats(), which
		// would take every shard's mutex just to recompute a hit rate we
		// discard. Size() still needs the lock for the byte totals.
		agg.Hits += s.hitCount.Load()
		agg.Misses += s.missCount.Load()
		agg.Evictions += s.evictions.Load()
		agg.CurrentSizeBytes += s.Size()
		agg.MaxSizeBytes += s.maxBytes
	}
	if total := agg.Hits + agg.Misses; total > 0 {
		agg.HitRate = float64(agg.Hits) / float64(total)
	}
	return agg
}

// ShardCount reports how many independent shards this cache was split into.
// Exposed for metrics and tests; 1 means the budget was too small to split.
func (c *shardedLIRSCache) ShardCount() int { return len(c.shards) }

// cacheShardCount returns the shard count of any Cache, for metrics and tests.
// Single-shard LIRSCache and other implementations report 1.
func cacheShardCount(c Cache) int {
	if sc, ok := c.(*shardedLIRSCache); ok {
		return sc.ShardCount()
	}
	return 1
}

// compile-time guarantee that both cache types satisfy the interface.
var (
	_ Cache = (*shardedLIRSCache)(nil)
	_ Cache = (*LIRSCache)(nil)
)
