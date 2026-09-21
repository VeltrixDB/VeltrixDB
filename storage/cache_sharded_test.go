package storage

import (
	"fmt"
	"sync"
	"testing"
)

// The sharded cache must be behaviourally indistinguishable from the single
// LIRSCache it wraps, apart from per-shard eviction budgets.

func TestShardedCache_ShardCountScalesWithBudget(t *testing.T) {
	cases := []struct {
		mb   uint32
		want int
	}{
		{1, 1},     // below 2 MB: not worth splitting
		{2, 2},     // 2 MB / 1 MB per shard
		{8, 8},     //
		{300, 256}, // capped at maxCacheShards
		{4096, 256},
	}
	for _, tc := range cases {
		got := cacheShardCountFor(uint64(tc.mb) * 1024 * 1024)
		if got != tc.want {
			t.Errorf("cacheShardCountFor(%d MB) = %d, want %d", tc.mb, got, tc.want)
		}
	}
}

// A budget too small to split must fall back to a plain LIRSCache rather than
// producing shards too small to hold anything.
func TestShardedCache_SmallBudgetFallsBackToSingle(t *testing.T) {
	c := NewShardedLIRSCache(1, 0.95)
	if _, ok := c.(*LIRSCache); !ok {
		t.Fatalf("1 MB cache = %T, want *LIRSCache", c)
	}
	if n := cacheShardCount(c); n != 1 {
		t.Errorf("cacheShardCount = %d, want 1", n)
	}
}

func TestShardedCache_PutGetEvictRoundTrip(t *testing.T) {
	c := NewShardedLIRSCache(64, 0.95)
	if n := cacheShardCount(c); n < 2 {
		t.Fatalf("expected a sharded cache, got shard count %d", n)
	}

	const n = 2000
	for i := 0; i < n; i++ {
		c.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
	}
	for i := 0; i < n; i++ {
		got, ok := c.Get(fmt.Sprintf("k%d", i))
		if !ok {
			t.Fatalf("key k%d missing after Put", i)
		}
		if want := fmt.Sprintf("v%d", i); string(got) != want {
			t.Fatalf("k%d = %q, want %q", i, got, want)
		}
	}

	// Evict must remove from the same shard the key routed to.
	for i := 0; i < n; i += 2 {
		c.Evict(fmt.Sprintf("k%d", i))
	}
	for i := 0; i < n; i++ {
		_, ok := c.Get(fmt.Sprintf("k%d", i))
		if i%2 == 0 && ok {
			t.Fatalf("k%d still present after Evict", i)
		}
		if i%2 == 1 && !ok {
			t.Fatalf("k%d evicted but should not have been", i)
		}
	}
}

// A key must always land in the same shard, or Put and Get would disagree.
func TestShardedCache_RoutingIsStable(t *testing.T) {
	c := NewShardedLIRSCache(64, 0.95).(*shardedLIRSCache)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("stable-%d", i)
		first := c.shardFor(key)
		for j := 0; j < 5; j++ {
			if c.shardFor(key) != first {
				t.Fatalf("key %q routed to different shards across calls", key)
			}
		}
	}
}

// Keys should spread across shards; a routing bug that collapsed everything
// onto one shard would silently reintroduce the contention this exists to fix.
func TestShardedCache_KeysSpreadAcrossShards(t *testing.T) {
	c := NewShardedLIRSCache(64, 0.95).(*shardedLIRSCache)
	seen := make(map[*LIRSCache]int)
	const n = 10000
	for i := 0; i < n; i++ {
		seen[c.shardFor(fmt.Sprintf("spread-%d", i))]++
	}
	if len(seen) < len(c.shards)/2 {
		t.Fatalf("keys hit only %d of %d shards — routing is not spreading",
			len(seen), len(c.shards))
	}
	// No shard should take a wildly disproportionate share.
	limit := (n / len(c.shards)) * 5
	for shard, count := range seen {
		if count > limit {
			t.Errorf("shard %p took %d of %d keys (limit %d) — skewed routing",
				shard, count, n, limit)
		}
	}
}

func TestShardedCache_StatsAggregate(t *testing.T) {
	c := NewShardedLIRSCache(64, 0.95)
	for i := 0; i < 500; i++ {
		c.Put(fmt.Sprintf("s%d", i), []byte("payload"))
	}
	for i := 0; i < 500; i++ {
		c.Get(fmt.Sprintf("s%d", i)) // hits
	}
	for i := 0; i < 250; i++ {
		c.Get(fmt.Sprintf("absent%d", i)) // misses
	}

	st := c.Stats()
	if st.Hits != 500 {
		t.Errorf("Hits = %d, want 500", st.Hits)
	}
	if st.Misses != 250 {
		t.Errorf("Misses = %d, want 250", st.Misses)
	}
	if st.CurrentSizeBytes == 0 {
		t.Error("CurrentSizeBytes = 0 after 500 Puts")
	}
	if want := 500.0 / 750.0; st.HitRate < want-0.01 || st.HitRate > want+0.01 {
		t.Errorf("HitRate = %f, want ~%f", st.HitRate, want)
	}
	if st.MaxSizeBytes == 0 {
		t.Error("MaxSizeBytes = 0")
	}
	if got := c.Size(); got != st.CurrentSizeBytes {
		t.Errorf("Size() = %d but Stats().CurrentSizeBytes = %d", got, st.CurrentSizeBytes)
	}
}

// The whole point of sharding is concurrent access; make sure -race is clean
// and no update is lost.
func TestShardedCache_ConcurrentAccess(t *testing.T) {
	c := NewShardedLIRSCache(64, 0.95)
	const workers = 16
	const perWorker = 500

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				k := fmt.Sprintf("w%d-k%d", w, i)
				c.Put(k, []byte(k))
				if got, ok := c.Get(k); ok && string(got) != k {
					t.Errorf("key %q = %q", k, got)
					return
				}
				c.Stats()
				c.Size()
			}
		}(w)
	}
	wg.Wait()
}
