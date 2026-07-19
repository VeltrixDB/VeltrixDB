package storage

// cache_test.go — unit tests for the LIRS data cache.
//
// Tests exercise NewLIRSCache directly via the Cache interface and via the
// concrete *LIRSCache type when we need to inspect internal state (Stats).

import (
	"fmt"
	"sync"
	"testing"
)

// smallValue returns a slice of exactly n bytes.
func smallValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i & 0xFF)
	}
	return b
}

// ── Basic operations ──────────────────────────────────────────────────────────

func TestLIRS_BasicPutGet(t *testing.T) {
	c := NewLIRSCache(1 /*MB*/, 0.95)
	c.Put("k1", []byte("val1"))

	got, ok := c.Get("k1")
	if !ok {
		t.Fatal("Get k1: not found")
	}
	if string(got) != "val1" {
		t.Errorf("Get k1 = %q, want %q", got, "val1")
	}
}

func TestLIRS_MissingKey(t *testing.T) {
	c := NewLIRSCache(1, 0.95)
	_, ok := c.Get("nonexistent")
	if ok {
		t.Error("expected miss for nonexistent key, got hit")
	}
}

func TestLIRS_Evict(t *testing.T) {
	c := NewLIRSCache(1, 0.95)
	c.Put("to-evict", []byte("data"))
	c.Evict("to-evict")
	_, ok := c.Get("to-evict")
	if ok {
		t.Error("key still present after Evict")
	}
}

// ── Eviction on capacity ──────────────────────────────────────────────────────

// TestLIRS_BasicEviction fills a tiny cache past capacity and verifies that
// the total cached bytes never exceed the limit.
func TestLIRS_BasicEviction(t *testing.T) {
	// 1 MB cache; fill with 2 MB of data (100 × 20 KB each).
	const cacheMB = 1
	const entrySize = 20 * 1024 // 20 KB
	const numEntries = 100      // 2 MB total

	c := NewLIRSCache(cacheMB, 0.95)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("evict-%d", i)
		c.Put(key, smallValue(entrySize))
	}

	stats := c.Stats()
	maxBytes := uint64(cacheMB) * 1024 * 1024
	if stats.CurrentSizeBytes > maxBytes {
		t.Errorf("cache exceeded limit: size=%d max=%d", stats.CurrentSizeBytes, maxBytes)
	}
	if stats.Evictions == 0 {
		t.Error("expected at least one eviction, got 0")
	}
}

// TestLIRS_SizeAccounting verifies that Size() tracks the sum of stored bytes
// and stays within the configured limit.
func TestLIRS_SizeAccounting(t *testing.T) {
	const cacheMB = 2
	c := NewLIRSCache(cacheMB, 0.95)

	// Insert 10 × 100 KB = 1 MB — should all fit.
	for i := 0; i < 10; i++ {
		c.Put(fmt.Sprintf("sz-%d", i), smallValue(100*1024))
	}

	sz := c.Size()
	maxBytes := uint64(cacheMB) * 1024 * 1024
	if sz > maxBytes {
		t.Errorf("Size() = %d exceeds max %d", sz, maxBytes)
	}
	if sz == 0 {
		t.Error("Size() = 0 after inserts")
	}
}

// ── Hit-rate (hot keys stay in cache) ────────────────────────────────────────

// TestLIRS_HitRate accesses a small "hot" key set repeatedly against a
// larger "cold" background.  The hot keys should remain in cache.
func TestLIRS_HitRate(t *testing.T) {
	// 1 MB cache, 100 B hot values — all hot keys must fit.
	c := NewLIRSCache(1, 0.95)

	const hotKeys = 50
	const coldKeys = 500
	const hotValSize = 100 // bytes

	// Insert cold background.
	for i := 0; i < coldKeys; i++ {
		c.Put(fmt.Sprintf("cold-%d", i), smallValue(1000))
	}

	// Insert and warm up hot keys.
	for i := 0; i < hotKeys; i++ {
		key := fmt.Sprintf("hot-%d", i)
		c.Put(key, smallValue(hotValSize))
		// Access each hot key multiple times to register as high-frequency.
		for j := 0; j < 5; j++ {
			c.Get(key)
		}
	}

	// Flush cold background again to compete with hot keys.
	for i := 0; i < coldKeys; i++ {
		c.Put(fmt.Sprintf("cold2-%d", i), smallValue(1000))
	}

	// Hot keys should still be in cache (LIRS LIR property).
	hotHits := 0
	for i := 0; i < hotKeys; i++ {
		key := fmt.Sprintf("hot-%d", i)
		if _, ok := c.Get(key); ok {
			hotHits++
		}
	}
	// Expect at least 80% of hot keys to survive — LIRS may demote a few
	// under tiny-cache pressure, but the majority should stay.
	minExpected := hotKeys * 8 / 10
	if hotHits < minExpected {
		t.Errorf("hot key retention too low: %d/%d (want >= %d)", hotHits, hotKeys, minExpected)
	}
}

// ── Small-value priority ──────────────────────────────────────────────────────

// TestLIRS_SmallValuePriority verifies that small values (≤ smallValueThreshold)
// receive priority=2 from the cache, while large values receive priority=1.
// We verify this by checking the priority field directly after insertion.
func TestLIRS_SmallValuePriority(t *testing.T) {
	lc := NewLIRSCache(4, 0.95).(*LIRSCache)

	smallVal := smallValue(64)   // 64 B ≤ smallValueThreshold (256 B)
	largeVal := smallValue(1024) // 1 KB > smallValueThreshold

	lc.Put("small-key", smallVal)
	lc.Put("large-key", largeVal)

	lc.mu.Lock()
	smallNode := lc.index["small-key"]
	largeNode := lc.index["large-key"]
	lc.mu.Unlock()

	if smallNode == nil {
		t.Fatal("small-key not found in index")
	}
	if largeNode == nil {
		t.Fatal("large-key not found in index")
	}
	if smallNode.priority != 2 {
		t.Errorf("small value priority = %d, want 2", smallNode.priority)
	}
	if largeNode.priority != 1 {
		t.Errorf("large value priority = %d, want 1", largeNode.priority)
	}
}

// ── Concurrent access ─────────────────────────────────────────────────────────

// TestLIRS_ConcurrentAccess runs 16 goroutines simultaneously doing Gets and
// Puts.  The test will fail with a data race if the cache's mutex is broken.
func TestLIRS_ConcurrentAccess(t *testing.T) {
	c := NewLIRSCache(4, 0.95)

	const goroutines = 16
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("conc-%d-%d", g, i%50)
				c.Put(key, smallValue(128))
				c.Get(key)
				if i%10 == 0 {
					c.Evict(key)
				}
			}
		}()
	}
	wg.Wait()
	// If we get here without a race-detector complaint, the test passes.
}

// ── Stats ─────────────────────────────────────────────────────────────────────

func TestLIRS_Stats(t *testing.T) {
	c := NewLIRSCache(1, 0.95)

	c.Put("s1", []byte("val1"))
	c.Put("s2", []byte("val2"))

	// One hit, one miss.
	c.Get("s1")
	c.Get("missing")

	stats := c.Stats()
	if stats.Hits == 0 {
		t.Error("expected at least one hit, got 0")
	}
	if stats.Misses == 0 {
		t.Error("expected at least one miss, got 0")
	}
	if stats.HitRate <= 0 || stats.HitRate > 1 {
		t.Errorf("HitRate out of range: %f", stats.HitRate)
	}
	if stats.MaxSizeBytes == 0 {
		t.Error("MaxSizeBytes should be non-zero")
	}
}
