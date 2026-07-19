package storage

// bloom_test.go — unit tests for the lock-free per-shard Bloom filter.
//
// newShardBloom is exercised directly without a full StorageEngine.

import (
	"sync"
	"testing"
)

// fnvHash is a minimal FNV-1a 64-bit hash for test key generation,
// matching the hash used by the engine for shard routing.
func fnvHash(key string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return h
}

// TestBloom_NoFalseNegative verifies that every key Add'd to the filter
// is subsequently reported by MayContain.
//
// A Bloom filter must NEVER produce a false negative: if the key was added,
// MayContain must return true.
func TestBloom_NoFalseNegative(t *testing.T) {
	const n = 10_000
	b := newShardBloom(1<<20 /*1M bits*/, 7)

	hashes := make([]uint64, n)
	for i := 0; i < n; i++ {
		key := "bloom-test-key-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26%10))
		h := fnvHash(key)
		hashes[i] = h
		b.Add(h)
	}

	for i, h := range hashes {
		if !b.MayContain(h) {
			t.Errorf("false negative for index %d hash %016x", i, h)
		}
	}
}

// TestBloom_FalsePositiveRate measures the false-positive rate on a set of
// keys that were NEVER added.  With 1M bits and 7 hashes the theoretical FP
// rate for 10K keys is ~1%; we allow up to 5%.
func TestBloom_FalsePositiveRate(t *testing.T) {
	const nAdded = 10_000
	const nProbe = 10_000
	b := newShardBloom(1<<20, 7)

	// Add nAdded keys with one hash space.
	for i := 0; i < nAdded; i++ {
		key := "added-" + string(rune('A'+i%52)) + string(rune('a'+i%26)) + "-suffix"
		b.Add(fnvHash(key))
	}

	// Probe nProbe completely different keys.
	fp := 0
	for i := 0; i < nProbe; i++ {
		key := "notadded-" + string(rune('Z'-i%26)) + "-" + string(rune('z'-i%26)) + "-probe"
		if b.MayContain(fnvHash(key)) {
			fp++
		}
	}

	fpRate := float64(fp) / float64(nProbe)
	if fpRate > 0.05 {
		t.Errorf("false-positive rate %.4f exceeds 5%% (%d/%d)", fpRate, fp, nProbe)
	}
}

// TestBloom_AtomicSafety verifies that concurrent Add calls from 100
// goroutines do not race.  This test is meaningful when run with -race.
func TestBloom_AtomicSafety(t *testing.T) {
	b := newShardBloom(1<<16, 7)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := "atomic-" + string(rune('A'+g%26)) + string(rune('0'+i%10))
				b.Add(fnvHash(key))
			}
		}()
	}
	wg.Wait()
	// No race-detector complaint = pass.
}

// TestBloom_Reset verifies that Reset() zeros all bits, so keys Added before
// Reset() are no longer found afterwards.
func TestBloom_Reset(t *testing.T) {
	b := newShardBloom(1<<14, 7)

	// Add 100 keys.
	hashes := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		key := "pre-reset-" + string(rune('A'+i%26))
		h := fnvHash(key)
		hashes[i] = h
		b.Add(h)
	}

	// All should be present before reset.
	for i, h := range hashes {
		if !b.MayContain(h) {
			t.Fatalf("false negative before reset at index %d", i)
		}
	}

	b.Reset()

	// After reset, all probes should return false (no false positives in an
	// empty filter — all bits are 0 so MayContain returns false immediately).
	for i, h := range hashes {
		if b.MayContain(h) {
			t.Errorf("key %d found after Reset() — expected empty filter", i)
		}
	}
}

// TestBloom_SizeBytes verifies that SizeBytes() returns the expected memory
// footprint: len(bits) × 8 bytes.
func TestBloom_SizeBytes(t *testing.T) {
	// 1M bits = 128 KB.
	b := newShardBloom(1<<20, 7)
	sz := b.SizeBytes()
	// The filter rounds up to the next power-of-2 words, so the actual bit
	// count is at least 1M.  Each word is 8 bytes.
	expectedMin := (1 << 20) / 8 // 128 KB
	if sz < expectedMin {
		t.Errorf("SizeBytes() = %d, want >= %d", sz, expectedMin)
	}
}

// TestBloom_Probe_ZeroHash exercises the edge case of hash=0 (both h1 and h2 are 0).
// All probes land on bit 0 of word 0 — Add should set that bit and MayContain return true.
func TestBloom_Probe_ZeroHash(t *testing.T) {
	b := newShardBloom(1<<10, 7)
	b.Add(0)
	if !b.MayContain(0) {
		t.Error("MayContain(0) = false after Add(0)")
	}
}
