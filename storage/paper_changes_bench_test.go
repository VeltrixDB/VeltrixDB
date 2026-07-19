package storage

// paper_changes_bench_test.go — Micro-benchmarks for the 5 paper-derived changes.
//
// Run all:
//   go test -bench=. -benchmem -benchtime=3s ./storage/ -run ^$
//
// Run a single group:
//   go test -bench=BenchmarkGCColdFirstSort -benchmem ./storage/ -run ^$
//
// CI ratchet: the bench job captures the output with benchstat and fails
// if any benchmark regresses by more than 15% vs the stored baseline.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// ── change-2: Temperature-weighted GC sort ─────────────────────────────────

// BenchmarkGCColdFirstSort measures the sort of vlogGCEntry slices at
// realistic GC candidate counts.  The Scavenger+ paper targets 10–100K
// entries per GC pass.
func BenchmarkGCColdFirstSort_10K(b *testing.B) {
	benchGCSort(b, 10_000)
}

func BenchmarkGCColdFirstSort_100K(b *testing.B) {
	benchGCSort(b, 100_000)
}

func benchGCSort(b *testing.B, n int) {
	b.Helper()
	src := makeGCEntries(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := make([]vlogGCEntry, len(src))
		copy(entries, src)
		sort.Slice(entries, func(a, c int) bool {
			if entries[a].writeTimestampUs != entries[c].writeTimestampUs {
				return entries[a].writeTimestampUs < entries[c].writeTimestampUs
			}
			return entries[a].diskOffset < entries[c].diskOffset
		})
	}
}

func makeGCEntries(n int) []vlogGCEntry {
	r := rand.New(rand.NewSource(42))
	entries := make([]vlogGCEntry, n)
	for i := range entries {
		entries[i] = vlogGCEntry{
			key:              fmt.Sprintf("k%d", i),
			diskOffset:       uint64(r.Int63n(1 << 30)),
			writeTimestampUs: r.Int63n(1_000_000_000),
		}
	}
	return entries
}

// ── change-5: NopColdTier throughput ──────────────────────────────────────

// BenchmarkNopColdTier_Put: single-goroutine Put throughput.
func BenchmarkNopColdTier_Put(b *testing.B) {
	nop := NewNopColdTier()
	val := make([]byte, 512)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := fmt.Sprintf("%08x", i)
		_ = nop.Put(h, val)
	}
}

// BenchmarkNopColdTier_Get_Hit: Get on an existing handle.
func BenchmarkNopColdTier_Get_Hit(b *testing.B) {
	nop := NewNopColdTier()
	const n = 1000
	handles := make([]string, n)
	for i := range handles {
		handles[i] = fmt.Sprintf("%08x", i)
		_ = nop.Put(handles[i], make([]byte, 512))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = nop.Get(handles[i%n])
	}
}

// BenchmarkNopColdTier_PutParallel: concurrent Put throughput (simulates
// TierManager + multiple shard eviction goroutines).
func BenchmarkNopColdTier_PutParallel(b *testing.B) {
	nop := NewNopColdTier()
	val := make([]byte, 512)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			h := fmt.Sprintf("%016x", i)
			_ = nop.Put(h, val)
			i++
		}
	})
}

// ── change-5: PutColdTier with LocalFS backend ────────────────────────────

// BenchmarkPutColdTier_LocalFS: full path through PutColdTier → LocalFSColdTier.
// Measures CRC32C + atomic increment + file write latency on tmpfs.
func BenchmarkPutColdTier_LocalFS(b *testing.B) {
	dir := b.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		b.Fatalf("NewLocalFSColdTier: %v", err)
	}
	SetColdTier(tier)
	b.Cleanup(func() { SetColdTier(nil) })

	val := make([]byte, 1024) // 1 KB value
	b.SetBytes(int64(len(val)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		if _, err := PutColdTier(key, val); err != nil {
			b.Fatalf("PutColdTier: %v", err)
		}
	}
}

// BenchmarkGetColdTier_LocalFS: full path through GetColdTier → LocalFSColdTier.
func BenchmarkGetColdTier_LocalFS(b *testing.B) {
	dir := b.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		b.Fatalf("NewLocalFSColdTier: %v", err)
	}
	SetColdTier(tier)
	b.Cleanup(func() { SetColdTier(nil) })

	val := make([]byte, 1024)
	const n = 200
	handles := make([]string, n)
	for i := range handles {
		h, _ := PutColdTier(fmt.Sprintf("k%d", i), val)
		handles[i] = h
	}

	b.SetBytes(int64(len(val)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := GetColdTier(handles[i%n]); err != nil {
			b.Fatalf("GetColdTier: %v", err)
		}
	}
}

// ── change-5: TierManager.EnqueueCandidate throughput ────────────────────

// BenchmarkTierManager_Enqueue: measures the non-blocking channel send.
func BenchmarkTierManager_Enqueue(b *testing.B) {
	tm := &TierManager{
		candidates: make(chan DemotionCandidate, 8192),
		done:       make(chan struct{}),
	}
	c := DemotionCandidate{Key: "bench-key", DiskOffset: 12345, ValueSize: 512}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Drain periodically so the channel never fills for this benchmark.
		select {
		case tm.candidates <- c:
		default:
			for len(tm.candidates) > 0 {
				<-tm.candidates
			}
			tm.candidates <- c
		}
	}
}
