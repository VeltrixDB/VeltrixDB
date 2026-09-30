package storage

// hnsw_bench_test.go — HNSW build / query cost and recall under deletes.
//
//	go test ./storage -run '^$' -bench HNSW -benchtime 2000x
//
// Recall is reported as a custom metric (recall@10) next to ns/op.

import (
	"fmt"
	"math/rand"
	"testing"
)

const benchN, benchDim = 20000, 128

func benchIndex(b *testing.B, deleteFrac float64) (*VectorIndex, map[string][]float32) {
	return benchIndexQ(b, deleteFrac, false)
}

func benchIndexQ(b *testing.B, deleteFrac float64, quant bool) (*VectorIndex, map[string][]float32) {
	b.Helper()
	r := rand.New(rand.NewSource(1))
	vi := newTestIndex(benchDim)
	vi.quant = quant
	ref := make(map[string][]float32, benchN)
	vi.mu.Lock()
	for i := 0; i < benchN; i++ {
		id := fmt.Sprintf("b%06d", i)
		v := randVec(r, benchDim)
		ref[id] = v
		vi.insertHNSW(id, v)
	}
	for i := 0; i < int(deleteFrac*benchN); i++ {
		id := fmt.Sprintf("b%06d", i)
		vi.removeHNSW(id)
		delete(ref, id)
	}
	vi.mu.Unlock()
	return vi, ref
}

func benchSearch(b *testing.B, deleteFrac float64) {
	vi, ref := benchIndex(b, deleteFrac)
	r := rand.New(rand.NewSource(2))
	queries := make([][]float32, 64)
	for i := range queries {
		queries[i] = randVec(r, benchDim)
	}
	hits, total := 0, 0
	for _, q := range queries {
		want := bruteTopK(ref, q, 10)
		got := map[string]bool{}
		for _, m := range vi.searchHNSW(q, 10) {
			got[m.ID] = true
		}
		for _, id := range want {
			total++
			if got[id] {
				hits++
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vi.mu.RLock()
		vi.searchHNSW(queries[i%len(queries)], 10)
		vi.mu.RUnlock()
	}
	b.ReportMetric(float64(hits)/float64(total), "recall@10")
}

func BenchmarkHNSW_Search(b *testing.B)              { benchSearch(b, 0) }
func BenchmarkHNSW_Search_Deleted50pct(b *testing.B) { benchSearch(b, 0.5) }

func BenchmarkHNSW_Insert(b *testing.B) {
	r := rand.New(rand.NewSource(3))
	vecs := make([][]float32, b.N)
	for i := range vecs {
		vecs[i] = randVec(r, benchDim)
	}
	vi := newTestIndex(benchDim)
	b.ResetTimer()
	vi.mu.Lock()
	for i := 0; i < b.N; i++ {
		vi.insertHNSW(fmt.Sprintf("i%07d", i), vecs[i])
	}
	vi.mu.Unlock()
}

// BenchmarkHNSW_SearchEf sweeps the query beam width (VSEARCH ... EF n).
// Uniform random 128-dim vectors have a high intrinsic dimension, which is
// a hard case for graph search — measure recall on real embeddings before
// picking an ef.
func BenchmarkHNSW_SearchEf(b *testing.B) {
	vi, ref := benchIndex(b, 0)
	r := rand.New(rand.NewSource(2))
	queries := make([][]float32, 64)
	for i := range queries {
		queries[i] = randVec(r, benchDim)
	}
	for _, ef := range []int{64, 128, 256, 512} {
		b.Run(fmt.Sprintf("ef=%d", ef), func(b *testing.B) {
			hits, total := 0, 0
			for _, q := range queries {
				got := map[string]bool{}
				for _, m := range vi.searchFiltered(q, 10, ef, nil) {
					got[m.ID] = true
				}
				for _, id := range bruteTopK(ref, q, 10) {
					total++
					if got[id] {
						hits++
					}
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vi.mu.RLock()
				vi.searchFiltered(queries[i%len(queries)], 10, ef, nil)
				vi.mu.RUnlock()
			}
			b.ReportMetric(float64(hits)/float64(total), "recall@10")
		})
	}
}

// BenchmarkHNSW_Int8 compares the graph walk on float32 and int8 nodes at
// ef=256: RAM per index (MB), walk latency, and recall@10 of the walk
// BEFORE the engine's full-precision re-rank of 4×k candidates.
func BenchmarkHNSW_Int8(b *testing.B) {
	for _, quant := range []bool{false, true} {
		name := "f32"
		if quant {
			name = "int8"
		}
		b.Run(name, func(b *testing.B) {
			vi, ref := benchIndexQ(b, 0, quant)
			r := rand.New(rand.NewSource(2))
			queries := make([][]float32, 64)
			for i := range queries {
				queries[i] = randVec(r, benchDim)
			}
			hits, total := 0, 0
			for _, q := range queries {
				got := map[string]bool{}
				for _, m := range vi.searchFiltered(q, 40, 256, nil) {
					got[m.ID] = true
				}
				for _, id := range bruteTopK(ref, q, 10) {
					total++
					if got[id] {
						hits++
					}
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vi.mu.RLock()
				vi.searchFiltered(queries[i%len(queries)], 40, 256, nil)
				vi.mu.RUnlock()
			}
			b.ReportMetric(float64(hits)/float64(total), "recall@10-in-top40")
			b.ReportMetric(float64(vi.hnswStatsBytes())/(1<<20), "MB")
		})
	}
}
