package storage

// vector_memory_test.go — RAM per vector for each layout at a real embedding
// size (768-dim). Prints a table; no gates (see search_quality_test.go).
//
//	VELTRIX_VECTOR_MEMORY=1 go test ./storage -run TestVectorMemoryTable -v

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestVectorMemoryTable(t *testing.T) {
	if os.Getenv("VELTRIX_VECTOR_MEMORY") == "" {
		t.Skip("set VELTRIX_VECTOR_MEMORY=1 (builds four 768-dim indexes)")
	}
	const n, dim, k, nq = 10000, 768, 10, 50
	// Per-component noise scaled so noise/centroid is the same ratio as the
	// 64-dim quality gate (0.3 at dim 64).
	noise := float32(0.3 * math.Sqrt(64.0/dim))
	data := clusteredSet(11, n, dim, 40, noise)
	queries := clusteredSet(12, nq, dim, 40, noise)
	ids := make([]string, n)
	ref := map[string][]float32{}
	for i := range ids {
		ids[i] = fmt.Sprintf("m%06d", i)
		ref[ids[i]] = data[i]
	}
	truth := make([][]string, nq)
	for i, q := range queries {
		truth[i] = bruteTopK(ref, q, k)
	}

	layouts := []struct {
		name string
		cfg  vectorNSConfig
	}{
		{"float32", vectorNSConfig{Dim: dim}},
		{"int8", vectorNSConfig{Dim: dim, Quant: QuantInt8}},
		{"pq m=96", vectorNSConfig{Dim: dim, Quant: QuantPQ, PQM: 96, PQTrainAt: 5000}},
		{"pq m=96 + disk graph", vectorNSConfig{Dim: dim, Quant: QuantPQ, PQM: 96, PQTrainAt: 5000, Graph: GraphDisk}},
	}
	t.Logf("%-22s %12s %12s %8s %8s %8s %8s", "layout", "heap B/vec", "mapped B/vec", "R@10 64", "q ef64", "R@10 256", "q ef256")
	for _, l := range layouts {
		c := l.cfg
		if err := c.normalize(); err != nil {
			t.Fatal(err)
		}
		// Each layout gets its own copy: a float32 index keeps the slices it
		// is given, so shared input would hide its vector memory.
		own := make([][]float32, n)
		before := heapInUse()
		for i, v := range data {
			own[i] = append([]float32(nil), v...)
		}
		vi := c.newIndex(t.TempDir())
		var wg sync.WaitGroup
		work := make(chan int, 256)
		for w := 0; w < runtime.GOMAXPROCS(0); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range work {
					vi.insert(ids[i], own[i])
				}
			}()
		}
		for i := range ids {
			work <- i
		}
		close(work)
		wg.Wait()
		for vi.pqMode {
			vi.mu.RLock()
			done := vi.pq != nil && !vi.pqTraining
			vi.mu.RUnlock()
			if done {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		own = nil // PQ / int8 indexes drop their float32 copies once encoded
		heap := heapInUse() - before
		var mapped int64
		if vi.adj0 != nil {
			mapped = vi.adj0.bytes()
		}

		// Recall of the engine's search at two beam widths, re-ranking the
		// candidates the engine would (rerankPlan) for approximate layouts.
		row := fmt.Sprintf("%-22s %12d %12d", l.name, heap/uint64(n), mapped/int64(n))
		for _, efReq := range []int{hnswEfSearch, 256} {
			hits := 0
			t0 := time.Now()
			for qi, q := range queries {
				kk, ef := k, efReq
				if vi.approximate() {
					kk, ef = rerankPlan(vi.pqMode, k, efReq)
				}
				vi.mu.RLock()
				got := vi.searchFiltered(q, kk, ef, nil)
				vi.mu.RUnlock()
				if vi.approximate() {
					for i := range got {
						got[i].Score = dot(q, ref[got[i].ID])
					}
					sort.Slice(got, func(i, j int) bool { return got[i].Score > got[j].Score })
					if len(got) > k {
						got = got[:k]
					}
				}
				in := map[string]bool{}
				for _, m := range got {
					in[m.ID] = true
				}
				for _, id := range truth[qi] {
					if in[id] {
						hits++
					}
				}
			}
			per := time.Since(t0) / nq
			row += fmt.Sprintf(" %8.3f %8v", float64(hits)/float64(nq*k), per.Round(time.Microsecond))
		}
		t.Log(row)
		if vi.adj0 != nil {
			vi.adj0.close()
		}
		runtime.KeepAlive(vi)
	}
}
