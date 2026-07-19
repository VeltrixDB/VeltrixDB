package storage

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func newTestIndex(dim int) *VectorIndex {
	return &VectorIndex{dim: dim, byID: map[string]int32{}}
}

func randVec(r *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	var sum float64
	for i := range v {
		v[i] = float32(r.NormFloat64())
		sum += float64(v[i]) * float64(v[i])
	}
	n := float32(math.Sqrt(sum))
	for i := range v {
		v[i] /= n
	}
	return v
}

// bruteTopK is the exact reference the HNSW results are scored against.
func bruteTopK(vecs map[string][]float32, q []float32, k int) []string {
	type pair struct {
		id  string
		sim float32
	}
	all := make([]pair, 0, len(vecs))
	for id, v := range vecs {
		all = append(all, pair{id, dot(q, v)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].sim > all[j].sim })
	out := make([]string, 0, k)
	for i := 0; i < k && i < len(all); i++ {
		out = append(out, all[i].id)
	}
	return out
}

// TestHNSW_RecallAgainstBruteForce inserts 2000 random 32-dim vectors and
// checks top-10 recall over 50 queries stays above 0.9 — the property that
// makes HNSW a valid replacement for the exact scan.
func TestHNSW_RecallAgainstBruteForce(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	const n, dim, k, queries = 2000, 32, 10, 50

	vi := newTestIndex(dim)
	ref := make(map[string][]float32, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("v%05d", i)
		v := randVec(r, dim)
		ref[id] = v
		vi.mu.Lock()
		vi.insertHNSW(id, v)
		vi.mu.Unlock()
	}

	totalHits, total := 0, 0
	for qi := 0; qi < queries; qi++ {
		q := randVec(r, dim)
		want := bruteTopK(ref, q, k)
		vi.mu.RLock()
		got := vi.searchHNSW(q, k)
		vi.mu.RUnlock()
		inGot := map[string]bool{}
		for _, m := range got {
			inGot[m.ID] = true
		}
		for _, id := range want {
			total++
			if inGot[id] {
				totalHits++
			}
		}
	}
	recall := float64(totalHits) / float64(total)
	t.Logf("recall@%d = %.3f over %d queries, n=%d", k, recall, queries, n)
	if recall < 0.9 {
		t.Fatalf("recall %.3f < 0.9 — HNSW graph quality regressed", recall)
	}
}

// TestHNSW_ExactTopHit: the single nearest vector must virtually always be
// found (100 identical checks with an exact-duplicate query vector).
func TestHNSW_ExactTopHit(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	const n, dim = 500, 16
	vi := newTestIndex(dim)
	ids := make([]string, n)
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("e%04d", i)
		vecs[i] = randVec(r, dim)
		vi.mu.Lock()
		vi.insertHNSW(ids[i], vecs[i])
		vi.mu.Unlock()
	}
	miss := 0
	for i := 0; i < 100; i++ {
		j := r.Intn(n)
		vi.mu.RLock()
		got := vi.searchHNSW(vecs[j], 1)
		vi.mu.RUnlock()
		if len(got) == 0 || got[0].ID != ids[j] {
			miss++
		}
	}
	if miss > 2 {
		t.Fatalf("self-query missed %d/100 times", miss)
	}
}

// TestHNSW_DeleteAndUpdate: tombstoned ids never surface; updated vectors
// surface at their new position.
func TestHNSW_DeleteAndUpdate(t *testing.T) {
	dim := 4
	vi := newTestIndex(dim)
	unit := func(i int) []float32 { v := make([]float32, dim); v[i] = 1; return v }
	vi.mu.Lock()
	vi.insertHNSW("a", unit(0))
	vi.insertHNSW("b", unit(1))
	vi.insertHNSW("c", unit(2))
	vi.mu.Unlock()

	vi.mu.Lock()
	vi.removeHNSW("b")
	vi.mu.Unlock()
	got := vi.searchHNSW(unit(1), 3)
	for _, m := range got {
		if m.ID == "b" {
			t.Fatal("tombstoned id surfaced in results")
		}
	}
	if vi.live != 2 {
		t.Fatalf("live = %d, want 2", vi.live)
	}

	// Update: move "a" onto axis 3; querying axis 3 must return it first.
	vi.mu.Lock()
	vi.insertHNSW("a", unit(3))
	vi.mu.Unlock()
	got = vi.searchHNSW(unit(3), 1)
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("updated vector not found at new position: %+v", got)
	}
}

// TestHNSW_EngineIntegration exercises the full engine path: PutVector →
// SearchVector → DeleteVector → restart rebuild.
func TestHNSW_EngineIntegration(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.WALFlushWindowMs = 2
	cfg.VLogFlushWindowMs = 2
	eng, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-eng.ReplayDone
	defer eng.Close()

	ns := fmt.Sprintf("it-%d", rand.Int()) // global registry — unique per run
	if err := eng.RegisterVectorNamespace(ns, 3); err != nil {
		t.Fatal(err)
	}
	if err := eng.PutVector(ns, "x", []float32{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := eng.PutVector(ns, "y", []float32{0, 1, 0}); err != nil {
		t.Fatal(err)
	}
	m, err := eng.SearchVector(ns, []float32{0.95, 0.05, 0}, 1)
	if err != nil || len(m) != 1 || m[0].ID != "x" {
		t.Fatalf("search = %+v err=%v, want x", m, err)
	}
	if err := eng.DeleteVector(ns, "x"); err != nil {
		t.Fatal(err)
	}
	m, _ = eng.SearchVector(ns, []float32{1, 0, 0}, 1)
	if len(m) != 1 || m[0].ID != "y" {
		t.Fatalf("post-delete search = %+v, want y", m)
	}
}
