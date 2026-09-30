package storage

// vector_search_test.go — HNSW maintenance (updates, compaction, concurrent
// inserts), filtered search and per-engine vector registries.

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// recallAt measures mean top-k recall of vi against an exact scan of ref
// over nq random queries.
func recallAt(t *testing.T, vi *VectorIndex, ref map[string][]float32, r *rand.Rand, k, nq int) float64 {
	t.Helper()
	hits, total := 0, 0
	for i := 0; i < nq; i++ {
		q := randVec(r, vi.dim)
		want := bruteTopK(ref, q, k)
		vi.mu.RLock()
		got := vi.searchHNSW(q, k)
		vi.mu.RUnlock()
		in := map[string]bool{}
		for _, m := range got {
			in[m.ID] = true
		}
		for _, id := range want {
			total++
			if in[id] {
				hits++
			}
		}
	}
	return float64(hits) / float64(total)
}

// clusteredVec returns a unit vector near one of nClusters random centres —
// the shape where plain top-M neighbour selection loses recall.
func clusteredVec(r *rand.Rand, centres [][]float32) []float32 {
	c := centres[r.Intn(len(centres))]
	v := make([]float32, len(c))
	for i := range v {
		v[i] = c[i] + 0.05*float32(r.NormFloat64())
	}
	n, _ := normalizeVector(v)
	return n
}

func TestHNSW_ClusteredRecall(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	const n, dim, clusters = 3000, 32, 30
	centres := make([][]float32, clusters)
	for i := range centres {
		centres[i] = randVec(r, dim)
	}
	vi := newTestIndex(dim)
	ref := map[string][]float32{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%05d", i)
		v := clusteredVec(r, centres)
		ref[id] = v
		vi.insert(id, v)
	}
	if rec := recallAt(t, vi, ref, r, 10, 50); rec < 0.9 {
		t.Fatalf("clustered recall@10 = %.3f < 0.9", rec)
	} else {
		t.Logf("clustered recall@10 = %.3f", rec)
	}
}

// TestHNSW_UpdateRelinks: after moving half the vectors, search must find
// them at their NEW positions with the same recall as fresh inserts.
func TestHNSW_UpdateRelinks(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	const n, dim = 2000, 32
	vi := newTestIndex(dim)
	ref := map[string][]float32{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("u%05d", i)
		ref[id] = randVec(r, dim)
		vi.insert(id, ref[id])
	}
	for i := 0; i < n; i += 2 {
		id := fmt.Sprintf("u%05d", i)
		ref[id] = randVec(r, dim)
		vi.insert(id, ref[id])
	}
	if vi.live != n {
		t.Fatalf("live = %d after updates, want %d", vi.live, n)
	}
	if rec := recallAt(t, vi, ref, r, 10, 50); rec < 0.9 {
		t.Fatalf("recall after updates = %.3f < 0.9", rec)
	}
	// Self-queries of moved vectors must return exactly that id, once.
	for i := 0; i < 100; i += 2 {
		id := fmt.Sprintf("u%05d", i)
		vi.mu.RLock()
		got := vi.searchHNSW(ref[id], 5)
		vi.mu.RUnlock()
		if len(got) == 0 || got[0].ID != id {
			t.Fatalf("moved %s not found at new position: %+v", id, got)
		}
		seen := map[string]bool{}
		for _, m := range got {
			if seen[m.ID] {
				t.Fatalf("duplicate id %s in results %+v", m.ID, got)
			}
			seen[m.ID] = true
		}
	}
}

// TestHNSW_CompactionAfterDeletes: deleting past the threshold triggers a
// background rebuild that drops the snapshot's tombstones and keeps recall.
func TestHNSW_CompactionAfterDeletes(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	const n, dim = 3000, 16
	vi := newTestIndex(dim)
	ref := map[string][]float32{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("d%05d", i)
		ref[id] = randVec(r, dim)
		vi.insert(id, ref[id])
	}
	for i := 0; i < 2000; i++ {
		id := fmt.Sprintf("d%05d", i)
		vi.remove(id)
		delete(ref, id)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		vi.mu.RLock()
		done := vi.compactions > 0 && !vi.compacting
		nodes, live := len(vi.nodes), vi.live
		vi.mu.RUnlock()
		if done {
			// Compaction fires at the 1024th delete; the deletes after its
			// snapshot replay onto the fresh graph as tombstones, which stay
			// until the next threshold crossing.
			if live != n-2000 || nodes-live >= hnswCompactMinDead || nodes >= n {
				t.Fatalf("after compaction nodes=%d live=%d, want live=%d and < %d tombstones", nodes, live, n-2000, hnswCompactMinDead)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("compaction did not run")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rec := recallAt(t, vi, ref, r, 10, 50); rec < 0.9 {
		t.Fatalf("recall after compaction = %.3f < 0.9", rec)
	}
}

// TestHNSW_CompactionReplaysConcurrentWrites: writes that land while the
// fresh graph is being built must survive the swap.
func TestHNSW_CompactionReplaysConcurrentWrites(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	const dim = 8
	vi := newTestIndex(dim)
	for i := 0; i < 1500; i++ {
		vi.insert(fmt.Sprintf("p%05d", i), randVec(r, dim))
	}

	// Start a compaction by hand, then write before it finishes.
	vi.mu.Lock()
	vi.compacting = true
	snap := vi.liveSnapshotLocked()
	vi.mu.Unlock()

	late := randVec(r, dim)
	vi.insert("late", late)
	vi.remove("p00000")
	vi.compact(snap)

	vi.mu.RLock()
	defer vi.mu.RUnlock()
	if _, ok := vi.byID["p00000"]; ok {
		t.Fatal("delete made during compaction was lost")
	}
	got := vi.searchHNSW(late, 1)
	if len(got) != 1 || got[0].ID != "late" {
		t.Fatalf("insert made during compaction was lost: %+v", got)
	}
	if vi.live != 1500 {
		t.Fatalf("live = %d, want 1500", vi.live)
	}
}

// TestHNSW_ConcurrentInsertSearch: parallel inserts (read-locked planning)
// and searches must not race (run with -race) and must lose no vector.
func TestHNSW_ConcurrentInsertSearch(t *testing.T) {
	const workers, per, dim = 8, 250, 16
	vi := newTestIndex(dim)
	var wg sync.WaitGroup
	refMu := sync.Mutex{}
	ref := map[string][]float32{}
	for w := 0; w < workers; w++ {
		wg.Add(2)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(100 + w)))
			for i := 0; i < per; i++ {
				id := fmt.Sprintf("w%d-%d", w, i)
				v := randVec(r, dim)
				vi.insert(id, v)
				refMu.Lock()
				ref[id] = v
				refMu.Unlock()
			}
		}(w)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(200 + w)))
			for i := 0; i < per; i++ {
				vi.mu.RLock()
				vi.searchHNSW(randVec(r, dim), 5)
				vi.mu.RUnlock()
			}
		}(w)
	}
	wg.Wait()
	if vi.live != workers*per || len(vi.byID) != workers*per {
		t.Fatalf("live=%d byID=%d, want %d", vi.live, len(vi.byID), workers*per)
	}
	if rec := recallAt(t, vi, ref, rand.New(rand.NewSource(1)), 10, 50); rec < 0.9 {
		t.Fatalf("recall after concurrent build = %.3f < 0.9", rec)
	}
}

// TestVector_FilteredSearch: every result satisfies the filter, and the top
// hit equals the exact filtered nearest neighbour — both with and without a
// secondary index on the field.
func TestVector_FilteredSearch(t *testing.T) {
	se := newTestEngine(t)
	<-se.ReplayDone
	const n, dim = 600, 8
	if err := se.RegisterVectorNamespace("docs", dim); err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(21))
	langs := []string{"en", "de", "fr"}
	vecs := map[string][]float32{}
	lang := map[string]string{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		l := langs[i%3]
		if i%50 == 0 {
			l = "hi" // rare value: 12 docs
		}
		lang[id] = l
		if err := se.Put(id, []byte(fmt.Sprintf(`{"lang":%q,"n":%d}`, l, i)), -1); err != nil {
			t.Fatal(err)
		}
		v := randVec(r, dim)
		vecs[id] = v
		if err := se.PutVector("docs", id, v); err != nil {
			t.Fatal(err)
		}
	}

	check := func(label, want string, q []float32) {
		t.Helper()
		got, err := se.SearchVectorWithOptions("docs", q, 5, VectorSearchOptions{
			Filter: &VectorFilter{Field: "lang", Op: "=", Value: want},
		})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(got) != 5 {
			t.Fatalf("%s: got %d results, want 5", label, len(got))
		}
		for _, m := range got {
			if lang[m.ID] != want {
				t.Fatalf("%s: result %s has lang %s, want %s", label, m.ID, lang[m.ID], want)
			}
		}
		subset := map[string][]float32{}
		for id, v := range vecs {
			if lang[id] == want {
				subset[id] = v
			}
		}
		if exact := bruteTopK(subset, q, 1)[0]; got[0].ID != exact {
			t.Fatalf("%s: top hit %s, exact filtered nearest %s", label, got[0].ID, exact)
		}
	}

	for i := 0; i < 10; i++ {
		q := randVec(r, dim)
		check("scan/en", "en", q)
		check("scan/hi", "hi", q)
	}
	if err := se.CreateFieldIndex("by_lang", "lang"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		q := randVec(r, dim)
		check("index/en", "en", q)
		check("index/hi", "hi", q)
	}

	// No match at all → empty, not an error.
	got, err := se.SearchVectorWithOptions("docs", randVec(r, dim), 5, VectorSearchOptions{
		Filter: &VectorFilter{Field: "lang", Op: "=", Value: "xx"},
	})
	if err != nil || len(got) != 0 {
		t.Fatalf("no-match filter: got %+v err=%v", got, err)
	}
	// Range filter through the scan path.
	got, err = se.SearchVectorWithOptions("docs", randVec(r, dim), 5, VectorSearchOptions{
		Filter: &VectorFilter{Field: "n", Op: "<", Value: "100"},
	})
	if err != nil || len(got) != 5 {
		t.Fatalf("range filter: got %d results err=%v", len(got), err)
	}
	for _, m := range got {
		var i int
		fmt.Sscanf(m.ID, "doc-%d", &i)
		if i >= 100 {
			t.Fatalf("range filter returned %s", m.ID)
		}
	}
	if _, err := se.SearchVectorWithOptions("docs", randVec(r, dim), 5, VectorSearchOptions{
		Filter: &VectorFilter{Field: "lang", Op: "~", Value: "en"},
	}); err == nil {
		t.Fatal("bad filter op must fail")
	}
}

// TestVector_RegistryPerEngine: two engines in one process keep separate
// vector namespaces (the registry used to be a package global).
func TestVector_RegistryPerEngine(t *testing.T) {
	a, b := newTestEngine(t), newTestEngine(t)
	if err := a.RegisterVectorNamespace("shared", 3); err != nil {
		t.Fatal(err)
	}
	if err := b.RegisterVectorNamespace("shared", 5); err != nil {
		t.Fatalf("second engine saw the first engine's namespace: %v", err)
	}
	if err := a.PutVector("shared", "x", []float32{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if m, _ := b.SearchVector("shared", []float32{1, 0, 0, 0, 0}, 1); len(m) != 0 {
		t.Fatalf("engine b returned engine a's vector: %+v", m)
	}
}

// TestVector_DeleteSurvivesRestart: a deleted vector must not come back when
// the index is rebuilt from the persisted keys.
func TestVector_DeleteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	se := newTestEngineWithDir(t, dir)
	<-se.ReplayDone
	if err := se.RegisterVectorNamespace("r", 2); err != nil {
		t.Fatal(err)
	}
	_ = se.PutVector("r", "keep", []float32{1, 0})
	_ = se.PutVector("r", "gone", []float32{0, 1})
	if err := se.DeleteVector("r", "gone"); err != nil {
		t.Fatal(err)
	}
	se.Close()

	se = newTestEngineWithDir(t, dir)
	defer se.Close()
	<-se.ReplayDone
	if n, err := se.RebuildVectorIndexes(); err != nil || n != 1 {
		t.Fatalf("rebuild loaded %d (err %v), want 1", n, err)
	}
	m, err := se.SearchVector("r", []float32{0, 1}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || m[0].ID != "keep" {
		t.Fatalf("after restart: %+v, want only keep", m)
	}
}
