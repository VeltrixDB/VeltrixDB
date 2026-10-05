package storage

// search_delete_test.go — deleting a KV record deletes its vectors and text
// documents in every namespace (search_hooks.go deleteDerivedSearchKeys);
// vectors / documents that never had a record are left alone.

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

// searchDelFixture writes n records with a vector in ns "v" and a text
// document in ns "t" (and in "t2" for even ids), plus pure-vector /
// pure-text ids ("pure-*") that have no record.
type searchDelFixture struct {
	vecs map[string][]float32
}

func writeSearchDelFixture(t *testing.T, se *StorageEngine, n, dim int) *searchDelFixture {
	t.Helper()
	r := rand.New(rand.NewSource(41))
	f := &searchDelFixture{vecs: map[string][]float32{}}
	for i := 0; i < n; i++ {
		for _, id := range []string{fmt.Sprintf("rec%04d", i), fmt.Sprintf("pure%04d", i)} {
			v := randVec(r, dim)
			f.vecs[id] = v
			if err := se.PutVector("v", id, v); err != nil {
				t.Fatal(err)
			}
			if err := se.PutText("t", id, "common words "+id); err != nil {
				t.Fatal(err)
			}
			if i%2 == 0 {
				if err := se.PutText("t2", id, "other "+id); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := se.Put(fmt.Sprintf("rec%04d", i), []byte(`{"kind":"rec"}`), -1); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func hitIDs(m []VectorMatch) map[string]bool {
	out := make(map[string]bool, len(m))
	for _, h := range m {
		out[h.ID] = true
	}
	return out
}

// checkSearchAfterDelete: deleted records are absent from vector (k = 0 and
// self-query), text and hybrid results and their reserved keys are gone;
// every other record and every pure id is still found.
func checkSearchAfterDelete(t *testing.T, se *StorageEngine, f *searchDelFixture, n int, deleted func(i int) bool) {
	t.Helper()
	all, err := se.SearchVectorWithOptions("v", f.vecs["pure0000"], 0, VectorSearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	vecHits := hitIDs(all)
	txt, err := se.SearchText("t", "common", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	txtHits := hitIDs(txt)
	for i := 0; i < n; i++ {
		rec, pure := fmt.Sprintf("rec%04d", i), fmt.Sprintf("pure%04d", i)
		if !vecHits[pure] || !txtHits[pure] {
			t.Fatalf("pure id %s lost (vector %v, text %v)", pure, vecHits[pure], txtHits[pure])
		}
		gone := deleted(i)
		if vecHits[rec] == gone || txtHits[rec] == gone {
			t.Fatalf("%s deleted=%v but vector hit=%v text hit=%v", rec, gone, vecHits[rec], txtHits[rec])
		}
		for _, k := range []string{VectorPersistKey("v", rec), TextPersistKey("t", rec), TextPersistKey("t2", rec)} {
			if k == TextPersistKey("t2", rec) && i%2 != 0 {
				continue
			}
			_, gerr := se.Get(k)
			if gone != errors.Is(gerr, ErrKeyNotFound) {
				t.Fatalf("%s deleted=%v but Get(%s) err=%v", rec, gone, k, gerr)
			}
		}
		if t2, _ := se.SearchText("t2", rec, 1, nil); i%2 == 0 && (len(t2) == 1) == gone {
			t.Fatalf("t2 search for %s (deleted=%v): %+v", rec, gone, t2)
		}
		if i%7 == 0 {
			got, err := se.SearchVectorWithOptions("v", f.vecs[rec], 1, VectorSearchOptions{Ef: 128})
			if err != nil {
				t.Fatal(err)
			}
			if gone && len(got) > 0 && got[0].ID == rec {
				t.Fatalf("self-query found deleted %s", rec)
			}
			hy, err := se.SearchHybrid("v", f.vecs[rec], rec, 5, HybridOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if hitIDs(hy)[rec] == gone {
				t.Fatalf("hybrid for %s (deleted=%v): %+v", rec, gone, hy)
			}
		}
	}
}

// TestSearch_DeleteRecordRemovesVectorsAndText covers every memory layout.
func TestSearch_DeleteRecordRemovesVectorsAndText(t *testing.T) {
	layouts := map[string]VectorNamespaceOptions{
		"float32":   {},
		"int8":      {Quantization: QuantInt8},
		"pq":        {Quantization: QuantPQ, PQSubspaces: 4, PQTrainAt: 256},
		"pq-disk":   {Quantization: QuantPQ, PQSubspaces: 4, PQTrainAt: 256, Graph: GraphDisk},
		"disk-only": {Graph: GraphDisk},
	}
	const n, dim = 200, 16 // 400 vectors, 333 after the deletes: PQ trains at 256
	for name, opts := range layouts {
		opts := opts
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			se := openSearchEngine(t, dir)
			if err := se.CreateVectorNamespace("v", dim, opts); err != nil {
				t.Fatal(err)
			}
			f := writeSearchDelFixture(t, se, n, dim)
			if opts.Quantization == QuantPQ {
				waitPQTrained(t, se, "v")
			}
			deleted := func(i int) bool { return i%3 == 0 }
			for i := 0; i < n; i++ {
				if deleted(i) {
					if err := se.Delete(fmt.Sprintf("rec%04d", i)); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Unrelated deletes: a key that never existed and an ordinary
			// key, neither of which is a pure id's record.
			_ = se.Delete("pure0001")
			_ = se.Put("unrelated", []byte("x"), -1)
			_ = se.Delete("unrelated")
			checkSearchAfterDelete(t, se, f, n, deleted)

			// Clean restart: still gone (the derived keys were deleted).
			se.Close()
			se = openSearchEngine(t, dir)
			defer se.Close()
			if _, _, err := se.RebuildSearchIndexes(); err != nil {
				t.Fatal(err)
			}
			if opts.Quantization == QuantPQ {
				waitPQTrained(t, se, "v")
			}
			checkSearchAfterDelete(t, se, f, n, deleted)
		})
	}
}

// TestSearch_DeleteRecordBatchAndTxn: deletes inside a transaction (the
// batched TXN / MULTI path) and a record re-created after its delete.
func TestSearch_DeleteRecordBatchAndTxn(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	if err := se.RegisterVectorNamespace("v", 2); err != nil {
		t.Fatal(err)
	}
	reqs := []MultiPutRequest{}
	for _, id := range []string{"a", "b", "c"} {
		reqs = append(reqs, MultiPutRequest{Key: id, Value: []byte("{}"), TTL: -1},
			MultiPutRequest{Key: VectorPersistKey("v", id), Value: encodeVector([]float32{1, 0}), TTL: -1})
	}
	for _, err := range se.MultiPut(reqs) {
		if err != nil {
			t.Fatal(err)
		}
	}
	txn := se.BeginTxn()
	txn.Delete("a")
	txn.Delete("b")
	txn.Set("c", []byte(`{"v":2}`), -1)
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := hitIDs(mustSearch(t, se, "v", []float32{1, 0})); got["a"] || got["b"] || !got["c"] {
		t.Fatalf("after txn delete: %v, want only c", got)
	}
	// A vector written after its record's delete is a new, record-less vector.
	if err := se.PutVector("v", "a", []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	if got := hitIDs(mustSearch(t, se, "v", []float32{1, 0})); !got["a"] {
		t.Fatalf("vector re-added after delete is missing: %v", got)
	}
}

func mustSearch(t *testing.T, se *StorageEngine, ns string, q []float32) []VectorMatch {
	t.Helper()
	m, err := se.SearchVector(ns, q, 0)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestSearch_CrashBetweenRecordAndDerivedDelete: a crash after the record's
// tombstone but before its vector / text deletes must not leave them
// searchable after restart — the rebuild deletes them durably. A pure vector,
// and a vector written after the record's delete, survive.
func TestSearch_CrashBetweenRecordAndDerivedDelete(t *testing.T) {
	dir := t.TempDir()
	se := openSearchEngine(t, dir)
	defer se.Close()
	if err := se.RegisterVectorNamespace("v", 2); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"gone", "pure", "back"} {
		if err := se.PutVector("v", id, []float32{1, 0}); err != nil {
			t.Fatal(err)
		}
		if err := se.PutText("t", id, "word "+id); err != nil {
			t.Fatal(err)
		}
	}
	_ = se.Put("gone", []byte("{}"), -1)
	_ = se.Put("back", []byte("{}"), -1)

	skipDerivedDeleteForTest.Store(true)
	err1, err2 := se.Delete("gone"), se.Delete("back")
	skipDerivedDeleteForTest.Store(false)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	// "back" got a new vector after its record's delete: it must stay.
	if err := se.PutVector("v", "back", []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	img := copyCrashImage(t, dir)

	re := openSearchEngine(t, img)
	defer re.Close()
	nv, nd, err := re.RebuildSearchIndexes()
	if err != nil {
		t.Fatal(err)
	}
	// Loaded: pure + back's new vector; pure's document only.
	if nv != 2 || nd != 1 {
		t.Fatalf("rebuild loaded %d vectors / %d docs, want 2 / 1 (orphans excluded)", nv, nd)
	}
	if got := hitIDs(mustSearch(t, re, "v", []float32{1, 0})); got["gone"] || !got["pure"] || !got["back"] {
		t.Fatalf("vectors after crash restart: %v, want pure + back", got)
	}
	txt, _ := re.SearchText("t", "word", 0, nil)
	if got := hitIDs(txt); got["gone"] || !got["pure"] || got["back"] {
		// "back"'s text was written before the delete and never rewritten.
		t.Fatalf("text after crash restart: %v, want only pure", got)
	}
	for _, k := range []string{VectorPersistKey("v", "gone"), TextPersistKey("t", "gone")} {
		if _, err := re.Get(k); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("orphan %s not deleted by the rebuild: %v", k, err)
		}
	}
}

// TestSearch_DeleteWithoutNamespaces: no search namespace → Delete never
// looks for derived keys (and still works).
func TestSearch_DeleteWithoutNamespaces(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	if se.vectors.namespaces() != nil || se.texts.namespaces() != nil {
		t.Fatal("fresh engine has search namespaces")
	}
	_ = se.Put("k", []byte("v"), -1)
	if err := se.Delete("k"); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() { se.deleteDerivedSearchKeys("k") })
	if allocs != 0 {
		t.Fatalf("cascade with no namespaces allocates %.0f times", allocs)
	}
}
