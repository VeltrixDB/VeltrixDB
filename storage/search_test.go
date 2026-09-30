package storage

// search_test.go — quantized vector namespaces, reserved-key hooks, BM25
// text search and hybrid (RRF) fusion.

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func openSearchEngine(t *testing.T, dir string) *StorageEngine {
	t.Helper()
	se := newTestEngineWithDir(t, dir)
	<-se.ReplayDone
	return se
}

func vecStats(t *testing.T, se *StorageEngine, ns string) VectorStats {
	t.Helper()
	for _, st := range se.VectorIndexStats() {
		if st.Namespace == ns {
			return st
		}
	}
	t.Fatalf("namespace %q not in stats", ns)
	return VectorStats{}
}

// TestVector_QuantizedNamespace: int8 namespaces keep ~dim+4 bytes per vector
// in RAM, return exact (re-ranked) scores and keep recall; the setting
// survives a restart.
func TestVector_QuantizedNamespace(t *testing.T) {
	dir := t.TempDir()
	se := openSearchEngine(t, dir)
	const n, dim, k = 1500, 64, 10
	if err := se.CreateVectorNamespace("q", dim, VectorNamespaceOptions{Quantization: QuantInt8}); err != nil {
		t.Fatal(err)
	}
	if err := se.CreateVectorNamespace("f", dim, VectorNamespaceOptions{}); err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(8))
	ref := map[string][]float32{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("v%05d", i)
		v := randVec(r, dim)
		ref[id] = v
		if err := se.PutVector("q", id, v); err != nil {
			t.Fatal(err)
		}
		if err := se.PutVector("f", id, v); err != nil {
			t.Fatal(err)
		}
	}

	qs, fs := vecStats(t, se, "q"), vecStats(t, se, "f")
	if qs.Quantization != QuantInt8 || qs.Count != n {
		t.Fatalf("quantized stats = %+v", qs)
	}
	// Vector payload: dim+4 vs 4×dim bytes; adjacency is the same for both.
	vecSaving := int64(n) * int64(4*dim-(dim+4))
	if got := fs.Bytes - qs.Bytes; got < vecSaving*9/10 {
		t.Fatalf("int8 saved %d bytes, want ≈ %d (f32 %d, int8 %d)", got, vecSaving, fs.Bytes, qs.Bytes)
	}

	hits, total := 0, 0
	for i := 0; i < 30; i++ {
		q := randVec(r, dim)
		want := bruteTopK(ref, q, k)
		got, err := se.SearchVector("q", q, k)
		if err != nil {
			t.Fatal(err)
		}
		in := map[string]bool{}
		for _, m := range got {
			in[m.ID] = true
			// Re-ranked scores are exact cosines.
			if exact := dot(q, ref[m.ID]); math.Abs(float64(exact-m.Score)) > 1e-4 {
				t.Fatalf("score %f for %s, exact %f", m.Score, m.ID, exact)
			}
		}
		for _, id := range want {
			total++
			if in[id] {
				hits++
			}
		}
	}
	if rec := float64(hits) / float64(total); rec < 0.9 {
		t.Fatalf("int8 recall@%d = %.3f < 0.9", k, rec)
	} else {
		t.Logf("int8 recall@%d = %.3f", k, rec)
	}
	se.Close()

	se = openSearchEngine(t, dir)
	defer se.Close()
	if nv, _, err := se.RebuildSearchIndexes(); err != nil || nv != 2*n {
		t.Fatalf("rebuild loaded %d vectors (err %v), want %d", nv, err, 2*n)
	}
	if st := vecStats(t, se, "q"); st.Quantization != QuantInt8 || st.Count != n {
		t.Fatalf("after restart: %+v, want int8 with %d vectors", st, n)
	}
}

// TestVector_ReconfigureQuantization: switching an existing namespace to int8
// and back re-encodes its vectors without losing any.
func TestVector_ReconfigureQuantization(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	r := rand.New(rand.NewSource(4))
	if err := se.RegisterVectorNamespace("m", 16); err != nil {
		t.Fatal(err)
	}
	vecs := map[string][]float32{}
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("m%03d", i)
		vecs[id] = randVec(r, 16)
		if err := se.PutVector("m", id, vecs[id]); err != nil {
			t.Fatal(err)
		}
	}
	for _, quant := range []string{QuantInt8, QuantNone} {
		if err := se.CreateVectorNamespace("m", 16, VectorNamespaceOptions{Quantization: quant}); err != nil {
			t.Fatalf("switch to %s: %v", quant, err)
		}
		st := vecStats(t, se, "m")
		if st.Quantization != quant || st.Count != 200 {
			t.Fatalf("after switch to %s: %+v", quant, st)
		}
		got, err := se.SearchVector("m", vecs["m042"], 1)
		if err != nil || len(got) != 1 || got[0].ID != "m042" {
			t.Fatalf("%s self-query: %+v err=%v", quant, got, err)
		}
	}
	if err := se.CreateVectorNamespace("m", 32, VectorNamespaceOptions{}); err == nil {
		t.Fatal("changing the dimension must fail")
	}
	if err := se.CreateVectorNamespace("m", 16, VectorNamespaceOptions{Quantization: "pq"}); err == nil {
		t.Fatal("unknown quantization must fail")
	}
}

// TestSearchHooks_PlainKVWrites: a vector or document written as a plain KV
// Put / Delete / MultiPut of its reserved key — the path replication, raft
// and partition transfer take — is indexed and un-indexed.
func TestSearchHooks_PlainKVWrites(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	v, _ := normalizeVector([]float32{1, 2, 3})
	if err := se.Put(VectorPersistKey("h", "a"), encodeVector(v), -1); err != nil {
		t.Fatal(err)
	}
	if err := se.Put(TextPersistKey("h", "a"), []byte("replicated document"), -1); err != nil {
		t.Fatal(err)
	}
	errs := se.MultiPut([]MultiPutRequest{
		{Key: TextPersistKey("h", "b"), Value: []byte("batched document"), TTL: -1},
	})
	if errs[0] != nil {
		t.Fatal(errs[0])
	}
	if m, err := se.SearchVector("h", []float32{1, 2, 3}, 1); err != nil || len(m) != 1 || m[0].ID != "a" {
		t.Fatalf("vector via plain Put: %+v err=%v", m, err)
	}
	if m, _ := se.SearchText("h", "document", 5, nil); len(m) != 2 {
		t.Fatalf("text via Put/MultiPut: %+v, want a and b", m)
	}

	if err := se.Delete(VectorPersistKey("h", "a")); err != nil {
		t.Fatal(err)
	}
	if err := se.Delete(TextPersistKey("h", "a")); err != nil {
		t.Fatal(err)
	}
	if m, _ := se.SearchVector("h", []float32{1, 2, 3}, 1); len(m) != 0 {
		t.Fatalf("vector survived plain Delete: %+v", m)
	}
	if m, _ := se.SearchText("h", "document", 5, nil); len(m) != 1 || m[0].ID != "b" {
		t.Fatalf("text after Delete: %+v, want only b", m)
	}
	// PutVector on the hook path indexes exactly once (no update tombstone).
	if err := se.PutVector("h", "c", []float32{3, 2, 1}); err != nil {
		t.Fatal(err)
	}
	if st := vecStats(t, se, "h"); st.Count != 1 || st.Tombstones != 1 {
		t.Fatalf("stats %+v, want 1 live + 1 tombstone (the deleted a)", st)
	}
}

func TestText_BM25Ranking(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	docs := map[string]string{
		"d1": "the quick brown fox jumps over the lazy dog",
		"d2": "quick quick quick fox",
		"d3": "a completely unrelated sentence about databases",
		"d4": "fox",
		"d5": "नमस्ते दुनिया, VeltrixDB में आपका स्वागत है",
	}
	for id, text := range docs {
		if err := se.PutText("t", id, text); err != nil {
			t.Fatal(err)
		}
	}
	got, err := se.SearchText("t", "Quick FOX", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	// d2 has three "quick" and a "fox"; d1 has both once but is long; d4 is
	// only "fox".
	if len(got) != 3 || got[0].ID != "d2" || got[1].ID != "d1" || got[2].ID != "d4" {
		t.Fatalf("ranking = %+v, want d2, d1, d4", got)
	}
	if got, _ := se.SearchText("t", "दुनिया", 5, nil); len(got) != 1 || got[0].ID != "d5" {
		t.Fatalf("Devanagari search = %+v", got)
	}
	if _, err := se.SearchText("t", " ,.! ", 5, nil); err == nil {
		t.Fatal("query without terms must fail")
	}

	// Update drops old terms; delete removes the document.
	if err := se.PutText("t", "d4", "badger"); err != nil {
		t.Fatal(err)
	}
	if got, _ := se.SearchText("t", "fox", 10, nil); len(got) != 2 {
		t.Fatalf("after update: %+v, want d1 and d2", got)
	}
	if err := se.DeleteText("t", "d2"); err != nil {
		t.Fatal(err)
	}
	if got, _ := se.SearchText("t", "quick", 10, nil); len(got) != 1 || got[0].ID != "d1" {
		t.Fatalf("after delete: %+v, want d1", got)
	}

	// Filter on the KV record with the same id.
	_ = se.Put("d1", []byte(`{"lang":"en"}`), -1)
	if got, _ := se.SearchText("t", "fox", 10, &VectorFilter{Field: "lang", Op: "=", Value: "de"}); len(got) != 0 {
		t.Fatalf("filtered = %+v, want none", got)
	}
	if got, _ := se.SearchText("t", "fox", 10, &VectorFilter{Field: "lang", Op: "=", Value: "en"}); len(got) != 1 {
		t.Fatalf("filtered = %+v, want d1", got)
	}
}

// TestText_GlobalStatsMatchSingleNode: splitting a corpus across two engines
// and scoring each half with the summed statistics reproduces the scores of
// one engine holding everything — what distributed text search relies on.
func TestText_GlobalStatsMatchSingleNode(t *testing.T) {
	all := openSearchEngine(t, t.TempDir())
	defer all.Close()
	a := openSearchEngine(t, t.TempDir())
	defer a.Close()
	b := openSearchEngine(t, t.TempDir())
	defer b.Close()
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	r := rand.New(rand.NewSource(12))
	for i := 0; i < 60; i++ {
		n := 3 + r.Intn(10)
		text := ""
		for j := 0; j < n; j++ {
			text += words[r.Intn(len(words))] + " "
		}
		id := fmt.Sprintf("doc%02d", i)
		_ = all.PutText("c", id, text)
		if i%2 == 0 {
			_ = a.PutText("c", id, text)
		} else {
			_ = b.PutText("c", id, text)
		}
	}
	const q = "alpha zeta"
	st := a.TextSearchStats("c", q)
	st.Add(b.TextSearchStats("c", q))
	want, _ := all.SearchText("c", q, 0, nil)
	ha, _ := a.SearchTextWithStats("c", q, 0, nil, &st)
	hb, _ := b.SearchTextWithStats("c", q, 0, nil, &st)
	got := append(ha, hb...)
	SortMatches(got)
	if len(got) != len(want) {
		t.Fatalf("got %d hits, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || math.Abs(float64(got[i].Score-want[i].Score)) > 1e-5 {
			t.Fatalf("hit %d: %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestHybrid_FuseRRF(t *testing.T) {
	v := []VectorMatch{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	x := []VectorMatch{{ID: "c"}, {ID: "d"}}
	got := FuseRRF(v, x, HybridOptions{}, 0)
	// c is in both lists, so it must win despite ranking last on vectors.
	if got[0].ID != "c" {
		t.Fatalf("fused = %+v, want c first", got)
	}
	wantC := 0.5/float64(rrfK+3) + 0.5/float64(rrfK+1)
	if math.Abs(float64(got[0].Score)-wantC) > 1e-6 {
		t.Fatalf("c score %f, want %f", got[0].Score, wantC)
	}
	one, zero := 1.0, 0.0
	if got := FuseRRF(v, x, HybridOptions{Alpha: &one}, 0); len(got) != 3 || got[0].ID != "a" {
		t.Fatalf("alpha=1 → %+v, want the vector list", got)
	}
	if got := FuseRRF(v, x, HybridOptions{Alpha: &zero}, 0); len(got) != 2 || got[0].ID != "c" {
		t.Fatalf("alpha=0 → %+v, want the text list", got)
	}
}

func TestHybrid_Search(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	// "sem" is closest to the query vector, "kw" is the only keyword match,
	// "both" is second on each list — the hybrid winner.
	put := func(id string, vec []float32, text string) {
		if err := se.RegisterVectorNamespace("hy", 2); err != nil {
			t.Fatal(err)
		}
		if err := se.PutVector("hy", id, vec); err != nil {
			t.Fatal(err)
		}
		if err := se.PutText("hy", id, text); err != nil {
			t.Fatal(err)
		}
	}
	put("sem", []float32{1, 0}, "nothing relevant here")
	put("both", []float32{0.9, 0.44}, "veltrix storage engine notes")
	put("kw", []float32{-1, 0}, "veltrix veltrix veltrix")
	put("far", []float32{0, -1}, "other words")

	got, err := se.SearchHybrid("hy", []float32{1, 0}, "veltrix", 2, HybridOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "both" {
		t.Fatalf("hybrid = %+v, want both first", got)
	}
	// Text-only and vector-only halves.
	if got, err := se.SearchHybrid("hy", nil, "veltrix", 1, HybridOptions{}); err != nil || got[0].ID != "kw" {
		t.Fatalf("text-only hybrid = %+v err=%v", got, err)
	}
	if got, err := se.SearchHybrid("hy", []float32{1, 0}, "", 1, HybridOptions{}); err != nil || got[0].ID != "sem" {
		t.Fatalf("vector-only hybrid = %+v err=%v", got, err)
	}
	if _, err := se.SearchHybrid("nope", nil, "", 1, HybridOptions{}); err == nil {
		t.Fatal("hybrid with neither half must fail")
	}
	bad := 1.5
	if _, err := se.SearchHybrid("hy", nil, "veltrix", 1, HybridOptions{Alpha: &bad}); err == nil {
		t.Fatal("alpha > 1 must fail")
	}
}

// TestText_RebuildAfterRestart: documents are re-indexed from "@txt/" keys.
func TestText_RebuildAfterRestart(t *testing.T) {
	dir := t.TempDir()
	se := openSearchEngine(t, dir)
	_ = se.PutText("r", "x", "persistent words")
	_ = se.PutText("r", "y", "gone soon")
	_ = se.DeleteText("r", "y")
	se.Close()
	se = openSearchEngine(t, dir)
	defer se.Close()
	if _, nd, err := se.RebuildSearchIndexes(); err != nil || nd != 1 {
		t.Fatalf("rebuild docs = %d err=%v, want 1", nd, err)
	}
	if got, _ := se.SearchText("r", "persistent", 5, nil); len(got) != 1 || got[0].ID != "x" {
		t.Fatalf("after restart: %+v", got)
	}
	if got, _ := se.SearchText("r", "gone", 5, nil); len(got) != 0 {
		t.Fatalf("deleted doc came back: %+v", got)
	}
}

// TestVector_FilteredGraphPath: an indexed "=" filter whose allow-list is
// larger than vectorBruteForceMax walks the graph restricted to members.
func TestVector_FilteredGraphPath(t *testing.T) {
	old := vectorBruteForceMax
	vectorBruteForceMax = 10
	defer func() { vectorBruteForceMax = old }()

	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	if err := se.CreateFieldIndex("by_color", "color"); err != nil {
		t.Fatal(err)
	}
	_ = se.RegisterVectorNamespace("g", 8)
	r := rand.New(rand.NewSource(33))
	red := map[string][]float32{}
	for i := 0; i < 400; i++ {
		id := fmt.Sprintf("g%03d", i)
		color := "blue"
		if i%4 == 0 {
			color = "red" // 100 ids > vectorBruteForceMax
		}
		_ = se.Put(id, []byte(fmt.Sprintf(`{"color":%q}`, color)), -1)
		v := randVec(r, 8)
		if color == "red" {
			red[id] = v
		}
		if err := se.PutVector("g", id, v); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		q := randVec(r, 8)
		got, err := se.SearchVectorWithOptions("g", q, 5, VectorSearchOptions{Filter: &VectorFilter{Field: "color", Op: "=", Value: "red"}})
		if err != nil || len(got) != 5 {
			t.Fatalf("got %d err=%v", len(got), err)
		}
		for _, m := range got {
			if _, ok := red[m.ID]; !ok {
				t.Fatalf("%s is not red", m.ID)
			}
		}
		if exact := bruteTopK(red, q, 1)[0]; got[0].ID != exact {
			t.Fatalf("top %s, exact filtered nearest %s", got[0].ID, exact)
		}
	}
	if _, err := se.SearchVector("missing", []float32{1}, 1); !errors.Is(err, ErrVectorNamespaceNotFound) {
		t.Fatalf("missing namespace error = %v, want ErrVectorNamespaceNotFound", err)
	}
}

// TestIndexDef_ReplicatedKey: an "@idxdef/<name>" Put (what a replica
// applies in replicated mode) creates the index and backfills it; its
// tombstone drops it again.
func TestIndexDef_ReplicatedKey(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	_ = se.Put("u1", []byte(`{"city":"pune"}`), -1)
	if err := se.Put(IndexDefKey("by_city_repl"), []byte("city"), -1); err != nil {
		t.Fatal(err)
	}
	if keys := se.LookupBySecondary("by_city_repl", "pune"); len(keys) != 1 || keys[0] != "u1" {
		t.Fatalf("index from replicated def: %v, want [u1]", keys)
	}
	_ = se.Put("u2", []byte(`{"city":"pune"}`), -1)
	if keys := se.LookupBySecondary("by_city_repl", "pune"); len(keys) != 2 {
		t.Fatalf("new write not indexed: %v", keys)
	}
	if err := se.Delete(IndexDefKey("by_city_repl")); err != nil {
		t.Fatal(err)
	}
	for _, d := range se.ListFieldIndexes() {
		if d.Name == "by_city_repl" {
			t.Fatal("tombstoned def left the index defined")
		}
	}
	if keys := se.LookupBySecondary("by_city_repl", "pune"); len(keys) != 0 {
		t.Fatalf("entries survived the drop: %v", keys)
	}
}

// TestReservedKeys_NotSecondaryIndexed: text documents and settings keys
// look like "k=v" / JSON records to a field extractor, but must never get
// index entries of their own.
func TestReservedKeys_NotSecondaryIndexed(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	if err := se.CreateFieldIndex("by_dim_rk", "dim"); err != nil {
		t.Fatal(err)
	}
	defer se.DropFieldIndex("by_dim_rk")
	_ = se.PutText("rk", "d", "dim=8")
	_ = se.CreateVectorNamespace("rk", 8, VectorNamespaceOptions{})
	_ = se.Put("plain", []byte("dim=8"), -1)
	keys := se.LookupBySecondary("by_dim_rk", "8")
	if len(keys) != 1 || keys[0] != "plain" {
		t.Fatalf("index entries = %v, want only [plain]", keys)
	}
}

func TestTokenize_KeepsCombiningMarks(t *testing.T) {
	cases := map[string][]string{
		"नमस्ते दुनिया":     {"नमस्ते", "दुनिया"},
		"Hello, World! 42x": {"hello", "world", "42x"},
		"café résumé":       {"café", "résumé"},
		"தமிழ் மொழி":        {"தமிழ்", "மொழி"},
	}
	for in, want := range cases {
		got := Tokenize(in)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("Tokenize(%q) = %q, want %q", in, got, want)
		}
	}
}
