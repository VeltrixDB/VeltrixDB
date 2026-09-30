package storage

// search_quality_test.go — recall / ranking regression gate for vector,
// filtered, quantized, text and hybrid search.
//
// Fixed seeds, fixed data, fixed thresholds: a change that makes search
// return worse results fails here even when every functional test passes.
// The thresholds sit a few points under what the current code measures (the
// measured values are logged), so noise from map iteration order cannot trip
// them but a real regression — a broken neighbour heuristic, a quantization
// bug, a tokenizer change — does. CI runs this in the "search" job.
//
//	go test ./storage -run TestSearchQualityGate -v

import (
	"fmt"
	"math/rand"
	"testing"
)

// Gate thresholds, recall@10. Measured on 2026-09-30, darwin/arm64 (the data
// and graph build are deterministic; other architectures can differ in the
// last digit because arm64 fuses multiply-adds): f32 0.927, f32 ef=256
// 0.998, int8 0.923, pq 0.882, pq + disk graph 0.881, filtered 0.994, after
// deletes 0.959–0.965 (depends on when the background compaction
// snapshots). The gates sit ~3 points lower. When a change moves a measured value, update the numbers
// here with the gate.
const (
	gateRecallF32      = 0.90 // clustered data, default ef (64)
	gateRecallF32Ef256 = 0.97
	gateRecallInt8     = 0.89 // after the full-precision re-rank
	gateRecallFiltered = 0.96 // 25 %-selective indexed filter, graph path
	gateRecallDeleted  = 0.92 // after deleting 40 % (compaction runs)
	gateRecallPQ       = 0.85 // pq m=16 (4 dims per code byte) + re-rank of the beam
	gateRecallDiskPQ   = 0.85 // same, layer-0 edges in the mapped file
)

const (
	gateN        = 5000
	gateDim      = 64
	gateClusters = 50
	gateQueries  = 100
	gateK        = 10
)

// gateData returns clustered unit vectors (the shape of real embeddings:
// dense clusters, not uniform noise) and held-out queries near the same
// clusters.
func gateData() (ids []string, vecs [][]float32, queries [][]float32) {
	r := rand.New(rand.NewSource(20260930))
	centres := make([][]float32, gateClusters)
	for i := range centres {
		centres[i] = randVec(r, gateDim)
	}
	sample := func() []float32 {
		c := centres[r.Intn(gateClusters)]
		v := make([]float32, gateDim)
		for i := range v {
			v[i] = c[i] + 0.3*float32(r.NormFloat64())
		}
		n, _ := normalizeVector(v)
		return n
	}
	for i := 0; i < gateN; i++ {
		ids = append(ids, fmt.Sprintf("g%05d", i))
		vecs = append(vecs, sample())
	}
	for i := 0; i < gateQueries; i++ {
		queries = append(queries, sample())
	}
	return ids, vecs, queries
}

// loadGate writes the vectors as reserved keys in one MultiPut per 500 (the
// Put hooks index them) plus a record per id with a "shard" field.
func loadGate(t *testing.T, se *StorageEngine, ns string, ids []string, vecs [][]float32) {
	t.Helper()
	var batch []MultiPutRequest
	flush := func() {
		for _, err := range se.MultiPut(batch) {
			if err != nil {
				t.Fatal(err)
			}
		}
		batch = batch[:0]
	}
	for i, id := range ids {
		batch = append(batch,
			MultiPutRequest{Key: VectorPersistKey(ns, id), Value: encodeVector(vecs[i]), TTL: -1},
			MultiPutRequest{Key: id, Value: []byte(fmt.Sprintf(`{"shard":"s%d"}`, i%4)), TTL: -1})
		if len(batch) >= 1000 {
			flush()
		}
	}
	flush()
}

// measureRecall is mean recall@k of search against an exact scan of ref.
func measureRecall(t *testing.T, ref map[string][]float32, queries [][]float32, k int,
	search func(q []float32) ([]VectorMatch, error)) float64 {
	t.Helper()
	hits, total := 0, 0
	for _, q := range queries {
		got, err := search(q)
		if err != nil {
			t.Fatal(err)
		}
		in := map[string]bool{}
		for _, m := range got {
			in[m.ID] = true
		}
		for _, id := range bruteTopK(ref, q, k) {
			total++
			if in[id] {
				hits++
			}
		}
	}
	return float64(hits) / float64(total)
}

func gate(t *testing.T, name string, got, min float64) {
	t.Helper()
	t.Logf("%-28s recall@%d = %.3f (gate %.2f)", name, gateK, got, min)
	if got < min {
		t.Errorf("%s: recall@%d %.3f is below the gate %.2f — search quality regressed", name, gateK, got, min)
	}
}

func TestSearchQualityGate(t *testing.T) {
	if testing.Short() {
		t.Skip("search quality gate builds 4 × 5000-vector indexes")
	}
	if raceEnabled {
		t.Skip("quality gate is single-threaded; CI runs it without -race (search job)")
	}
	ids, vecs, queries := gateData()
	ref := make(map[string][]float32, gateN)
	for i, id := range ids {
		ref[id] = vecs[i]
	}

	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	if err := se.CreateVectorNamespace("gf", gateDim, VectorNamespaceOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := se.CreateVectorNamespace("gq", gateDim, VectorNamespaceOptions{Quantization: QuantInt8}); err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"gp", "gd"} {
		opts := VectorNamespaceOptions{Quantization: QuantPQ, PQSubspaces: 16, PQTrainAt: 2000}
		if ns == "gd" {
			opts.Graph = GraphDisk
		}
		if err := se.CreateVectorNamespace(ns, gateDim, opts); err != nil {
			t.Fatal(err)
		}
	}
	loadGate(t, se, "gf", ids, vecs)
	loadGate(t, se, "gq", ids, vecs)
	loadGate(t, se, "gp", ids, vecs)
	loadGate(t, se, "gd", ids, vecs)
	waitPQTrained(t, se, "gp")
	waitPQTrained(t, se, "gd")

	gate(t, "f32 default ef", measureRecall(t, ref, queries, gateK, func(q []float32) ([]VectorMatch, error) {
		return se.SearchVector("gf", q, gateK)
	}), gateRecallF32)
	gate(t, "f32 ef=256", measureRecall(t, ref, queries, gateK, func(q []float32) ([]VectorMatch, error) {
		return se.SearchVectorWithOptions("gf", q, gateK, VectorSearchOptions{Ef: 256})
	}), gateRecallF32Ef256)
	gate(t, "int8 + re-rank", measureRecall(t, ref, queries, gateK, func(q []float32) ([]VectorMatch, error) {
		return se.SearchVector("gq", q, gateK)
	}), gateRecallInt8)
	gate(t, "pq m=16 + re-rank", measureRecall(t, ref, queries, gateK, func(q []float32) ([]VectorMatch, error) {
		return se.SearchVector("gp", q, gateK)
	}), gateRecallPQ)
	gate(t, "pq + disk graph", measureRecall(t, ref, queries, gateK, func(q []float32) ([]VectorMatch, error) {
		return se.SearchVector("gd", q, gateK)
	}), gateRecallDiskPQ)

	// Filtered: an indexed "=" filter matching 25 % (1250 ids) — above
	// vectorBruteForceMax/2 but forced onto the graph path.
	old := vectorBruteForceMax
	vectorBruteForceMax = 100
	defer func() { vectorBruteForceMax = old }()
	if err := se.CreateFieldIndex("gate_shard", "shard"); err != nil {
		t.Fatal(err)
	}
	defer se.DropFieldIndex("gate_shard")
	s1 := map[string][]float32{}
	for i, id := range ids {
		if i%4 == 1 {
			s1[id] = vecs[i]
		}
	}
	filter := &VectorFilter{Field: "shard", Op: "=", Value: "s1"}
	gate(t, "filtered (graph path)", measureRecall(t, s1, queries, gateK, func(q []float32) ([]VectorMatch, error) {
		got, err := se.SearchVectorWithOptions("gf", q, gateK, VectorSearchOptions{Filter: filter})
		for _, m := range got {
			if _, ok := s1[m.ID]; !ok {
				t.Fatalf("filtered result %s does not match the filter", m.ID)
			}
		}
		return got, err
	}), gateRecallFiltered)

	// Deletes: remove 40 %, let compaction rebuild, recall must hold.
	live := map[string][]float32{}
	for i, id := range ids {
		if i%5 < 2 {
			if err := se.DeleteVector("gf", id); err != nil {
				t.Fatal(err)
			}
		} else {
			live[id] = vecs[i]
		}
	}
	gate(t, "after 40 % deletes", measureRecall(t, live, queries, gateK, func(q []float32) ([]VectorMatch, error) {
		return se.SearchVector("gf", q, gateK)
	}), gateRecallDeleted)
	if st := vecStats(t, se, "gf"); st.Count != len(live) {
		t.Fatalf("live count %d, want %d", st.Count, len(live))
	}
}

// TestSearchQualityGate_TextAndHybrid pins BM25 and RRF behaviour on a small
// labelled corpus: each query has one intended answer that must rank first.
func TestSearchQualityGate_TextAndHybrid(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	corpus := map[string]string{
		"raft":     "raft consensus leader election log replication quorum",
		"hnsw":     "hnsw graph approximate nearest neighbour vector search",
		"bm25":     "bm25 ranking inverted index term frequency document length",
		"wal":      "write ahead log group commit fdatasync durability crash recovery",
		"lirs":     "lirs cache eviction recency reuse distance",
		"vlog":     "value log key value separation garbage collection nvme",
		"hindi":    "वेक्टर खोज और पूर्ण पाठ खोज एक साथ",
		"rrf":      "reciprocal rank fusion combines ranked lists hybrid search",
		"quant":    "int8 scalar quantization reduces vector ram memory",
		"transfer": "partition transfer rebalance migrates keys to new owners",
	}
	for id, text := range corpus {
		if err := se.PutText("kb", id, text); err != nil {
			t.Fatal(err)
		}
	}
	for q, want := range map[string]string{
		"leader election":             "raft",
		"nearest neighbour graph":     "hnsw",
		"crash recovery durability":   "wal",
		"garbage collection":          "vlog",
		"पाठ खोज":                     "hindi",
		"hybrid search ranked lists":  "rrf",
		"vector memory quantization":  "quant",
		"rebalance migrates keys":     "transfer",
		"term frequency ranking bm25": "bm25",
	} {
		got, err := se.SearchText("kb", q, 3, nil)
		if err != nil || len(got) == 0 || got[0].ID != want {
			t.Errorf("TSEARCH %q: %+v err=%v, want %s first", q, got, err, want)
		}
	}

	// Hybrid: the vector list is hnsw, quant, …; only "quant" matches the
	// text "memory". quant (2nd + 1st) must beat hnsw (1st on vectors only).
	_ = se.RegisterVectorNamespace("kb", 3)
	for id, v := range map[string][]float32{
		"hnsw": {1, 0, 0}, "quant": {0.7, 0.7, 0}, "raft": {0, 0, 1}, "bm25": {0, 1, 0},
	} {
		if err := se.PutVector("kb", id, v); err != nil {
			t.Fatal(err)
		}
	}
	got, err := se.SearchHybrid("kb", []float32{1, 0, 0}, "memory", 3, HybridOptions{})
	if err != nil || len(got) == 0 || got[0].ID != "quant" {
		t.Fatalf("hybrid: %+v err=%v, want quant (2nd on vectors, 1st on text) first", got, err)
	}
	one := 1.0
	if got, _ := se.SearchHybrid("kb", []float32{1, 0, 0}, "memory", 1, HybridOptions{Alpha: &one}); got[0].ID != "hnsw" {
		t.Fatalf("alpha=1 hybrid: %+v, want hnsw", got)
	}
}
