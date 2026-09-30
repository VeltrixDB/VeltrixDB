package main

// search_test.go — wire-level text / hybrid / namespace ops, and
// distributed search across a rebalanced two-node cluster.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/storage"
)

func TestTextHybridWire(t *testing.T) {
	dir := t.TempDir()
	ts := startTestServer(t, dir, nil)

	bc := dialBinary(t, ts.addr)
	tc := dialText(t, ts.addr)

	if err := bc.VCreate("kb", 2, "int8"); err != nil {
		t.Fatalf("binary vcreate: %v", err)
	}
	if err := tc.VCreate("kb2", 3, ""); err != nil {
		t.Fatalf("text vcreate: %v", err)
	}
	if err := tc.VCreate("kb", 2, "bogus"); err == nil {
		t.Fatal("unknown quantization must fail")
	}

	docs := []struct {
		id   string
		vec  []float32
		text string
	}{
		{"sem", []float32{1, 0}, "nothing relevant here"},
		{"both", []float32{0.9, 0.44}, "veltrix storage engine notes"},
		{"kw", []float32{-1, 0}, "veltrix veltrix veltrix"},
		// Pushes kw to 4th on the vector list, so "both" (2nd on each list)
		// is the RRF winner.
		{"far", []float32{0, -1}, "other words"},
	}
	for i, d := range docs {
		if err := bc.VSetNS("kb", d.id, d.vec); err != nil {
			t.Fatal(err)
		}
		// Alternate protocols for the text half.
		if i%2 == 0 {
			err := bc.TSet("kb", d.id, d.text)
			if err != nil {
				t.Fatalf("binary tset: %v", err)
			}
		} else if err := tc.TSet("kb", d.id, d.text); err != nil {
			t.Fatalf("text tset: %v", err)
		}
	}

	for name, search := range map[string]func() ([]client.VectorResult, error){
		"binary": func() ([]client.VectorResult, error) {
			return bc.TSearch(5, "Veltrix", client.TextSearchOptions{NS: "kb"})
		},
		"text": func() ([]client.VectorResult, error) {
			return tc.TSearch(5, "Veltrix", client.TextSearchOptions{NS: "kb"})
		},
	} {
		got, err := search()
		if err != nil || len(got) != 2 || got[0].ID != "kw" {
			t.Fatalf("%s tsearch: %+v err=%v", name, got, err)
		}
	}
	for name, search := range map[string]func() ([]client.VectorResult, error){
		"binary": func() ([]client.VectorResult, error) {
			return bc.HSearch(2, []float32{1, 0}, "veltrix", client.HybridSearchOptions{NS: "kb"})
		},
		"text": func() ([]client.VectorResult, error) {
			return tc.HSearch(2, []float32{1, 0}, "veltrix", client.HybridSearchOptions{NS: "kb", Ef: 32, Candidates: 10})
		},
	} {
		got, err := search()
		if err != nil || len(got) != 2 || got[0].ID != "both" {
			t.Fatalf("%s hsearch: %+v err=%v", name, got, err)
		}
	}
	one := 1.0
	if got, err := bc.HSearch(1, []float32{1, 0}, "veltrix", client.HybridSearchOptions{NS: "kb", Alpha: &one}); err != nil || got[0].ID != "sem" {
		t.Fatalf("alpha=1 hsearch: %+v err=%v", got, err)
	}

	// Filter on the KV record with the document id.
	if err := bc.Put("kw", []byte(`{"tier":"gold"}`), 0); err != nil {
		t.Fatal(err)
	}
	got, err := bc.TSearch(5, "veltrix", client.TextSearchOptions{NS: "kb", FilterField: "tier", FilterOp: "=", FilterValue: "gold"})
	if err != nil || len(got) != 1 || got[0].ID != "kw" {
		t.Fatalf("filtered tsearch: %+v err=%v", got, err)
	}

	if err := tc.TDel("kb", "kw"); err != nil {
		t.Fatal(err)
	}
	if err := bc.TDel("kb", "both"); err != nil {
		t.Fatal(err)
	}
	if got, err := bc.TSearch(5, "veltrix", client.TextSearchOptions{NS: "kb"}); err != nil || len(got) != 0 {
		t.Fatalf("after tdel: %+v err=%v", got, err)
	}

	// Errors are protocol errors, not dropped connections.
	if _, err := bc.TSearch(5, "   ", client.TextSearchOptions{NS: "kb"}); err == nil {
		t.Fatal("empty query must fail")
	}
	if _, err := tc.HSearch(1, nil, "", client.HybridSearchOptions{NS: "nope"}); err == nil {
		t.Fatal("hsearch with neither half must fail")
	}
	if _, err := bc.TSearch(5, "notes", client.TextSearchOptions{NS: "kb"}); err != nil {
		t.Fatalf("binary connection broken after error: %v", err)
	}
	if err := tc.TSet("kb", "nl", "two\nlines"); err == nil {
		t.Fatal("text protocol must refuse newlines")
	}
	bc.Close()
	tc.Close()

	// Restart: text documents and the int8 setting are rebuilt.
	ts.stop(t)
	ts = startTestServer(t, dir, nil)
	defer ts.stop(t)
	bc = dialBinary(t, ts.addr)
	defer bc.Close()
	if got, err := bc.TSearch(5, "nothing", client.TextSearchOptions{NS: "kb"}); err != nil || len(got) != 1 || got[0].ID != "sem" {
		t.Fatalf("after restart: %+v err=%v", got, err)
	}
	for _, st := range ts.engine.VectorIndexStats() {
		if st.Namespace == "kb" && st.Quantization != storage.QuantInt8 {
			t.Fatalf("kb lost its int8 setting across restart: %+v", st)
		}
	}
}

// TestDistributedSearch_AfterRebalance: once a join has moved part of the
// keys to node-2, node-1 alone no longer sees every vector / document, but a
// fanned-out search does — including the rebalanced namespace setting and
// filters on co-located records.
func TestDistributedSearch_AfterRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-engine integration test")
	}
	cfg := cluster.DefaultClusterConfig()
	pm1 := cluster.NewPartitionMap(cfg)
	if err := pm1.AddNode("node-1", "127.0.0.1", 7201); err != nil {
		t.Fatal(err)
	}
	eng1 := newFSMTestEngine(t)
	// Both nodes sign transfer + search traffic with a shared secret.
	secret := []byte("distributed-search-test-secret")
	ta1 := cluster.NewTransferAgent(pm1, "node-1", eng1, "127.0.0.1:0")
	ta1.SetClusterSecret(secret)
	ta1.Handle(searchPath, newSearchHandler(eng1))
	if err := ta1.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ta1.Stop)
	pm1.SetNodeTransferAddr("node-1", ta1.BoundAddr())
	t.Cleanup(startAutoRebalancer(pm1, ta1, "node-1"))

	const n, dim = 200, 8
	// A replicated index definition (what IDXCREATE writes in replicated
	// mode) and some namespaced records for QUERY.
	if err := eng1.Put(storage.IndexDefKey("by_even"), []byte("even"), -1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		grp := "a"
		if i%4 == 0 {
			grp = "b"
		}
		if err := eng1.PutNS("users", fmt.Sprintf("u%02d", i), []byte(fmt.Sprintf(`{"grp":%q}`, grp)), -1); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng1.CreateVectorNamespace("dv", dim, storage.VectorNamespaceOptions{Quantization: storage.QuantInt8}); err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(77))
	vecs := map[string][]float32{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("doc%03d", i)
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[id] = v
		if err := eng1.PutVector("dv", id, v); err != nil {
			t.Fatal(err)
		}
		if err := eng1.PutText("dv", id, fmt.Sprintf("shared term and unique%03d", i)); err != nil {
			t.Fatal(err)
		}
		if err := eng1.Put(id, []byte(fmt.Sprintf(`{"even":%q}`, fmt.Sprint(i%2 == 0))), -1); err != nil {
			t.Fatal(err)
		}
	}

	pm2 := cluster.NewPartitionMap(cfg)
	if err := pm2.AddNode("node-1", "127.0.0.1", 7201); err != nil {
		t.Fatal(err)
	}
	eng2 := newFSMTestEngine(t)
	ta2 := cluster.NewTransferAgent(pm2, "node-2", eng2, "127.0.0.1:0")
	ta2.SetClusterSecret(secret)
	ta2.Handle(searchPath, newSearchHandler(eng2))
	if err := ta2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ta2.Stop)
	if err := pm2.AddNode("node-2", "127.0.0.1", 7202); err != nil {
		t.Fatal(err)
	}
	if err := pm1.AddNodeWithTransfer("node-2", "127.0.0.1", 7202, ta2.BoundAddr()); err != nil {
		t.Fatal(err)
	}

	localCount := func(se *storage.StorageEngine) int {
		for _, st := range se.VectorIndexStats() {
			if st.Namespace == "dv" {
				return st.Count
			}
		}
		return 0
	}
	deadline := time.Now().Add(30 * time.Second)
	for !(localCount(eng2) > 0 && localCount(eng1)+localCount(eng2) == n) {
		if time.Now().After(deadline) {
			t.Fatalf("rebalance did not settle: node-1 %d, node-2 %d vectors", localCount(eng1), localCount(eng2))
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The namespace setting was copied, not moved, and applied on node-2.
	for _, se := range []*storage.StorageEngine{eng1, eng2} {
		for _, st := range se.VectorIndexStats() {
			if st.Namespace == "dv" && st.Quantization != storage.QuantInt8 {
				t.Fatalf("namespace setting not applied everywhere: %+v", st)
			}
		}
	}
	// Co-location: every vector sits with its record.
	for _, id := range eng2.ScanKeys() {
		if storage.IsVectorKey(id) {
			rec := id[len("@vec/dv/"):]
			if _, err := eng2.Get(rec); err != nil {
				t.Fatalf("vector %s moved without its record: %v", id, err)
			}
		}
	}

	coord := &coordinator{mode: modeReplicated, engine: eng1, localID: "node-1", pm: pm1, ta: ta1, searchFanout: true}

	// Vector: all n reachable from node-1.
	all, err := coord.VSearch("dv", vecs["doc000"], 0, storage.VectorSearchOptions{})
	if err != nil || len(all) != n {
		t.Fatalf("distributed k=0 vector search: %d hits err=%v, want %d", len(all), err, n)
	}
	for _, target := range []string{"doc007", "doc123", "doc199"} {
		got, err := coord.VSearch("dv", vecs[target], 1, storage.VectorSearchOptions{})
		if err != nil || len(got) != 1 || got[0].ID != target {
			t.Fatalf("self-query %s: %+v err=%v", target, got, err)
		}
	}
	// Filter evaluated on each node against its co-located records.
	even, err := coord.VSearch("dv", vecs["doc001"], 0, storage.VectorSearchOptions{
		Filter: &storage.VectorFilter{Field: "even", Op: "=", Value: "true"}})
	if err != nil || len(even) != n/2 {
		t.Fatalf("filtered distributed search: %d hits err=%v, want %d", len(even), err, n/2)
	}

	// Text: every document, scored with cluster-wide statistics.
	txt, err := coord.TSearch("dv", "shared", 0, nil)
	if err != nil || len(txt) != n {
		t.Fatalf("distributed text search: %d hits err=%v, want %d", len(txt), err, n)
	}
	for _, target := range []string{"doc010", "doc150"} {
		got, err := coord.TSearch("dv", "unique"+target[3:], 1, nil)
		if err != nil || len(got) != 1 || got[0].ID != target {
			t.Fatalf("text search for %s: %+v err=%v", target, got, err)
		}
	}
	// Hybrid on the merged lists.
	hy, err := coord.HSearch("dv", vecs["doc042"], "unique042", 3, storage.HybridOptions{})
	if err != nil || len(hy) == 0 || hy[0].ID != "doc042" {
		t.Fatalf("distributed hybrid: %+v err=%v", hy, err)
	}
	// The index definition was copied to node-2, and IDXQUERY / QUERY see
	// both nodes' records.
	defined := false
	for _, d := range eng2.ListFieldIndexes() {
		defined = defined || d.Name == "by_even"
	}
	if !defined {
		t.Fatal("replicated index definition did not reach node-2")
	}
	keys, err := coord.IdxQuery("by_even", "true", 0)
	if err != nil || len(keys) != n/2 {
		t.Fatalf("distributed IDXQUERY: %d keys err=%v, want %d", len(keys), err, n/2)
	}
	if local := eng1.LookupBySecondary("by_even", "true"); len(local) >= n/2 {
		t.Fatalf("node-1 alone holds all %d index entries; nothing moved", len(local))
	}
	rows, err := coord.Query("users", "grp", "=", "b", 0)
	if err != nil || len(rows) != 10 {
		t.Fatalf("distributed QUERY: %d rows err=%v, want 10", len(rows), err)
	}
	if rows, _ := coord.Query("users", "grp", "=", "b", 3); len(rows) != 3 {
		t.Fatalf("QUERY limit: %d rows, want 3", len(rows))
	}

	// A namespace no node has is still an error.
	if _, err := coord.VSearch("nope", vecs["doc000"], 1, storage.VectorSearchOptions{}); err == nil {
		t.Fatal("unknown namespace must fail cluster-wide")
	}

	// node-1 alone sees only its share: that is the gap fan-out closes.
	if local, _ := eng1.SearchVector("dv", vecs["doc000"], 0); len(local) >= n {
		t.Fatalf("node-1 holds all %d vectors; the rebalance moved nothing", len(local))
	}
}

// TestSearchFanout_UnreachablePeer: by default a peer that cannot answer
// fails the search and is named; --search-allow-partial returns the local
// part instead and counts it.
func TestSearchFanout_UnreachablePeer(t *testing.T) {
	pm := cluster.NewPartitionMap(cluster.DefaultClusterConfig())
	_ = pm.AddNode("node-1", "127.0.0.1", 7401)
	_ = pm.AddNode("node-2", "127.0.0.1", 7402)
	pm.SetNodeTransferAddr("node-2", "127.0.0.1:1") // nothing listens there
	eng := newFSMTestEngine(t)
	ta := cluster.NewTransferAgent(pm, "node-1", eng, "127.0.0.1:0")
	_ = eng.RegisterVectorNamespace("pv", 2)
	_ = eng.PutVector("pv", "only-local", []float32{1, 0})
	_ = eng.PutText("pv", "only-local", "local words")

	c := &coordinator{mode: modeReplicated, engine: eng, localID: "node-1", pm: pm, ta: ta,
		searchFanout: true, searchTimeout: 500 * time.Millisecond}
	checks := map[string]func() error{
		"vector": func() error {
			_, err := c.VSearch("pv", []float32{1, 0}, 1, storage.VectorSearchOptions{})
			return err
		},
		"text": func() error { _, err := c.TSearch("pv", "local", 1, nil); return err },
		"hybrid": func() error {
			_, err := c.HSearch("pv", []float32{1, 0}, "local", 1, storage.HybridOptions{})
			return err
		},
		"idxquery": func() error { _, err := c.IdxQuery("x", "y", 0); return err },
		"query":    func() error { _, err := c.Query("users", "f", "=", "v", 0); return err },
	}
	for name, run := range checks {
		if err := run(); err == nil || !strings.Contains(err.Error(), "node-2") {
			t.Fatalf("%s with an unreachable peer: err=%v, want a failure naming node-2", name, err)
		}
	}

	c.searchAllowPartial = true
	before := partialSearches.Load()
	got, err := c.VSearch("pv", []float32{1, 0}, 1, storage.VectorSearchOptions{})
	if err != nil || len(got) != 1 || got[0].ID != "only-local" {
		t.Fatalf("partial vector search: %+v err=%v", got, err)
	}
	if partialSearches.Load() != before+1 {
		t.Fatal("partial search not counted")
	}

	// A peer the failure detector marked failed is not asked at all.
	c.searchAllowPartial = false
	if err := pm.UpdateNodeState("node-2", cluster.NodeStateFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := c.VSearch("pv", []float32{1, 0}, 1, storage.VectorSearchOptions{}); err != nil {
		t.Fatalf("search with the dead peer marked failed: %v", err)
	}
}
