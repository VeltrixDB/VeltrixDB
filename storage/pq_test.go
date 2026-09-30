package storage

// pq_test.go — product quantization and the disk-resident graph.

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func clusteredSet(seed int64, n, dim, clusters int, noise float32) [][]float32 {
	r := rand.New(rand.NewSource(seed))
	centres := make([][]float32, clusters)
	for i := range centres {
		centres[i] = randVec(r, dim)
	}
	out := make([][]float32, n)
	for i := range out {
		c := centres[r.Intn(clusters)]
		v := make([]float32, dim)
		for j := range v {
			v[j] = c[j] + noise*float32(r.NormFloat64())
		}
		out[i], _ = normalizeVector(v)
	}
	return out
}

func TestPQ_TrainEncodeADC(t *testing.T) {
	const dim, m = 32, 8
	data := clusteredSet(1, 3000, dim, 20, 0.2)
	cb, err := trainPQ(append([][]float32(nil), data...), dim, m, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cb.ksub != pqKsub || len(cb.bounds) != m+1 || cb.bounds[m] != dim {
		t.Fatalf("codebook shape: ksub=%d bounds=%v", cb.ksub, cb.bounds)
	}
	// Reconstruction error well below the data's own spread.
	var errSum, normSum float64
	q := data[7]
	table := cb.table(q)
	for _, v := range data[:500] {
		code := cb.encode(v)
		rec := cb.decode(code)
		for j := range v {
			d := float64(v[j] - rec[j])
			errSum += d * d
			normSum += float64(v[j]) * float64(v[j])
		}
		// ADC is exactly the dot product with the reconstruction.
		if a, b := cb.adcScore(table, code), dot(q, rec); math.Abs(float64(a-b)) > 1e-4 {
			t.Fatalf("adc %f != dot(q, decode) %f", a, b)
		}
	}
	if rel := errSum / normSum; rel > 0.15 {
		t.Fatalf("relative reconstruction error %.3f too high", rel)
	} else {
		t.Logf("relative reconstruction error %.3f", rel)
	}
	// Uneven subspaces (dim not divisible by m).
	if b := pqBounds(10, 3); fmt.Sprint(b) != "[0 4 7 10]" {
		t.Fatalf("pqBounds(10,3) = %v", b)
	}
	if _, err := trainPQ(data[:100], dim, m, 1); err == nil {
		t.Fatal("training on fewer than pqMinTrainN vectors must fail")
	}
}

func waitPQTrained(t *testing.T, se *StorageEngine, ns string) VectorStats {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		st := vecStats(t, se, ns)
		if st.PQTrained {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("pq codebook for %q not trained: %+v", ns, st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPQ_Namespace: a PQ namespace stores float32 until PQTrainAt vectors,
// trains in the background, then answers with exact (re-ranked) scores at
// recall close to float32 with a fraction of the RAM; the setting and the
// trained state come back after a restart.
func TestPQ_Namespace(t *testing.T) {
	dir := t.TempDir()
	se := openSearchEngine(t, dir)
	const n, dim = 3000, 64
	data := clusteredSet(2, n, dim, 30, 0.25)
	if err := se.CreateVectorNamespace("p", dim, VectorNamespaceOptions{Quantization: QuantPQ, PQSubspaces: 16, PQTrainAt: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := se.CreateVectorNamespace("f", dim, VectorNamespaceOptions{}); err != nil {
		t.Fatal(err)
	}
	ref := map[string][]float32{}
	for i, v := range data {
		id := fmt.Sprintf("p%05d", i)
		ref[id] = v
		if err := se.PutVector("p", id, v); err != nil {
			t.Fatal(err)
		}
		if err := se.PutVector("f", id, v); err != nil {
			t.Fatal(err)
		}
	}
	st := waitPQTrained(t, se, "p")
	fs := vecStats(t, se, "f")
	t.Logf("pq bytes=%d f32 bytes=%d", st.Bytes, fs.Bytes)
	if st.Bytes >= fs.Bytes/2 {
		t.Fatalf("pq index %d B is not well below float32's %d B", st.Bytes, fs.Bytes)
	}
	queries := clusteredSet(3, 50, dim, 30, 0.25)
	rec := measureRecall(t, ref, queries, 10, func(q []float32) ([]VectorMatch, error) {
		got, err := se.SearchVector("p", q, 10)
		for _, m := range got {
			if exact := dot(q, ref[m.ID]); math.Abs(float64(exact-m.Score)) > 1e-4 {
				t.Fatalf("score %f is not the exact cosine %f", m.Score, exact)
			}
		}
		return got, err
	})
	t.Logf("pq recall@10 = %.3f", rec)
	if rec < 0.85 {
		t.Fatalf("pq recall@10 %.3f < 0.85", rec)
	}
	se.Close()

	se = openSearchEngine(t, dir)
	defer se.Close()
	if _, _, err := se.RebuildSearchIndexes(); err != nil {
		t.Fatal(err)
	}
	st = waitPQTrained(t, se, "p")
	if st.Quantization != QuantPQ || st.Count != n {
		t.Fatalf("after restart: %+v", st)
	}
}

// TestDiskGraph: layer-0 edges live in the mapped file; results match an
// in-memory graph and survive compaction and reconfiguration.
func TestDiskGraph(t *testing.T) {
	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	const n, dim = 3000, 32
	data := clusteredSet(4, n, dim, 25, 0.25)
	if err := se.CreateVectorNamespace("dg", dim, VectorNamespaceOptions{Graph: GraphDisk}); err != nil {
		t.Fatal(err)
	}
	if err := se.CreateVectorNamespace("mg", dim, VectorNamespaceOptions{}); err != nil {
		t.Fatal(err)
	}
	ref := map[string][]float32{}
	for i, v := range data {
		id := fmt.Sprintf("d%05d", i)
		ref[id] = v
		_ = se.PutVector("dg", id, v)
		_ = se.PutVector("mg", id, v)
	}
	dst, mst := vecStats(t, se, "dg"), vecStats(t, se, "mg")
	if dst.Graph != GraphDisk || dst.DiskGraph < int64(n)*adjRecordBytes {
		t.Fatalf("disk graph stats: %+v", dst)
	}
	if dst.Bytes >= mst.Bytes {
		t.Fatalf("disk graph heap bytes %d not below in-memory %d", dst.Bytes, mst.Bytes)
	}
	queries := clusteredSet(5, 50, dim, 25, 0.25)
	search := func(ns string) func(q []float32) ([]VectorMatch, error) {
		return func(q []float32) ([]VectorMatch, error) { return se.SearchVector(ns, q, 10) }
	}
	rd, rm := measureRecall(t, ref, queries, 10, search("dg")), measureRecall(t, ref, queries, 10, search("mg"))
	t.Logf("recall@10 disk=%.3f memory=%.3f; heap %d vs %d B, mapped %d B", rd, rm, dst.Bytes, mst.Bytes, dst.DiskGraph)
	if rd < rm-0.02 {
		t.Fatalf("disk graph recall %.3f below memory graph %.3f", rd, rm)
	}

	// Compaction swaps in a fresh mapped file.
	live := map[string][]float32{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("d%05d", i)
		if i%5 < 2 {
			_ = se.DeleteVector("dg", id)
		} else {
			live[id] = ref[id]
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for vecStats(t, se, "dg").Compactions == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no compaction")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rec := measureRecall(t, live, queries, 10, search("dg")); rec < 0.9 {
		t.Fatalf("recall after compaction on the disk graph: %.3f", rec)
	}

	// Reconfigure: disk graph + PQ, then back to memory.
	if err := se.CreateVectorNamespace("dg", dim, VectorNamespaceOptions{Graph: GraphDisk, Quantization: QuantPQ, PQSubspaces: 8, PQTrainAt: 500}); err != nil {
		t.Fatal(err)
	}
	if st := vecStats(t, se, "dg"); !st.PQTrained || st.Graph != GraphDisk || st.Count != len(live) {
		t.Fatalf("after switch to disk+pq: %+v", st)
	}
	if rec := measureRecall(t, live, queries, 10, search("dg")); rec < 0.85 {
		t.Fatalf("disk+pq recall %.3f", rec)
	}
	if err := se.CreateVectorNamespace("dg", dim, VectorNamespaceOptions{}); err != nil {
		t.Fatal(err)
	}
	if st := vecStats(t, se, "dg"); st.Graph != GraphMemory || st.DiskGraph != 0 {
		t.Fatalf("after switch back to memory: %+v", st)
	}
}

func TestVectorNSConfig_Validation(t *testing.T) {
	for _, c := range []vectorNSConfig{
		{Dim: 8, Quant: "bogus"},
		{Dim: 8, Graph: "ssd"},
		{Dim: 8, Quant: QuantPQ, PQM: 9},
		{Dim: 8, Quant: QuantPQ, PQTrainAt: 10},
	} {
		if err := c.normalize(); err == nil {
			t.Errorf("%+v must be rejected", c)
		}
	}
	c := vectorNSConfig{Dim: 768, Quant: "PQ"}
	if err := c.normalize(); err != nil || c.PQM != 96 || c.Graph != GraphMemory {
		t.Fatalf("defaults: %+v err=%v", c, err)
	}
}
