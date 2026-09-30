package storage

// real_embeddings_test.go — recall on a real embedding dataset.
//
// The quality gate (search_quality_test.go) uses synthetic clusters; this
// test uses real vectors, whose neighbourhood structure synthetic data does
// not reproduce. It loads an ann-benchmarks dataset converted to fvecs by
// scripts/ann-dataset.py, computes exact cosine neighbours, and measures
// recall@10 and query latency for float32, int8, pq and pq + disk graph
// namespaces at several beam widths. The nightly workflow runs it on
// GloVe-100 (100K words).
//
//	scripts/ann-dataset.py glove-100-angular /tmp/ann --train 100000 --test 500
//	VELTRIX_ANN_DIR=/tmp/ann go test ./storage -run TestRealEmbeddings -v -timeout 30m
//
// Env: VELTRIX_ANN_DIR (required), VELTRIX_ANN_NAME (glove-100-angular),
// VELTRIX_ANN_TRAIN (all), VELTRIX_ANN_GATES=0 to only print.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/internal/annds"
)

// realGates are recall@10 minimums for GloVe-100 (first 100K base vectors,
// first 500 queries), applied only to that exact setup. Measured 2026-09-30
// (darwin/arm64) at ef=64 / 128 / 256 / 512:
//
//	f32    0.842 0.906 0.953 0.983
//	int8   0.839 0.905 0.952 0.983
//	pq     0.797 0.870 0.927 0.969   (m = 25)
//	pqdisk 0.788 0.875 0.931 0.966
//
// Gates sit 2–3 points under at ef=256 and ef=512.
var realGates = map[string]float64{
	"f32@256": 0.93, "int8@256": 0.93, "pq@256": 0.90, "pqdisk@256": 0.90,
	"f32@512": 0.96, "int8@512": 0.96, "pq@512": 0.94, "pqdisk@512": 0.94,
}

const realGatesDataset, realGatesTrain = "glove-100-angular", 100000

func TestRealEmbeddings(t *testing.T) {
	dir := os.Getenv("VELTRIX_ANN_DIR")
	if dir == "" {
		t.Skip("set VELTRIX_ANN_DIR (see scripts/ann-dataset.py)")
	}
	name := os.Getenv("VELTRIX_ANN_NAME")
	if name == "" {
		name = "glove-100-angular"
	}
	maxTrain, _ := strconv.Atoi(os.Getenv("VELTRIX_ANN_TRAIN"))
	base, err := annds.ReadFvecs(filepath.Join(dir, name+".train.fvecs"), maxTrain)
	if err != nil {
		t.Fatal(err)
	}
	queries, err := annds.ReadFvecs(filepath.Join(dir, name+".test.fvecs"), 0)
	if err != nil {
		t.Fatal(err)
	}
	dim := len(base[0])
	const k = 10
	t0 := time.Now()
	truth := annds.GroundTruth(base, queries, k)
	t.Logf("%s: %d base × %d dim, %d queries; ground truth in %s", name, len(base), dim, len(queries), time.Since(t0).Round(time.Millisecond))

	se := openSearchEngine(t, t.TempDir())
	defer se.Close()
	layouts := []struct {
		ns   string
		opts VectorNamespaceOptions
	}{
		{"f32", VectorNamespaceOptions{}},
		{"int8", VectorNamespaceOptions{Quantization: QuantInt8}},
		{"pq", VectorNamespaceOptions{Quantization: QuantPQ, PQSubspaces: dim / 4}},
		{"pqdisk", VectorNamespaceOptions{Quantization: QuantPQ, PQSubspaces: dim / 4, Graph: GraphDisk}},
	}
	ids := make([]string, len(base))
	index := make(map[string]int, len(base))
	for i := range base {
		ids[i] = fmt.Sprintf("w%07d", i)
		index[ids[i]] = i
	}
	gates := os.Getenv("VELTRIX_ANN_GATES") != "0" && name == realGatesDataset && len(base) == realGatesTrain && len(queries) == 500

	t.Logf("%-7s %8s %9s %7s %8s %9s %8s %9s", "layout", "build", "bytes/vec", "ef", "R@10", "p50", "p99", "QPS")
	for _, l := range layouts {
		if err := se.CreateVectorNamespace(l.ns, dim, l.opts); err != nil {
			t.Fatal(err)
		}
		tb := time.Now()
		var wg sync.WaitGroup
		work := make(chan int, 1024)
		for w := 0; w < runtime.GOMAXPROCS(0); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range work {
					if err := se.PutVector(l.ns, ids[i], base[i]); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		for i := range base {
			work <- i
		}
		close(work)
		wg.Wait()
		if l.opts.Quantization == QuantPQ {
			waitPQTrained(t, se, l.ns)
		}
		build := time.Since(tb)
		st := vecStats(t, se, l.ns)

		for _, ef := range []int{64, 128, 256, 512} {
			got := make([][]int, len(queries))
			lat := make([]float64, len(queries))
			tq := time.Now()
			for qi, q := range queries {
				t1 := time.Now()
				hits, err := se.SearchVectorWithOptions(l.ns, q, k, VectorSearchOptions{Ef: ef})
				lat[qi] = float64(time.Since(t1).Microseconds())
				if err != nil {
					t.Fatal(err)
				}
				for _, h := range hits {
					got[qi] = append(got[qi], index[h.ID])
				}
			}
			qps := float64(len(queries)) / time.Since(tq).Seconds()
			rec := annds.Recall(got, truth, k)
			t.Logf("%-7s %8s %9d %7d %8.3f %8.0fµs %7.0fµs %9.0f", l.ns, build.Round(time.Second),
				(st.Bytes+st.DiskGraph)/int64(st.Count), ef, rec, annds.Percentile(lat, 50), annds.Percentile(lat, 99), qps)
			if min, ok := realGates[fmt.Sprintf("%s@%d", l.ns, ef)]; ok && gates && rec < min {
				t.Errorf("%s ef=%d: recall@10 %.3f below the gate %.2f", l.ns, ef, rec, min)
			}
		}
	}
}
