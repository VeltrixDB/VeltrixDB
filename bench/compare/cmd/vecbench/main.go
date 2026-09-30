// Command vecbench measures vector search the way ANN benchmarks do: load a
// real dataset (fvecs from scripts/ann-dataset.py), compute exact cosine
// neighbours, then sweep the beam width and report recall@k, QPS, p50 / p99
// latency, load time and throughput. Queries run concurrently over the
// network, against a server on the machine under test.
//
//	vecbench -db veltrixdb -addr 127.0.0.1:9000 -data /tmp/ann -name glove-100-angular \
//	         -quant pq -graph disk -efs 64,128,256,512 -threads 16 -out results/
//
// Only a VeltrixDB driver is included. Aerospike Vector Search and ScyllaDB
// vector search expose different client APIs that could not be exercised
// where this was written, and VectorDBBench has no client for either; add a
// driver (the vectorDB interface below) when benchmarking them, and run it
// on the same machine with the same dataset and flags.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
	"github.com/VeltrixDB/veltrixdb/internal/annds"
)

// vectorDB is one database under test. Implementations must be safe for
// concurrent use.
type vectorDB interface {
	Setup(dim int) error
	Insert(id int, v []float32) error
	Search(v []float32, k, ef int) ([]int, error)
	Close()
}

// veltrix drives VeltrixDB over the binary protocol with a connection pool.
type veltrix struct {
	addr, ns string
	opts     client.VectorNamespaceOptions
	pool     chan *client.BinaryConn
}

func newVeltrix(addr, ns string, opts client.VectorNamespaceOptions, conns int) (*veltrix, error) {
	v := &veltrix{addr: addr, ns: ns, opts: opts, pool: make(chan *client.BinaryConn, conns)}
	for i := 0; i < conns; i++ {
		c, err := client.DialBinary(addr, 5*time.Second)
		if err != nil {
			return nil, err
		}
		v.pool <- c
	}
	return v, nil
}

func (v *veltrix) with(fn func(c *client.BinaryConn) error) error {
	c := <-v.pool
	defer func() { v.pool <- c }()
	return fn(c)
}

func (v *veltrix) Setup(dim int) error {
	return v.with(func(c *client.BinaryConn) error { return c.VCreateWithOptions(v.ns, dim, v.opts) })
}

func (v *veltrix) Insert(id int, vec []float32) error {
	return v.with(func(c *client.BinaryConn) error { return c.VSetNS(v.ns, strconv.Itoa(id), vec) })
}

func (v *veltrix) Search(vec []float32, k, ef int) ([]int, error) {
	var out []int
	err := v.with(func(c *client.BinaryConn) error {
		hits, err := c.VSearchWithOptions(k, vec, client.VectorSearchOptions{NS: v.ns, Ef: ef})
		for _, h := range hits {
			n, _ := strconv.Atoi(h.ID)
			out = append(out, n)
		}
		return err
	})
	return out, err
}

func (v *veltrix) Close() {
	close(v.pool)
	for c := range v.pool {
		c.Close()
	}
}

type row struct {
	EF      int     `json:"ef"`
	Recall  float64 `json:"recall"`
	QPS     float64 `json:"qps"`
	P50us   float64 `json:"p50_us"`
	P99us   float64 `json:"p99_us"`
	Queries int     `json:"queries"`
}

type report struct {
	DB         string    `json:"db"`
	Dataset    string    `json:"dataset"`
	Base       int       `json:"base"`
	Dim        int       `json:"dim"`
	K          int       `json:"k"`
	Threads    int       `json:"threads"`
	Settings   string    `json:"settings"`
	LoadSecs   float64   `json:"load_seconds"`
	LoadPerSec float64   `json:"load_vectors_per_second"`
	Rows       []row     `json:"rows"`
	Host       string    `json:"host"`
	When       time.Time `json:"when"`
}

func main() {
	dbName := flag.String("db", "veltrixdb", "database driver (veltrixdb)")
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	dataDir := flag.String("data", "", "directory with <name>.train.fvecs / <name>.test.fvecs")
	name := flag.String("name", "glove-100-angular", "dataset name")
	maxBase := flag.Int("train", 0, "base vectors to load (0 = all in the file)")
	maxQ := flag.Int("queries", 0, "queries to run (0 = all)")
	k := flag.Int("k", 10, "neighbours per query")
	efsFlag := flag.String("efs", "64,128,256,512", "beam widths to sweep")
	threads := flag.Int("threads", runtime.GOMAXPROCS(0), "concurrent clients for load and queries")
	ns := flag.String("ns", "vecbench", "VeltrixDB vector namespace")
	quant := flag.String("quant", "", "VeltrixDB quantization: none|int8|pq")
	pqm := flag.Int("pqm", 0, "VeltrixDB pq subspaces (0 = dim/8)")
	graph := flag.String("graph", "", "VeltrixDB graph placement: memory|disk")
	skipLoad := flag.Bool("skip-load", false, "reuse vectors already loaded in -ns")
	out := flag.String("out", "", "directory for report .json and .md (default: stdout only)")
	flag.Parse()
	if *dataDir == "" {
		log.Fatal("-data is required (see scripts/ann-dataset.py)")
	}

	base, err := annds.ReadFvecs(filepath.Join(*dataDir, *name+".train.fvecs"), *maxBase)
	if err != nil {
		log.Fatal(err)
	}
	queries, err := annds.ReadFvecs(filepath.Join(*dataDir, *name+".test.fvecs"), *maxQ)
	if err != nil {
		log.Fatal(err)
	}
	dim := len(base[0])
	log.Printf("%s: %d base × %d dim, %d queries; computing exact neighbours", *name, len(base), dim, len(queries))
	truth := annds.GroundTruth(base, queries, *k)

	var db vectorDB
	switch *dbName {
	case "veltrixdb":
		opts := client.VectorNamespaceOptions{Quantization: *quant, PQSubspaces: *pqm, Graph: *graph}
		if *quant == "pq" && len(base) < 10000 {
			opts.PQTrainAt = len(base)
		}
		db, err = newVeltrix(*addr, *ns, opts, *threads)
	default:
		log.Fatalf("unknown -db %q (only veltrixdb is included; see the package comment)", *dbName)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rep := report{DB: *dbName, Dataset: *name, Base: len(base), Dim: dim, K: *k, Threads: *threads,
		Settings: fmt.Sprintf("quant=%s pqm=%d graph=%s", *quant, *pqm, *graph), When: time.Now().UTC()}
	rep.Host, _ = os.Hostname()

	if !*skipLoad {
		if err := db.Setup(dim); err != nil {
			log.Fatalf("setup: %v", err)
		}
		t0 := time.Now()
		var next atomic.Int64
		var wg sync.WaitGroup
		var loadErr atomic.Value
		for w := 0; w < *threads; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= len(base) {
						return
					}
					if err := db.Insert(i, base[i]); err != nil {
						loadErr.Store(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		if e := loadErr.Load(); e != nil {
			log.Fatalf("load: %v", e)
		}
		rep.LoadSecs = time.Since(t0).Seconds()
		rep.LoadPerSec = float64(len(base)) / rep.LoadSecs
		log.Printf("loaded %d vectors in %.1fs (%.0f/s)", len(base), rep.LoadSecs, rep.LoadPerSec)
		if *quant == "pq" {
			log.Printf("pq: waiting 10s for background codebook training before querying")
			time.Sleep(10 * time.Second)
		}
	}

	for _, s := range strings.Split(*efsFlag, ",") {
		ef, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			log.Fatalf("bad -efs entry %q", s)
		}
		got := make([][]int, len(queries))
		lat := make([]float64, len(queries))
		var next atomic.Int64
		var wg sync.WaitGroup
		t0 := time.Now()
		for w := 0; w < *threads; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= len(queries) {
						return
					}
					t1 := time.Now()
					ids, err := db.Search(queries[i], *k, ef)
					lat[i] = float64(time.Since(t1).Microseconds())
					if err != nil {
						log.Fatalf("search: %v", err)
					}
					got[i] = ids
				}
			}()
		}
		wg.Wait()
		r := row{EF: ef, Recall: annds.Recall(got, truth, *k), QPS: float64(len(queries)) / time.Since(t0).Seconds(), Queries: len(queries)}
		r.P50us, r.P99us = annds.Percentile(lat, 50), annds.Percentile(lat, 99)
		rep.Rows = append(rep.Rows, r)
		log.Printf("ef=%d recall@%d=%.3f qps=%.0f p50=%.0fµs p99=%.0fµs", ef, *k, r.Recall, r.QPS, r.P50us, r.P99us)
	}

	md := markdown(rep)
	fmt.Print(md)
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			log.Fatal(err)
		}
		stem := filepath.Join(*out, fmt.Sprintf("vec-%s-%s-%s", rep.DB, rep.Dataset, strings.NewReplacer(" ", "_", "=", "-").Replace(rep.Settings)))
		b, _ := json.MarshalIndent(rep, "", "  ")
		_ = os.WriteFile(stem+".json", b, 0o644)
		_ = os.WriteFile(stem+".md", []byte(md), 0o644)
		log.Printf("wrote %s.{json,md}", stem)
	}
}

func markdown(r report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### %s — %s (%d × %d), k=%d, %d threads, %s\n\n", r.DB, r.Dataset, r.Base, r.Dim, r.K, r.Threads, r.Settings)
	if r.LoadSecs > 0 {
		fmt.Fprintf(&b, "Load: %.1f s (%.0f vectors/s)\n\n", r.LoadSecs, r.LoadPerSec)
	}
	fmt.Fprintf(&b, "| ef | recall@%d | QPS | p50 | p99 |\n|--|--|--|--|--|\n", r.K)
	for _, x := range r.Rows {
		fmt.Fprintf(&b, "| %d | %.3f | %.0f | %.0f µs | %.0f µs |\n", x.EF, x.Recall, x.QPS, x.P50us, x.P99us)
	}
	fmt.Fprintf(&b, "\nHost: %s, %s\n\n", r.Host, r.When.Format(time.RFC3339))
	return b.String()
}
