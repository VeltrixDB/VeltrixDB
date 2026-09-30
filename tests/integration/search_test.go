package integration_test

// search_test.go — search regression suite against a REAL 3-node replicated
// cluster (three server processes signing their transfer / search traffic
// with a shared --cluster-secret-file).
//
// It drives the public client API only and checks what a user sees:
//   - an int8 vector namespace, text documents, records and a secondary
//     index written through node 1 are searchable from nodes 2 and 3
//     (vector, filtered vector, BM25, hybrid, IDXQUERY);
//   - the node-to-node search endpoint rejects unsigned requests;
//   - with a node killed, a search fails loudly (naming the peer) instead of
//     returning a silently partial answer, and succeeds again with every
//     record once the failure detector has marked the node failed.

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	veltrixclient "github.com/VeltrixDB/veltrixdb/client"
)

const searchDocs = 60

func TestSearchCluster_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, []byte("ci-search-regression-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	used := map[int]bool{}
	ids := []string{"s1", "s2", "s3"}
	ports := []int{reserveBlock(t, used), reserveBlock(t, used), reserveBlock(t, used)}
	pf := peersFlag(ids, ports)
	nodes := make([]*testServer, 3)
	for i := range ids {
		nodes[i] = startNode(t, ports[i], ports[i]+4,
			"-mode", "replicated", "-node-id", ids[i], "-peers", pf,
			"-consistency", "eventual", "-cluster-secret-file", secretFile,
			"-search-timeout-ms", "1500")
		defer nodes[i].stop()
	}
	time.Sleep(500 * time.Millisecond) // replica clients connect

	dial := func(i int) *veltrixclient.TCPConn {
		c, err := veltrixclient.DialTCP(nodes[i].Addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial node %d: %v", i+1, err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	w := dial(0)

	// ── Write everything through node 1 ────────────────────────────────────
	if err := w.VCreate("docs", 4, "int8"); err != nil {
		t.Fatalf("VCREATE: %v", err)
	}
	if err := w.IdxCreate("by_lang", "lang"); err != nil {
		t.Fatalf("IDXCREATE: %v", err)
	}
	r := rand.New(rand.NewSource(7))
	vecs := map[string][]float32{}
	for i := 0; i < searchDocs; i++ {
		id := fmt.Sprintf("doc-%02d", i)
		lang := "en"
		if i%3 == 0 {
			lang = "hi"
		}
		v := []float32{float32(r.NormFloat64()), float32(r.NormFloat64()), float32(r.NormFloat64()), float32(r.NormFloat64())}
		vecs[id] = v
		if err := w.Put(id, []byte(fmt.Sprintf(`{"lang":%q}`, lang))); err != nil {
			t.Fatalf("PUT %s: %v", id, err)
		}
		if err := w.VSetNS("docs", id, v); err != nil {
			t.Fatalf("VSET %s: %v", id, err)
		}
		if err := w.TSet("docs", id, fmt.Sprintf("common words and marker%02d", i)); err != nil {
			t.Fatalf("TSET %s: %v", id, err)
		}
	}

	// ── Visible from every other node ──────────────────────────────────────
	for i := 1; i < 3; i++ {
		c := dial(i)
		name := fmt.Sprintf("node %d", i+1)
		eventually(t, name+" vector k=0", 15*time.Second, func() error {
			got, err := c.VSearchWithOptions(0, vecs["doc-00"], veltrixclient.VectorSearchOptions{NS: "docs"})
			return wantCount(got, err, searchDocs)
		})
		eventually(t, name+" filtered vector", 10*time.Second, func() error {
			got, err := c.VSearchWithOptions(0, vecs["doc-00"], veltrixclient.VectorSearchOptions{
				NS: "docs", FilterField: "lang", FilterOp: "=", FilterValue: "hi"})
			return wantCount(got, err, searchDocs/3)
		})
		for _, target := range []string{"doc-07", "doc-42"} {
			got, err := c.VSearchWithOptions(1, vecs[target], veltrixclient.VectorSearchOptions{NS: "docs"})
			if err != nil || len(got) != 1 || got[0].ID != target {
				t.Fatalf("%s self-query %s: %+v err=%v", name, target, got, err)
			}
			if got[0].Score < 0.999 {
				t.Fatalf("%s: int8 re-rank should return the exact cosine 1, got %f", name, got[0].Score)
			}
		}
		eventually(t, name+" text", 10*time.Second, func() error {
			got, err := c.TSearch(0, "common", veltrixclient.TextSearchOptions{NS: "docs"})
			return wantCount(got, err, searchDocs)
		})
		got, err := c.TSearch(1, "marker33", veltrixclient.TextSearchOptions{NS: "docs"})
		if err != nil || len(got) != 1 || got[0].ID != "doc-33" {
			t.Fatalf("%s TSEARCH marker33: %+v err=%v", name, got, err)
		}
		got, err = c.HSearch(1, vecs["doc-21"], "marker21", veltrixclient.HybridSearchOptions{NS: "docs"})
		if err != nil || len(got) != 1 || got[0].ID != "doc-21" {
			t.Fatalf("%s HSEARCH: %+v err=%v", name, got, err)
		}
		eventually(t, name+" IDXQUERY", 10*time.Second, func() error {
			keys, err := c.IdxQuery("by_lang", "hi", 0)
			if err != nil {
				return err
			}
			if len(keys) != searchDocs/3 {
				return fmt.Errorf("%d keys, want %d", len(keys), searchDocs/3)
			}
			return nil
		})
	}

	// ── Node-to-node endpoint refuses unsigned callers ─────────────────────
	transfer := fmt.Sprintf("http://127.0.0.1:%d/internal/search", ports[1]+5)
	resp, err := http.Post(transfer, "application/json",
		strings.NewReader(`{"kind":"idxquery","index":"by_lang","value":"hi"}`))
	if err != nil {
		t.Fatalf("unsigned POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unsigned /internal/search: HTTP %d, want 401", resp.StatusCode)
	}

	// ── A dead node fails searches loudly, then drops out ──────────────────
	nodes[2].stop()
	_, err = w.VSearchWithOptions(0, vecs["doc-00"], veltrixclient.VectorSearchOptions{NS: "docs"})
	if err == nil || !strings.Contains(err.Error(), "did not answer") || !strings.Contains(err.Error(), "s3") {
		t.Fatalf("search right after killing s3: err=%v, want a failure naming s3", err)
	}
	t.Logf("fail-closed as expected: %v", err)
	eventually(t, "search after s3 is marked failed", 45*time.Second, func() error {
		got, err := w.VSearchWithOptions(0, vecs["doc-00"], veltrixclient.VectorSearchOptions{NS: "docs"})
		return wantCount(got, err, searchDocs)
	})
}

func wantCount(got []veltrixclient.VectorResult, err error, n int) error {
	if err != nil {
		return err
	}
	if len(got) != n {
		return fmt.Errorf("%d results, want %d", len(got), n)
	}
	return nil
}

// eventually retries fn until it succeeds or timeout passes.
func eventually(t *testing.T, what string, timeout time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s: not reached within %s: %v", what, timeout, err)
}
