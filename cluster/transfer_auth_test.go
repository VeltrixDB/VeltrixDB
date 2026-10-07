package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// startSignedAgent starts an agent whose listener requires secret, with an
// echo handler on /echo.
func startSignedAgent(t *testing.T, pm *PartitionMap, id string, secret []byte) *TransferAgent {
	t.Helper()
	ta := NewTransferAgent(pm, id, newMemStore(), "127.0.0.1:0")
	ta.SetClusterSecret(secret)
	ta.Handle("/echo", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
	}))
	if err := ta.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ta.Stop)
	pm.SetNodeTransferAddr(id, ta.BoundAddr())
	return ta
}

func TestTransferAuth_SignedRequestsOnly(t *testing.T) {
	pm := NewPartitionMap(DefaultClusterConfig())
	_ = pm.AddNode("a", "127.0.0.1", 7301)
	_ = pm.AddNode("b", "127.0.0.1", 7302)
	secret := []byte("0123456789abcdef-shared")
	startSignedAgent(t, pm, "a", secret)
	caller := NewTransferAgent(pm, "b", newMemStore(), "127.0.0.1:0")
	caller.SetClusterSecret(secret)

	var out struct{ OK bool }
	if err := caller.PostJSON(context.Background(), "a", "/echo", map[string]int{"x": 1}, &out); err != nil || !out.OK {
		t.Fatalf("signed request rejected: %v", err)
	}

	url := "http://" + pm.GetNodeTransferAddr("a")
	// Unsigned: both the new search-style route and the key-write route.
	for _, path := range []string{"/echo", "/transfer/keys"} {
		resp, err := http.Post(url+path, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unsigned %s: HTTP %d, want 401", path, resp.StatusCode)
		}
	}
	// Wrong secret.
	wrong := NewTransferAgent(pm, "b", newMemStore(), "127.0.0.1:0")
	wrong.SetClusterSecret([]byte("not-the-cluster-secret"))
	if err := wrong.PostJSON(context.Background(), "a", "/echo", 1, &out); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("wrong secret: err=%v, want HTTP 401", err)
	}
	// Tampered body under a valid signature.
	body := []byte(`{"x":1}`)
	req, _ := http.NewRequest(http.MethodPost, url+"/echo", bytes.NewReader([]byte(`{"x":2}`)))
	ts := fmt.Sprint(time.Now().Unix())
	req.Header.Set(clusterTimeHeader, ts)
	req.Header.Set(clusterAuthHeader, signClusterRequest(secret, ts, http.MethodPost, "/echo", body))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("tampered body accepted: %v %v", resp, err)
	}
	// Stale timestamp (replay outside the skew window).
	old := fmt.Sprint(time.Now().Add(-2 * clusterAuthMaxSkew).Unix())
	req, _ = http.NewRequest(http.MethodPost, url+"/echo", bytes.NewReader(body))
	req.Header.Set(clusterTimeHeader, old)
	req.Header.Set(clusterAuthHeader, signClusterRequest(secret, old, http.MethodPost, "/echo", body))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stale request accepted: %v %v", resp, err)
	}
	// Health stays open for probes.
	resp, err := http.Get(url + "/transfer/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health probe: %v %v", resp, err)
	}
	resp.Body.Close()
}

// TestTransferAuth_MigrationSigned: key migration between two agents sharing
// a secret still works (sendBatches signs its requests).
func TestTransferAuth_MigrationSigned(t *testing.T) {
	// RF=1: each key has one replica, so keys owned by dst move (with RF >=
	// node count every node is a replica and keys are only copied).
	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	_ = pm.AddNode("src", "127.0.0.1", 7311)
	secret := []byte("migration-secret-0123456789")
	srcStore, dstStore := newMemStore(), newMemStore()
	src := NewTransferAgent(pm, "src", srcStore, "127.0.0.1:0")
	src.SetClusterSecret(secret)
	dst := NewTransferAgent(pm, "dst", dstStore, "127.0.0.1:0")
	dst.SetClusterSecret(secret)
	if err := dst.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dst.Stop)
	for i := 0; i < 50; i++ {
		_ = srcStore.Put(fmt.Sprintf("k%02d", i), []byte("v"), -1)
	}
	_ = srcStore.Put("@vecns/docs", []byte(`{"dim":4,"quant":"int8"}`), -1)
	if err := pm.AddNodeWithTransfer("dst", "127.0.0.1", 7312, dst.BoundAddr()); err != nil {
		t.Fatal(err)
	}
	if err := pm.Rebalance(pm.PartitionCount()); err != nil {
		t.Fatal(err)
	}
	if err := src.MigrateToNewOwners(); err != nil {
		t.Fatalf("signed migration: %v", err)
	}
	moved := 0
	for _, k := range dstStore.ScanKeys() {
		if !strings.HasPrefix(k, "@vecns/") {
			moved++
		}
	}
	if moved == 0 || moved+len(srcStore.ScanKeys())-1 != 50 {
		t.Fatalf("moved=%d src=%d, want a split of 50 keys", moved, len(srcStore.ScanKeys()))
	}
	// The pinned namespace setting is on both nodes.
	if _, err := srcStore.Get("@vecns/docs"); err != nil {
		t.Fatal("pinned key was deleted from the source")
	}
	if _, err := dstStore.Get("@vecns/docs"); err != nil {
		t.Fatal("pinned key was not copied to the destination")
	}
}
