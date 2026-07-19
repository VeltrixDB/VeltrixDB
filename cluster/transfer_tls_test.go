package cluster

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// genSelfSignedCert writes a self-signed ECDSA P-256 certificate + key
// (valid for 127.0.0.1 / localhost, usable as its own CA and as a client
// certificate) into dir and returns the file paths.
func genSelfSignedCert(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "veltrixdb-transfer-test"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true, // self-signed: acts as its own CA
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(crand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// registerTransferAddr points nodeID's transfer traffic at addr and undoes the
// registration at test end (transferAddrs is package-global).
func registerTransferAddr(t *testing.T, nodeID, addr string) {
	t.Helper()
	transferAddrs.Store(nodeID, addr)
	t.Cleanup(func() { transferAddrs.Delete(nodeID) })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestTransfer_TLSRoundTrip: full mutual-TLS migration between two agents
// using in-test self-signed certificates. Keys owned by the destination move
// over HTTPS and are deleted at the source; keys owned by the source stay.
func TestTransfer_TLSRoundTrip(t *testing.T) {
	t.Parallel()

	certFile, keyFile := genSelfSignedCert(t, t.TempDir())
	tlsCfg := &TransferTLSConfig{
		Enabled:           true,
		CertFile:          certFile,
		KeyFile:           keyFile,
		CAFile:            certFile, // self-signed cert is its own CA
		RequireClientCert: true,     // exercise mTLS
	}

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	const srcID, dstID = "tlsrt-src", "tlsrt-dst"
	for i, id := range []string{srcID, dstID} {
		if err := pm.AddNode(id, "127.0.0.1", 9000+i); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	srcStore, dstStore := newMemStore(), newMemStore()
	src, err := NewTransferAgentTLS(pm, srcID, srcStore, "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("NewTransferAgentTLS(src): %v", err)
	}
	dst, err := NewTransferAgentTLS(pm, dstID, dstStore, "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("NewTransferAgentTLS(dst): %v", err)
	}
	if err := src.Start(); err != nil {
		t.Fatalf("src.Start: %v", err)
	}
	if err := dst.Start(); err != nil {
		t.Fatalf("dst.Start: %v", err)
	}
	t.Cleanup(src.Stop)
	t.Cleanup(dst.Stop)
	registerTransferAddr(t, srcID, src.BoundAddr())
	registerTransferAddr(t, dstID, dst.BoundAddr())

	// Load keys spanning the ring into the source store.
	const n = 60
	for i := 0; i < n; i++ {
		k := diverseKey(i)
		if err := srcStore.Put(k, []byte(fmt.Sprintf("val-%d", i)), -1); err != nil {
			t.Fatalf("seed put: %v", err)
		}
	}

	if err := src.MigrateToNewOwners(); err != nil {
		t.Fatalf("MigrateToNewOwners over TLS: %v", err)
	}

	moved, stayed := 0, 0
	for i := 0; i < n; i++ {
		k := diverseKey(i)
		owner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%q): %v", k, err)
		}
		switch owner {
		case dstID:
			moved++
			if _, err := dstStore.Get(k); err != nil {
				t.Errorf("key %q owned by dst missing from dst store: %v", k, err)
			}
			if _, err := srcStore.Get(k); err == nil {
				t.Errorf("key %q owned by dst still present at src", k)
			}
		case srcID:
			stayed++
			if _, err := srcStore.Get(k); err != nil {
				t.Errorf("key %q owned by src missing from src store: %v", k, err)
			}
		default:
			t.Fatalf("key %q owned by unexpected node %q", k, owner)
		}
	}
	if moved == 0 {
		t.Fatal("no keys were owned by the destination — test exercised nothing over TLS")
	}
	t.Logf("TLS round-trip: moved=%d stayed=%d", moved, stayed)
}

// TestTransfer_PlaintextToTLSFails: a plaintext agent must fail to deliver
// batches to a TLS-only receiver, and no keys may be applied.
func TestTransfer_PlaintextToTLSFails(t *testing.T) {
	t.Parallel()

	certFile, keyFile := genSelfSignedCert(t, t.TempDir())
	tlsCfg := &TransferTLSConfig{Enabled: true, CertFile: certFile, KeyFile: keyFile, CAFile: certFile}

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	const srcID, dstID = "pt2tls-src", "pt2tls-dst"
	for i, id := range []string{srcID, dstID} {
		if err := pm.AddNode(id, "127.0.0.1", 9000+i); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	dstStore := newMemStore()
	dst, err := NewTransferAgentTLS(pm, dstID, dstStore, "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("NewTransferAgentTLS: %v", err)
	}
	if err := dst.Start(); err != nil {
		t.Fatalf("dst.Start: %v", err)
	}
	t.Cleanup(dst.Stop)
	registerTransferAddr(t, dstID, dst.BoundAddr())

	src := NewTransferAgent(pm, srcID, newMemStore(), "127.0.0.1:0") // plaintext

	sent, err := src.sendBatches(dstID, []KeyValue{{Key: "k1", Value: []byte("v1"), TTL: -1}})
	if err == nil {
		t.Fatal("plaintext sendBatches to TLS receiver succeeded; want error")
	}
	if sent != 0 {
		t.Errorf("sent = %d; want 0", sent)
	}
	if dstStore.count() != 0 {
		t.Errorf("TLS receiver applied %d keys from plaintext sender; want 0", dstStore.count())
	}
}

// TestTransfer_StaleEpochRejected: a KeyBatch carrying an epoch older than the
// receiver's is rejected with HTTP 409 and not applied; a batch with the
// current epoch is accepted.
func TestTransfer_StaleEpochRejected(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	const dstID = "ser-dst"
	if err := pm.AddNode(dstID, "127.0.0.1", 9000); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	dstStore := newMemStore()
	dst := NewTransferAgent(pm, dstID, dstStore, "127.0.0.1:0")
	if err := dst.Start(); err != nil {
		t.Fatalf("dst.Start: %v", err)
	}
	t.Cleanup(dst.Stop)

	staleEpoch := pm.Epoch()
	// Advance the epoch with a membership change — as if the receiver learned
	// of a newer topology while a partitioned sender did not.
	if err := pm.AddNode("ser-extra", "127.0.0.1", 9001); err != nil {
		t.Fatalf("AddNode(extra): %v", err)
	}
	if pm.Epoch() <= staleEpoch {
		t.Fatalf("epoch did not advance on membership change: %d → %d", staleEpoch, pm.Epoch())
	}

	url := "http://" + dst.BoundAddr() + "/transfer/keys"
	post := func(epoch uint64, key string) int {
		t.Helper()
		body, err := json.Marshal(KeyBatch{
			SourceNode: "ser-src",
			Epoch:      epoch,
			Keys:       []KeyValue{{Key: key, Value: []byte("v"), TTL: -1}},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if code := post(staleEpoch, "stale-key"); code != http.StatusConflict {
		t.Errorf("stale-epoch batch → HTTP %d; want %d", code, http.StatusConflict)
	}
	if _, err := dstStore.Get("stale-key"); err == nil {
		t.Error("stale-epoch batch was applied; key must not exist")
	}

	if code := post(pm.Epoch(), "fresh-key"); code != http.StatusOK {
		t.Errorf("current-epoch batch → HTTP %d; want %d", code, http.StatusOK)
	}
	if _, err := dstStore.Get("fresh-key"); err != nil {
		t.Errorf("current-epoch batch not applied: %v", err)
	}
}
