package replication

// tls_test.go — TLS transport tests using self-signed certificates generated
// in-test with crypto/x509 (no fixtures, no external dependencies).

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// testPKI holds file paths for an in-test certificate authority and the leaf
// certificates it signed.
type testPKI struct {
	caFile     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

func writePEMFile(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// newTestPKI generates a CA plus server and client leaf certificates (valid
// for 127.0.0.1/localhost) and writes them as PEM files under t.TempDir().
func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()

	// Certificate authority.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "veltrix-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	pki := &testPKI{
		caFile:     filepath.Join(dir, "ca.pem"),
		serverCert: filepath.Join(dir, "server.pem"),
		serverKey:  filepath.Join(dir, "server.key"),
		clientCert: filepath.Join(dir, "client.pem"),
		clientKey:  filepath.Join(dir, "client.key"),
	}
	writePEMFile(t, pki.caFile, "CERTIFICATE", caDER)

	// Leaf certificates (usable for both server and client auth).
	leaf := func(serial int64, cn, certFile, keyFile string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate %s key: %v", cn, err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			DNSNames:     []string{"localhost"},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create %s cert: %v", cn, err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatalf("marshal %s key: %v", cn, err)
		}
		writePEMFile(t, certFile, "CERTIFICATE", der)
		writePEMFile(t, keyFile, "EC PRIVATE KEY", keyDER)
	}
	leaf(2, "veltrix-test-server", pki.serverCert, pki.serverKey)
	leaf(3, "veltrix-test-client", pki.clientCert, pki.clientKey)

	return pki
}

// startTLSServer starts a ReplicationServer with the given TLS config,
// applying ops into a fresh mockStore.
func startTLSServer(t *testing.T, tlsCfg *TransportTLSConfig) (*ReplicationServer, *mockStore) {
	t.Helper()
	store := &mockStore{}
	srv, err := NewReplicationServerTLS("127.0.0.1:0", store.apply, tlsCfg)
	if err != nil {
		t.Fatalf("NewReplicationServerTLS: %v", err)
	}
	go srv.ListenAndServe()
	return srv, store
}

// TestReplication_TLSRoundTrip: a TLS client sends a batch to a TLS server;
// Send returns only after the server applied the batch and acked, so the
// store contents can be asserted immediately — no sleeps.
func TestReplication_TLSRoundTrip(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	srv, store := startTLSServer(t, &TransportTLSConfig{
		TLSEnabled: true,
		CertFile:   pki.serverCert,
		KeyFile:    pki.serverKey,
	})
	defer srv.Stop()

	client, err := NewReplicationClientTLS(srv.Addr(), &TransportTLSConfig{
		TLSEnabled: true,
		CAFile:     pki.caFile,
	})
	if err != nil {
		t.Fatalf("NewReplicationClientTLS: %v", err)
	}
	defer client.Close()

	ops := []*WriteOperation{
		newOp(1, "tls-k1", []byte("v1")),
		newOp(2, "tls-k2", []byte("v2")),
		newOp(3, "tls-k3", []byte("v3")),
	}
	if err := client.Send(ops); err != nil {
		t.Fatalf("Send over TLS: %v", err)
	}

	if got := store.count(); got != len(ops) {
		t.Fatalf("server applied %d ops, want %d", got, len(ops))
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for i, op := range store.ops {
		if op.Key != ops[i].Key || string(op.Value) != string(ops[i].Value) {
			t.Errorf("op %d mismatch: got %q=%q", i, op.Key, op.Value)
		}
	}
}

// TestReplication_TLSMutualAuth: with RequireClientCert the server verifies
// the client certificate against the CA (mTLS) and the round trip succeeds.
func TestReplication_TLSMutualAuth(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	srv, store := startTLSServer(t, &TransportTLSConfig{
		TLSEnabled:        true,
		CertFile:          pki.serverCert,
		KeyFile:           pki.serverKey,
		CAFile:            pki.caFile,
		RequireClientCert: true,
	})
	defer srv.Stop()

	client, err := NewReplicationClientTLS(srv.Addr(), &TransportTLSConfig{
		TLSEnabled: true,
		CAFile:     pki.caFile,
		CertFile:   pki.clientCert,
		KeyFile:    pki.clientKey,
	})
	if err != nil {
		t.Fatalf("NewReplicationClientTLS: %v", err)
	}
	defer client.Close()

	if err := client.Send([]*WriteOperation{newOp(1, "mtls-k", []byte("v"))}); err != nil {
		t.Fatalf("Send over mTLS: %v", err)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("server applied %d ops, want 1", got)
	}
}

// TestReplication_PlaintextClientFailsAgainstTLSServer: a plaintext client
// must fail against a TLS server, and nothing may be applied.
func TestReplication_PlaintextClientFailsAgainstTLSServer(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	srv, store := startTLSServer(t, &TransportTLSConfig{
		TLSEnabled: true,
		CertFile:   pki.serverCert,
		KeyFile:    pki.serverKey,
	})
	defer srv.Stop()

	client := NewReplicationClient(srv.Addr()) // plaintext
	defer client.Close()

	if err := client.Send([]*WriteOperation{newOp(1, "plain-k", []byte("v"))}); err == nil {
		t.Fatal("plaintext Send against TLS server succeeded; want error")
	}
	if got := store.count(); got != 0 {
		t.Fatalf("server applied %d ops from a failed plaintext client, want 0", got)
	}
}

// TestReplication_TLSRejectsPre13Clients: the server enforces TLS 1.3 as the
// minimum protocol version.
func TestReplication_TLSRejectsPre13Clients(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	srv, _ := startTLSServer(t, &TransportTLSConfig{
		TLSEnabled: true,
		CertFile:   pki.serverCert,
		KeyFile:    pki.serverKey,
	})
	defer srv.Stop()

	caPEM, err := os.ReadFile(pki.caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", srv.Addr(), &tls.Config{
		RootCAs:    pool,
		MaxVersion: tls.VersionTLS12,
	})
	if err == nil {
		conn.Close()
		t.Fatal("TLS 1.2 handshake succeeded; server must require TLS 1.3")
	}
}

// TestReplication_EngineTLSEndToEnd: engine-level wiring — AddReplica creates
// a TLS client from ReplicationConfig.TLS, the background worker flushes the
// write, and WaitForReplication confirms the replica's ack.
func TestReplication_EngineTLSEndToEnd(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	tlsCfg := &TransportTLSConfig{
		TLSEnabled: true,
		CertFile:   pki.serverCert, // reused as client cert; server does not require it
		KeyFile:    pki.serverKey,
		CAFile:     pki.caFile,
	}

	srv, store := startTLSServer(t, tlsCfg)
	defer srv.Stop()

	_, portStr, err := net.SplitHostPort(srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portStr)

	cfg := fastCfg(QuorumConsistency)
	cfg.TLS = tlsCfg
	re := NewReplicationEngine("primary", cfg)
	if err := re.AddReplicaWithReplPort("tls-replica", "127.0.0.1", port-1, port); err != nil {
		t.Fatalf("AddReplicaWithReplPort: %v", err)
	}
	re.Start()
	defer re.Close()

	op := newOp(1, fmt.Sprintf("e2e-%d", port), []byte("val"))
	if err := re.OnLocalWrite(op); err != nil {
		t.Fatalf("OnLocalWrite: %v", err)
	}
	if err := re.WaitForReplication(1, 2, 5000); err != nil {
		t.Fatalf("WaitForReplication over TLS: %v", err)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("replica applied %d ops, want 1", got)
	}
}
