package consensus

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ── In-test certificate authority ─────────────────────────────────────────────

type testPKI struct {
	caPEM     string
	serverPEM string
	serverKey string
	clientPEM string
	clientKey string
}

// newTestPKI generates a CA plus CA-signed server and client certificates
// (SANs: 127.0.0.1, localhost) and writes them as PEM files under dir.
func newTestPKI(t *testing.T, dir string) testPKI {
	t.Helper()

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

	leaf := func(cn string, serial int64) (certPEM, keyPEM []byte) {
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
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	}

	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}

	srvCert, srvKey := leaf("veltrix-test-server", 2)
	cliCert, cliKey := leaf("veltrix-test-client", 3)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	return testPKI{
		caPEM:     write("ca.pem", caPEM),
		serverPEM: write("server.pem", srvCert),
		serverKey: write("server-key.pem", srvKey),
		clientPEM: write("client.pem", cliCert),
		clientKey: write("client-key.pem", cliKey),
	}
}

// staticHandler echoes each RPC's term back, marking success.
type staticHandler struct{}

func (staticHandler) HandleRequestVote(a RequestVoteArgs) RequestVoteReply {
	return RequestVoteReply{Term: a.Term, VoteGranted: true}
}
func (staticHandler) HandleAppendEntries(a AppendEntriesArgs) AppendEntriesReply {
	return AppendEntriesReply{Term: a.Term, Success: true}
}
func (staticHandler) HandleInstallSnapshot(a InstallSnapshotArgs) InstallSnapshotReply {
	return InstallSnapshotReply{Term: a.Term}
}

// TestTransport_TLSRoundTrip: all three RPC types round-trip over mutual
// TLS 1.3; a plaintext client and a certificate-less TLS client are both
// rejected.
func TestTransport_TLSRoundTrip(t *testing.T) {
	pki := newTestPKI(t, t.TempDir())

	serverOpts := TLSOptions{
		Enabled:  true,
		CertFile: pki.serverPEM,
		KeyFile:  pki.serverKey,
		CAFile:   pki.caPEM, // require + verify client certs (mTLS)
	}
	srv, err := NewRPCServerTLS("127.0.0.1:0", staticHandler{}, serverOpts)
	if err != nil {
		t.Fatalf("NewRPCServerTLS: %v", err)
	}
	go srv.ListenAndServe()
	defer srv.Stop()

	clientOpts := TLSOptions{
		Enabled:  true,
		CertFile: pki.clientPEM,
		KeyFile:  pki.clientKey,
		CAFile:   pki.caPEM,
	}
	tr, err := NewTCPTransportTLS(map[string]string{"peer": srv.Addr()}, clientOpts)
	if err != nil {
		t.Fatalf("NewTCPTransportTLS: %v", err)
	}
	defer tr.Close()

	rvReply, err := tr.SendRequestVote("peer", RequestVoteArgs{Term: 7, CandidateID: "tls-c"})
	if err != nil {
		t.Fatalf("SendRequestVote over TLS: %v", err)
	}
	if rvReply.Term != 7 || !rvReply.VoteGranted {
		t.Errorf("RequestVote reply mismatch: %+v", rvReply)
	}

	aeReply, err := tr.SendAppendEntries("peer", AppendEntriesArgs{Term: 9, LeaderID: "tls-l"})
	if err != nil {
		t.Fatalf("SendAppendEntries over TLS: %v", err)
	}
	if aeReply.Term != 9 || !aeReply.Success {
		t.Errorf("AppendEntries reply mismatch: %+v", aeReply)
	}

	snapData := bytes.Repeat([]byte("veltrix"), 1024)
	isReply, err := tr.SendInstallSnapshot("peer", InstallSnapshotArgs{
		Term: 11, LeaderID: "tls-l", LastIncludedIndex: 42, LastIncludedTerm: 11,
		Config: Configuration{Servers: []string{"a", "b"}}, Data: snapData,
	})
	if err != nil {
		t.Fatalf("SendInstallSnapshot over TLS: %v", err)
	}
	if isReply.Term != 11 {
		t.Errorf("InstallSnapshot reply mismatch: %+v", isReply)
	}

	// A plaintext client must not get an RPC through a TLS listener.
	plain := NewTCPTransport(map[string]string{"peer": srv.Addr()})
	if _, err := plain.SendRequestVote("peer", RequestVoteArgs{Term: 1}); err == nil {
		t.Error("plaintext client completed an RPC against the TLS server")
	}
	plain.Close()

	// mTLS: a TLS client WITHOUT a client certificate must be rejected.
	noCert, err := NewTCPTransportTLS(map[string]string{"peer": srv.Addr()},
		TLSOptions{Enabled: true, CAFile: pki.caPEM})
	if err != nil {
		t.Fatalf("NewTCPTransportTLS (no client cert): %v", err)
	}
	if _, err := noCert.SendRequestVote("peer", RequestVoteArgs{Term: 1}); err == nil {
		t.Error("client without certificate completed an RPC against the mTLS server")
	}
	noCert.Close()
}
