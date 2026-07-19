// Package security provides TLS configuration helpers and RBAC enforcement
// for VeltrixDB's TCP data server, HTTP metrics server, and replication transport.
package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSMode controls how TLS client certificates are handled.
type TLSMode int

const (
	TLSModeDisabled   TLSMode = iota // no TLS
	TLSModeServerOnly                // server cert only; clients are not verified
	TLSModeMutual                    // mTLS: server + client certs required
)

// TLSConfig holds paths to TLS credential files.
type TLSConfig struct {
	CertFile string  // server certificate (PEM)
	KeyFile  string  // server private key (PEM)
	CAFile   string  // CA certificate for client verification (mTLS only)
	Mode     TLSMode // derived automatically if empty
}

// IsEnabled returns true when TLS should be active.
func (c *TLSConfig) IsEnabled() bool {
	return c != nil && c.CertFile != "" && c.KeyFile != ""
}

// ServerTLSConfig builds a *tls.Config suitable for a server listener.
//
// With CAFile set, mTLS is enabled: every connecting client must present a
// certificate signed by the given CA.  Without CAFile, only server-side TLS
// is active — clients are not verified.
func ServerTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	if !cfg.IsEnabled() {
		return nil, fmt.Errorf("TLS disabled: CertFile and KeyFile required")
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		// TLS 1.3 cipher suites are fixed; this only covers TLS 1.2 fallback.
		CipherSuites: []uint16{
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}

	if cfg.CAFile != "" {
		pool, err := loadCertPool(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("load CA cert: %w", err)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

// ClientTLSConfig builds a *tls.Config for a client dialling a TLS server.
// Pass caFile="" to trust the system root CAs (no mTLS).
// Pass certFile+keyFile to present a client certificate (mTLS).
func ClientTLSConfig(caFile, certFile, keyFile string, serverName string) (*tls.Config, error) {
	cfg := &tls.Config{
		ServerName: serverName,
		MinVersion: tls.VersionTLS13,
	}

	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("load CA cert: %w", err)
		}
		cfg.RootCAs = pool
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid PEM certs found in %s", caFile)
	}
	return pool, nil
}
