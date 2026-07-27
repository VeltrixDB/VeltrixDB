package replication

// transport.go — TCP-based replication transport.
//
// Replaces the no-op sendReplicationRPC stub with a real implementation:
//
//   • ReplicationServer listens for incoming batches on a TCP port.
//   • ReplicationClient dials the primary and streams WriteOperation batches.
//
// Wire framing (little-endian, all fields):
//   request : [4B magic=0x52454C50 "RELP"][4B payloadLen][gob-encoded []WriteOperation]
//   response: [1B status]  (0x00 = batch applied, 0x01 = apply error)
//
// The 1-byte response is the replica's application-level ack: the client's
// Send only returns nil after the replica has applied the batch and written
// the ack byte back.  A successful TCP write alone is never treated as an ack.
//
// Design notes
//   • One persistent TCP (or TLS 1.3, see TransportTLSConfig) connection per
//     (source, destination) pair.  The connection is re-established on error
//     with exponential back-off, bounded to replMaxSendAttempts per Send so a
//     dead replica surfaces as an error instead of blocking forever.
//   • The server side applies received operations to the local storage engine
//     via the ApplyFn callback, which the caller wires to engine.Put / engine.Delete.
//   • Quorum and sequence tracking remain in ReplicationEngine.replicateBatch;
//     this file only handles the network layer.

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	replMagic           uint32 = 0x52454C50 // "RELP"
	replDialTimeout            = 3 * time.Second
	replSendTimeout            = 10 * time.Second
	replMaxBackoff             = 30 * time.Second
	replMaxSendAttempts        = 3

	replAckOK  byte = 0x00
	replAckErr byte = 0x01

	// Message kinds (byte after the magic in every request/response frame).
	kindBatch   byte = 0x00 // req: gob([]*WriteOperation)  resp: [1B ack status]
	kindDigests byte = 0x01 // req: (empty)                 resp: gob(map[uint16]uint64)
	kindFetch   byte = 0x02 // req: gob(shardID uint16)      resp: gob([]SyncEntry)
	kindError   byte = 0xFF // resp only: payload is a UTF-8 error message
)

// SyncEntry is the wire form of one key's replicable state during anti-entropy.
// It mirrors storage.SyncEntry; the cmd/server bridge converts between the two
// (replication must not import storage). It is gob-encoded on the wire.
type SyncEntry struct {
	Key        string
	Value      []byte
	LWWStampNs int64
	Tombstone  bool
	TTLSeconds int32
}

// ── TLS configuration ─────────────────────────────────────────────────────────

// TransportTLSConfig configures TLS 1.3 for the inter-node replication
// transport (both the clients created by AddReplica / NewReplicationClientTLS
// and the server created by NewReplicationServerTLS).  A nil config or
// TLSEnabled=false keeps the default plaintext TCP transport.
// Uses only the standard library (crypto/tls, crypto/x509).
type TransportTLSConfig struct {
	// TLSEnabled turns TLS on.  All other fields are ignored when false.
	TLSEnabled bool

	// CertFile / KeyFile are the node's PEM-encoded certificate and private
	// key.  Required on the server side; on the client side they are
	// presented when the server requires client certificates (mTLS).
	CertFile string
	KeyFile  string

	// CAFile is a PEM CA bundle used by clients to verify the server
	// certificate and, when RequireClientCert is set, by the server to
	// verify client certificates.  Empty means the host's root CA pool.
	CAFile string

	// RequireClientCert makes the server demand and verify a client
	// certificate signed by CAFile (mutual TLS).  CAFile is then required.
	RequireClientCert bool

	// ServerName overrides the expected name on the server certificate when
	// dialing.  Empty means the host portion of the dialed address.
	ServerName string
}

// enabled reports whether TLS should be used.  Safe on a nil receiver.
func (c *TransportTLSConfig) enabled() bool { return c != nil && c.TLSEnabled }

// caPool loads CAFile into a cert pool.  Returns (nil, nil) when CAFile is
// empty, which callers treat as "use the system pool".
func (c *TransportTLSConfig) caPool() (*x509.CertPool, error) {
	if c.CAFile == "" {
		return nil, nil
	}
	pemBytes, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no valid CA certificates in %s", c.CAFile)
	}
	return pool, nil
}

// serverTLSConfig builds the *tls.Config for the replication listener.
func (c *TransportTLSConfig) serverTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}
	if c.RequireClientCert {
		pool, err := c.caPool()
		if err != nil {
			return nil, err
		}
		if pool == nil {
			return nil, fmt.Errorf("RequireClientCert set but CAFile is empty")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// clientTLSConfig builds the *tls.Config used to dial a replica.  serverName
// is the host portion of the dialed address unless overridden via ServerName.
func (c *TransportTLSConfig) clientTLSConfig(serverName string) (*tls.Config, error) {
	if c.ServerName != "" {
		serverName = c.ServerName
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
	}
	pool, err := c.caPool()
	if err != nil {
		return nil, err
	}
	if pool != nil {
		cfg.RootCAs = pool
	}
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// ApplyFn is called by the server for each received operation.
// The implementation is expected to call engine.Put or engine.Delete.
type ApplyFn func(op *WriteOperation) error

// ── Client ────────────────────────────────────────────────────────────────────

// ReplicationClient maintains a persistent TCP (or TLS) connection to one
// replica and sends batches of WriteOperations.
type ReplicationClient struct {
	addr      string
	tlsConfig *tls.Config // nil = plaintext TCP
	sendMu    sync.Mutex  // serializes Send: one in-flight batch per connection
	mu        sync.Mutex  // guards conn
	conn      net.Conn
	backoff   time.Duration
	done      chan struct{}
}

// NewReplicationClient creates a plaintext-TCP client that will dial addr on
// first send.
func NewReplicationClient(addr string) *ReplicationClient {
	return &ReplicationClient{
		addr:    addr,
		backoff: 100 * time.Millisecond,
		done:    make(chan struct{}),
	}
}

// NewReplicationClientTLS creates a client that dials addr over TLS 1.3.
// The certificate material in tlsCfg is loaded eagerly so misconfiguration
// surfaces here rather than on the first Send; the handshake itself happens
// on first Send.  A nil / disabled tlsCfg yields a plaintext client.
func NewReplicationClientTLS(addr string, tlsCfg *TransportTLSConfig) (*ReplicationClient, error) {
	c := NewReplicationClient(addr)
	if !tlsCfg.enabled() {
		return c, nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	cfg, err := tlsCfg.clientTLSConfig(host)
	if err != nil {
		return nil, fmt.Errorf("tls client for %s: %w", addr, err)
	}
	c.tlsConfig = cfg
	return c, nil
}

// Send sends a batch of operations to the replica and waits for the replica's
// application-level ack.  Returns nil only once the replica acknowledged the
// batch.  On network error it re-dials with exponential back-off, giving up
// after replMaxSendAttempts so a dead replica yields an error instead of
// blocking the caller indefinitely.
func (c *ReplicationClient) Send(ops []*WriteOperation) error {
	if len(ops) == 0 {
		return nil
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	var lastErr error
	for attempt := 0; attempt < replMaxSendAttempts; attempt++ {
		if attempt > 0 && !c.wait() {
			return fmt.Errorf("client closed")
		}
		conn, err := c.getConn()
		if err != nil {
			lastErr = err
			continue
		}
		conn.SetDeadline(time.Now().Add(replSendTimeout))
		payload, encErr := gobEncode(ops)
		if encErr != nil {
			return fmt.Errorf("encode batch: %w", encErr) // not retryable
		}
		if err := writeFrame(conn, kindBatch, payload); err != nil {
			lastErr = err
			c.closeConn()
			continue
		}
		kind, resp, err := readFrame(conn)
		if err != nil {
			lastErr = fmt.Errorf("read ack: %w", err)
			c.closeConn()
			continue
		}
		if kind == kindError {
			return fmt.Errorf("replica error: %s", string(resp)) // applied-error is not retryable
		}
		if kind != kindBatch || len(resp) != 1 || resp[0] != replAckOK {
			return fmt.Errorf("replica reported apply error")
		}
		c.backoff = 100 * time.Millisecond // reset on success
		return nil
	}
	return fmt.Errorf("send to %s failed after %d attempts: %w", c.addr, replMaxSendAttempts, lastErr)
}

// roundTrip sends one framed request and reads one framed response, serialized
// with sendMu (one in-flight exchange per connection). Used by the anti-entropy
// pull RPCs. On a kindError response it returns the remote error message.
func (c *ReplicationClient) roundTrip(kind byte, payload []byte) (byte, []byte, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	var lastErr error
	for attempt := 0; attempt < replMaxSendAttempts; attempt++ {
		if attempt > 0 && !c.wait() {
			return 0, nil, fmt.Errorf("client closed")
		}
		conn, err := c.getConn()
		if err != nil {
			lastErr = err
			continue
		}
		conn.SetDeadline(time.Now().Add(replSendTimeout))
		if err := writeFrame(conn, kind, payload); err != nil {
			lastErr = err
			c.closeConn()
			continue
		}
		rKind, resp, err := readFrame(conn)
		if err != nil {
			lastErr = err
			c.closeConn()
			continue
		}
		if rKind == kindError {
			return 0, nil, fmt.Errorf("remote: %s", string(resp))
		}
		c.backoff = 100 * time.Millisecond
		return rKind, resp, nil
	}
	return 0, nil, fmt.Errorf("rpc to %s failed after %d attempts: %w", c.addr, replMaxSendAttempts, lastErr)
}

// FetchDigests requests the replica's per-shard digests (anti-entropy).
func (c *ReplicationClient) FetchDigests() (map[uint16]uint64, error) {
	kind, resp, err := c.roundTrip(kindDigests, nil)
	if err != nil {
		return nil, err
	}
	if kind != kindDigests {
		return nil, fmt.Errorf("unexpected response kind 0x%02x", kind)
	}
	var digests map[uint16]uint64
	if err := gobDecode(resp, &digests); err != nil {
		return nil, fmt.Errorf("decode digests: %w", err)
	}
	return digests, nil
}

// FetchShard requests all of the replica's entries for one shard (anti-entropy).
func (c *ReplicationClient) FetchShard(shardID uint16) ([]SyncEntry, error) {
	req, err := gobEncode(shardID)
	if err != nil {
		return nil, err
	}
	kind, resp, err := c.roundTrip(kindFetch, req)
	if err != nil {
		return nil, err
	}
	if kind != kindFetch {
		return nil, fmt.Errorf("unexpected response kind 0x%02x", kind)
	}
	var entries []SyncEntry
	if err := gobDecode(resp, &entries); err != nil {
		return nil, fmt.Errorf("decode shard entries: %w", err)
	}
	return entries, nil
}

// Close shuts down the client.
func (c *ReplicationClient) Close() {
	close(c.done)
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
}

func (c *ReplicationClient) getConn() (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}
	var conn net.Conn
	var err error
	if c.tlsConfig != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: replDialTimeout}, "tcp", c.addr, c.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", c.addr, replDialTimeout)
	}
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *ReplicationClient) closeConn() {
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
}

// wait sleeps the current back-off duration and doubles it (up to replMaxBackoff).
// Returns false if the client was closed during the wait.
func (c *ReplicationClient) wait() bool {
	t := time.NewTimer(c.backoff)
	defer t.Stop()
	c.backoff *= 2
	if c.backoff > replMaxBackoff {
		c.backoff = replMaxBackoff
	}
	select {
	case <-c.done:
		return false
	case <-t.C:
		return true
	}
}

// ── Server ────────────────────────────────────────────────────────────────────

// ReplicationServer accepts incoming batches and applies them via applyFn, and
// serves anti-entropy digest/fetch requests via the optional sync handlers.
type ReplicationServer struct {
	listener net.Listener
	applyFn  ApplyFn
	// digestFn / fetchFn serve anti-entropy pulls. Nil ⇒ the server replies
	// with kindError to those requests (anti-entropy not enabled on this node).
	digestFn func() (map[uint16]uint64, error)
	fetchFn  func(shardID uint16) ([]SyncEntry, error)
	done     chan struct{}
	wg       sync.WaitGroup
}

// SetSyncHandlers wires the anti-entropy digest/fetch handlers. Safe to call
// before ListenAndServe.
func (s *ReplicationServer) SetSyncHandlers(
	digestFn func() (map[uint16]uint64, error),
	fetchFn func(shardID uint16) ([]SyncEntry, error),
) {
	s.digestFn = digestFn
	s.fetchFn = fetchFn
}

// NewReplicationServer creates a plaintext-TCP server but does not start it.
func NewReplicationServer(listenAddr string, applyFn ApplyFn) (*ReplicationServer, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", listenAddr, err)
	}
	return &ReplicationServer{
		listener: l,
		applyFn:  applyFn,
		done:     make(chan struct{}),
	}, nil
}

// NewReplicationServerTLS creates a server that accepts TLS 1.3 connections
// only.  A nil / disabled tlsCfg yields a plaintext server (same as
// NewReplicationServer).
func NewReplicationServerTLS(listenAddr string, applyFn ApplyFn, tlsCfg *TransportTLSConfig) (*ReplicationServer, error) {
	srv, err := NewReplicationServer(listenAddr, applyFn)
	if err != nil {
		return nil, err
	}
	if !tlsCfg.enabled() {
		return srv, nil
	}
	cfg, err := tlsCfg.serverTLSConfig()
	if err != nil {
		srv.listener.Close()
		return nil, fmt.Errorf("tls server on %s: %w", listenAddr, err)
	}
	srv.listener = tls.NewListener(srv.listener, cfg)
	return srv, nil
}

// ListenAndServe accepts connections until Stop is called.
func (s *ReplicationServer) ListenAndServe() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				log.Printf("[repl] accept error: %v", err)
				continue
			}
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer c.Close()
			s.serveConn(c)
		}(conn)
	}
}

// Stop shuts down the listener.
func (s *ReplicationServer) Stop() {
	close(s.done)
	s.listener.Close()
	s.wg.Wait()
}

// Addr returns the listen address.
func (s *ReplicationServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *ReplicationServer) serveConn(conn net.Conn) {
	for {
		conn.SetDeadline(time.Now().Add(60 * time.Second))
		kind, payload, err := readFrame(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("[repl] receive error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if err := s.handleRequest(conn, kind, payload); err != nil {
			return // connection is unusable
		}
	}
}

// handleRequest dispatches one request frame and writes exactly one response
// frame. Returns an error only when the connection can no longer be used.
func (s *ReplicationServer) handleRequest(conn net.Conn, kind byte, payload []byte) error {
	switch kind {
	case kindBatch:
		var ops []*WriteOperation
		if err := gobDecode(payload, &ops); err != nil {
			return writeFrame(conn, kindError, []byte("decode batch: "+err.Error()))
		}
		status := replAckOK
		for _, op := range ops {
			if err := s.applyFn(op); err != nil {
				log.Printf("[repl] apply op key=%q: %v", op.Key, err)
				status = replAckErr
			}
		}
		return writeFrame(conn, kindBatch, []byte{status})

	case kindDigests:
		if s.digestFn == nil {
			return writeFrame(conn, kindError, []byte("anti-entropy not enabled"))
		}
		digests, err := s.digestFn()
		if err != nil {
			return writeFrame(conn, kindError, []byte(err.Error()))
		}
		out, err := gobEncode(digests)
		if err != nil {
			return writeFrame(conn, kindError, []byte("encode digests: "+err.Error()))
		}
		return writeFrame(conn, kindDigests, out)

	case kindFetch:
		if s.fetchFn == nil {
			return writeFrame(conn, kindError, []byte("anti-entropy not enabled"))
		}
		var shardID uint16
		if err := gobDecode(payload, &shardID); err != nil {
			return writeFrame(conn, kindError, []byte("decode shard id: "+err.Error()))
		}
		entries, err := s.fetchFn(shardID)
		if err != nil {
			return writeFrame(conn, kindError, []byte(err.Error()))
		}
		out, err := gobEncode(entries)
		if err != nil {
			return writeFrame(conn, kindError, []byte("encode entries: "+err.Error()))
		}
		return writeFrame(conn, kindFetch, out)

	default:
		return writeFrame(conn, kindError, []byte(fmt.Sprintf("unknown request kind 0x%02x", kind)))
	}
}

// ── Wire format ───────────────────────────────────────────────────────────────
//
// Every request and response is one frame:
//   [magic:4][kind:1][payloadLen:4][payload:payloadLen]   (all little-endian)

func gobEncode(v any) ([]byte, error) {
	var buf byteWriter
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.buf, nil
}

func gobDecode(data []byte, v any) error {
	return gob.NewDecoder(&byteReader{buf: data}).Decode(v)
}

func writeFrame(conn net.Conn, kind byte, payload []byte) error {
	if len(payload) > 64<<20 {
		return fmt.Errorf("frame payload too large: %d bytes", len(payload))
	}
	hdr := make([]byte, 9)
	binary.LittleEndian.PutUint32(hdr[0:4], replMagic)
	hdr[4] = kind
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(payload)))
	if _, err := conn.Write(hdr); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

func readFrame(conn net.Conn) (byte, []byte, error) {
	hdr := make([]byte, 9)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return 0, nil, err
	}
	if magic := binary.LittleEndian.Uint32(hdr[0:4]); magic != replMagic {
		return 0, nil, fmt.Errorf("bad magic 0x%08x", magic)
	}
	kind := hdr[4]
	n := binary.LittleEndian.Uint32(hdr[5:9])
	if n > 64<<20 {
		return 0, nil, fmt.Errorf("frame payload too large: %d bytes", n)
	}
	buf := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(conn, buf); err != nil {
			return 0, nil, fmt.Errorf("read payload: %w", err)
		}
	}
	return kind, buf, nil
}

// byteWriter is a minimal io.Writer accumulating into a slice (for gobEncode).
type byteWriter struct{ buf []byte }

func (w *byteWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// byteReader wraps a []byte as an io.Reader for gob.NewDecoder.
type byteReader struct {
	buf []byte
	off int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.off >= len(r.buf) {
		return 0, io.EOF
	}
	n := copy(p, r.buf[r.off:])
	r.off += n
	return n, nil
}
