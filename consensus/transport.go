package consensus

// transport.go — TCP-based Transport implementation for Raft RPCs.
//
// Each RPC uses a fresh TCP connection over which a single request+reply are
// gob-encoded.  This is intentionally simple: at Raft's RPC rate (one
// AppendEntries per peer per heartbeat_interval=50ms) connection overhead is
// negligible compared to the fd.sync latency in the storage engine.
//
// Wire framing:
//   [1B  msgType] [gob-encoded args] → [gob-encoded reply]
//
// msgType values:
//   0x01 = RequestVote
//   0x02 = AppendEntries
//   0x03 = InstallSnapshot
//
// The server side is RPCServer.  Create one per peer address and call
// ListenAndServe in a goroutine.  The RaftNode is supplied as the handler.
//
// # TLS
//
// Both sides optionally speak TLS 1.3 (stdlib crypto/tls): construct with
// NewTCPTransportTLS / NewRPCServerTLS and a TLSOptions.  When
// TLSOptions.Enabled is false (or the plain constructors are used) the wire
// stays plaintext — the default.  Setting CAFile enables mutual TLS: the
// server requires and verifies client certificates against the CA, and
// clients verify the server against the same CA.

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	msgRequestVote     byte = 0x01
	msgAppendEntries   byte = 0x02
	msgInstallSnapshot byte = 0x03
	rpcDialTimeout          = 2 * time.Second
	rpcCallTimeout          = 5 * time.Second
	// snapshotCallTimeout bounds InstallSnapshot RPCs, which ship the whole
	// state machine in one shot and can far exceed ordinary RPC sizes.
	snapshotCallTimeout = 60 * time.Second
)

// ── TLS options ───────────────────────────────────────────────────────────────

// TLSOptions configures optional transport encryption.  The zero value (and
// the plain constructors) mean plaintext.
type TLSOptions struct {
	// Enabled turns TLS on.  When false all other fields are ignored.
	Enabled bool
	// CertFile/KeyFile are this node's PEM certificate and private key.
	// Required on the server side; presented by clients for mutual TLS when set.
	CertFile string
	KeyFile  string
	// CAFile is a PEM CA bundle.  Clients use it to verify servers.  When set
	// on the server, client certificates are required and verified against it
	// (mutual TLS).
	CAFile string
	// ServerName overrides the hostname clients expect in the server
	// certificate.  Defaults to the host part of the dialled address.
	ServerName string
}

func (o TLSOptions) caPool() (*x509.CertPool, error) {
	pem, err := os.ReadFile(o.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates parsed from %s", o.CAFile)
	}
	return pool, nil
}

// clientTLSConfig builds the dial-side TLS config (TLS 1.3 minimum).
func (o TLSOptions) clientTLSConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: o.ServerName,
	}
	if o.CAFile != "" {
		pool, err := o.caPool()
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if o.CertFile != "" && o.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// serverTLSConfig builds the listen-side TLS config (TLS 1.3 minimum).
func (o TLSOptions) serverTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}
	if o.CAFile != "" {
		pool, err := o.caPool()
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert // mutual TLS
	}
	return cfg, nil
}

// nodeAddresses maps nodeID → "host:port".  Populated when TCPTransport is created.
type peerMap map[string]string

// TCPTransport sends Raft RPCs over persistent-ish TCP connections.
// Connections are established lazily and re-dialled on error.
type TCPTransport struct {
	mu     sync.Mutex
	addrs  peerMap            // nodeID → dial address
	conns  map[string]net.Conn // cached connections
	callMu map[string]*sync.Mutex // one mutex per peer — serialises concurrent RPCs
	tlsCfg *tls.Config // nil = plaintext
}

// NewTCPTransport creates a plaintext transport that dials the given peer
// addresses.
//   peers  — map[nodeID]"host:port"
func NewTCPTransport(peers map[string]string) *TCPTransport {
	callMu := make(map[string]*sync.Mutex, len(peers))
	for id := range peers {
		callMu[id] = &sync.Mutex{}
	}
	return &TCPTransport{
		addrs:  peers,
		conns:  make(map[string]net.Conn),
		callMu: callMu,
	}
}

// NewTCPTransportTLS is NewTCPTransport with optional TLS.  When opts.Enabled
// is false it behaves exactly like NewTCPTransport (plaintext).
func NewTCPTransportTLS(peers map[string]string, opts TLSOptions) (*TCPTransport, error) {
	t := NewTCPTransport(peers)
	if !opts.Enabled {
		return t, nil
	}
	cfg, err := opts.clientTLSConfig()
	if err != nil {
		return nil, err
	}
	t.tlsCfg = cfg
	return t, nil
}

// SendRequestVote dials peer, sends a RequestVote RPC, returns the reply.
func (t *TCPTransport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.call(peer, msgRequestVote, args, &reply)
	return reply, err
}

// SendAppendEntries dials peer, sends an AppendEntries RPC, returns the reply.
func (t *TCPTransport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.call(peer, msgAppendEntries, args, &reply)
	return reply, err
}

// SendInstallSnapshot dials peer, sends an InstallSnapshot RPC, returns the
// reply.  Uses a longer deadline than ordinary RPCs — the payload is the
// whole state machine.
func (t *TCPTransport) SendInstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	err := t.call(peer, msgInstallSnapshot, args, &reply)
	return reply, err
}

// AddPeer registers a new peer so the transport can dial it.
// Safe to call concurrently after construction.
func (t *TCPTransport) AddPeer(id, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addrs[id] = addr
	if _, ok := t.callMu[id]; !ok {
		t.callMu[id] = &sync.Mutex{}
	}
}

// Close closes all cached connections.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Close()
	}
	t.conns = make(map[string]net.Conn)
	return nil
}

// call sends one RPC to peer and decodes the reply.
// A per-peer mutex serialises concurrent callers so the shared connection is
// never written from two goroutines at the same time.
func (t *TCPTransport) call(peer string, msgType byte, args, reply interface{}) error {
	if mu, ok := t.callMu[peer]; ok {
		mu.Lock()
		defer mu.Unlock()
	}
	conn, err := t.getConn(peer)
	if err != nil {
		return err
	}

	timeout := rpcCallTimeout
	if msgType == msgInstallSnapshot {
		timeout = snapshotCallTimeout
	}
	conn.SetDeadline(time.Now().Add(timeout))

	// Write: [msgType][gob-encoded args].
	if _, err := conn.Write([]byte{msgType}); err != nil {
		t.evictConn(peer)
		return fmt.Errorf("write msgType: %w", err)
	}
	if err := gob.NewEncoder(conn).Encode(args); err != nil {
		t.evictConn(peer)
		return fmt.Errorf("encode args: %w", err)
	}

	// Read: [gob-encoded reply].
	if err := gob.NewDecoder(conn).Decode(reply); err != nil {
		t.evictConn(peer)
		return fmt.Errorf("decode reply: %w", err)
	}
	return nil
}

func (t *TCPTransport) getConn(peer string) (net.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return c, nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", peer)
	}
	var (
		c   net.Conn
		err error
	)
	if t.tlsCfg != nil {
		// DialWithDialer derives ServerName from addr when the config leaves
		// it empty, and runs the handshake within the dial timeout.
		c, err = tls.DialWithDialer(&net.Dialer{Timeout: rpcDialTimeout}, "tcp", addr, t.tlsCfg)
	} else {
		c, err = net.DialTimeout("tcp", addr, rpcDialTimeout)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	t.conns[peer] = c
	return c, nil
}

func (t *TCPTransport) evictConn(peer string) {
	t.mu.Lock()
	if c, ok := t.conns[peer]; ok {
		c.Close()
		delete(t.conns, peer)
	}
	t.mu.Unlock()
}

// ── RPCServer ─────────────────────────────────────────────────────────────────

// RPCHandler is implemented by *RaftNode.  The server calls these methods on
// incoming connections.
type RPCHandler interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
	HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply
}

// RPCServer listens for incoming Raft RPC connections and dispatches them to
// the given handler.  One server should run per Raft node.
type RPCServer struct {
	handler     RPCHandler
	listener    net.Listener
	done        chan struct{}
	wg          sync.WaitGroup
	connsMu     sync.Mutex
	activeConns map[net.Conn]struct{}
}

// NewRPCServer creates a plaintext RPCServer but does not start it.
func NewRPCServer(listenAddr string, handler RPCHandler) (*RPCServer, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", listenAddr, err)
	}
	return &RPCServer{
		handler:     handler,
		listener:    l,
		done:        make(chan struct{}),
		activeConns: make(map[net.Conn]struct{}),
	}, nil
}

// NewRPCServerTLS is NewRPCServer with optional TLS.  When opts.Enabled is
// false it behaves exactly like NewRPCServer (plaintext).
func NewRPCServerTLS(listenAddr string, handler RPCHandler, opts TLSOptions) (*RPCServer, error) {
	if !opts.Enabled {
		return NewRPCServer(listenAddr, handler)
	}
	cfg, err := opts.serverTLSConfig()
	if err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", listenAddr, err)
	}
	return &RPCServer{
		handler:     handler,
		listener:    tls.NewListener(l, cfg),
		done:        make(chan struct{}),
		activeConns: make(map[net.Conn]struct{}),
	}, nil
}

// ListenAndServe accepts connections until Stop is called.
func (s *RPCServer) ListenAndServe() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				log.Printf("[raft-rpc] accept error: %v", err)
				continue
			}
		}
		s.connsMu.Lock()
		s.activeConns[conn] = struct{}{}
		s.connsMu.Unlock()

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() {
				s.connsMu.Lock()
				delete(s.activeConns, c)
				s.connsMu.Unlock()
				c.Close()
			}()
			s.serveConn(c)
		}(conn)
	}
}

// Stop closes the listener, forcibly closes all active connections so
// in-flight serveConn goroutines exit promptly, then waits for them.
func (s *RPCServer) Stop() {
	close(s.done)
	s.listener.Close()
	s.connsMu.Lock()
	for c := range s.activeConns {
		c.Close()
	}
	s.connsMu.Unlock()
	s.wg.Wait()
}

// Addr returns the server's listen address string.
func (s *RPCServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *RPCServer) serveConn(conn net.Conn) {
	// One connection may carry multiple sequential RPCs.
	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		// Read message type.
		typeBuf := make([]byte, 1)
		if _, err := conn.Read(typeBuf); err != nil {
			return // EOF or timeout — connection closed by peer
		}
		msgType := typeBuf[0]

		dec := gob.NewDecoder(conn)
		enc := gob.NewEncoder(conn)

		switch msgType {
		case msgRequestVote:
			var args RequestVoteArgs
			if err := dec.Decode(&args); err != nil {
				return
			}
			reply := s.handler.HandleRequestVote(args)
			if err := enc.Encode(reply); err != nil {
				return
			}

		case msgAppendEntries:
			var args AppendEntriesArgs
			if err := dec.Decode(&args); err != nil {
				return
			}
			reply := s.handler.HandleAppendEntries(args)
			if err := enc.Encode(reply); err != nil {
				return
			}

		case msgInstallSnapshot:
			// Snapshot payloads can be large — extend the per-RPC deadline.
			conn.SetDeadline(time.Now().Add(snapshotCallTimeout))
			var args InstallSnapshotArgs
			if err := dec.Decode(&args); err != nil {
				return
			}
			reply := s.handler.HandleInstallSnapshot(args)
			if err := enc.Encode(reply); err != nil {
				return
			}

		default:
			log.Printf("[raft-rpc] unknown message type 0x%02x", msgType)
			return
		}
	}
}
