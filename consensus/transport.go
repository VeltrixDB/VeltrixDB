package consensus

// transport.go — TCP-based Transport implementation for Raft RPCs.
//
// # Stream mode (default between nodes of this version)
//
// One persistent connection per peer carries a multiplexed stream: the client
// opens it with the byte msgStreamHello, the server answers msgStreamHello,
// and from then on each side writes gob-encoded envelopes on ONE long-lived
// gob encoder (type information is sent once per connection, not per RPC):
//
//	client → server  rpcRequest  {ID, Type, AE | RV | IS args}
//	server → client  rpcResponse {ID, Type, AE | RV | IS reply}
//
// Requests are written in call order and the server reads and dispatches them
// in that order — an AppendEntries is appended (in memory, and queued for the
// log file) before the next request is read — but replies are matched by ID
// and may come back out of order: a follower answers a heartbeat at once
// while an earlier append still waits for its fsync.  This is what lets the
// leader keep several AppendEntries in flight per follower
// (SendAppendEntriesAsync, pipeline.go).  A request that is not answered
// within its timeout fails and the connection is closed (every other pending
// request on it fails too, and the next call re-dials).
//
// # Legacy mode
//
// A server of an older build does not know msgStreamHello and closes the
// connection; the client then falls back, for that peer, to the old
// one-RPC-per-round-trip framing below and retries the stream after
// legacyRetryAfter.  The server keeps serving legacy clients:
//
//	[1B msgType] [gob-encoded args] → [gob-encoded reply]
//
// msgType values:
//
//	0x01 = RequestVote
//	0x02 = AppendEntries
//	0x03 = InstallSnapshot
//	0x10 = stream hello (stream mode)
//
// The server side is RPCServer.  Create one per node and call ListenAndServe
// in a goroutine.  The RaftNode is supplied as the handler.
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
	"bufio"
	"crypto/tls"
	"crypto/x509"
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
	msgRequestVote     byte = 0x01
	msgAppendEntries   byte = 0x02
	msgInstallSnapshot byte = 0x03
	msgStreamHello     byte = 0x10
	rpcDialTimeout          = 2 * time.Second
	rpcCallTimeout          = 5 * time.Second
	// snapshotCallTimeout bounds InstallSnapshot RPCs, which ship the whole
	// state machine in one shot and can far exceed ordinary RPC sizes.
	snapshotCallTimeout = 60 * time.Second
	// legacyRetryAfter: how long a peer that refused the stream hello is
	// served in legacy mode before the stream is tried again.
	legacyRetryAfter = 30 * time.Second
	// streamIdleTimeout closes a server-side stream with no request for this
	// long (a leader heartbeats every 50 ms).
	streamIdleTimeout = 60 * time.Second
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

// TCPTransport sends Raft RPCs to peers: a multiplexed stream per peer
// (falling back to legacy one-at-a-time RPCs against older servers).
// It implements AsyncTransport.  Safe for concurrent use.
type TCPTransport struct {
	mu      sync.Mutex
	addrs   peerMap                // nodeID → dial address
	streams map[string]*streamConn // live stream per peer
	dialMu  map[string]*sync.Mutex // one dial at a time per peer
	legacy  map[string]time.Time   // peer → stream retry time (legacy until then)
	closed  bool
	tlsCfg  *tls.Config // nil = plaintext

	// Legacy mode state.
	conns  map[string]net.Conn    // cached legacy connections
	callMu map[string]*sync.Mutex // one mutex per peer — serialises legacy RPCs
}

// NewTCPTransport creates a plaintext transport that dials the given peer
// addresses.
//
//	peers  — map[nodeID]"host:port"
func NewTCPTransport(peers map[string]string) *TCPTransport {
	t := &TCPTransport{
		addrs:   make(peerMap, len(peers)),
		streams: make(map[string]*streamConn),
		dialMu:  make(map[string]*sync.Mutex),
		legacy:  make(map[string]time.Time),
		conns:   make(map[string]net.Conn),
		callMu:  make(map[string]*sync.Mutex),
	}
	for id, addr := range peers {
		t.addPeerLocked(id, addr)
	}
	return t
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

func (t *TCPTransport) addPeerLocked(id, addr string) {
	t.addrs[id] = addr
	if _, ok := t.callMu[id]; !ok {
		t.callMu[id] = &sync.Mutex{}
	}
	if _, ok := t.dialMu[id]; !ok {
		t.dialMu[id] = &sync.Mutex{}
	}
}

// AddPeer registers a new peer so the transport can dial it.
// Safe to call concurrently after construction.
func (t *TCPTransport) AddPeer(id, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addPeerLocked(id, addr)
}

// Close closes all connections; pending requests fail and later calls return
// an error.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	streams := make([]*streamConn, 0, len(t.streams))
	for _, sc := range t.streams {
		streams = append(streams, sc)
	}
	t.streams = make(map[string]*streamConn)
	for _, c := range t.conns {
		c.Close()
	}
	t.conns = make(map[string]net.Conn)
	t.mu.Unlock()
	for _, sc := range streams {
		sc.fail(errTransportClosed)
	}
	return nil
}

var errTransportClosed = fmt.Errorf("raft transport closed")

// SendRequestVote sends a RequestVote RPC and returns the reply.
func (t *TCPTransport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	resp, err := t.roundTrip(peer, rpcRequest{Type: msgRequestVote, RV: &args}, rpcCallTimeout, &reply)
	if err == nil && resp.RV != nil {
		reply = *resp.RV
	}
	return reply, err
}

// SendAppendEntries sends an AppendEntries RPC and returns the reply.
func (t *TCPTransport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	resp, err := t.roundTrip(peer, rpcRequest{Type: msgAppendEntries, AE: &args}, rpcCallTimeout, &reply)
	if err == nil && resp.AE != nil {
		reply = *resp.AE
	}
	return reply, err
}

// SendInstallSnapshot sends an InstallSnapshot RPC and returns the reply.
// Uses a longer deadline than ordinary RPCs — the payload is the whole state
// machine.
func (t *TCPTransport) SendInstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	resp, err := t.roundTrip(peer, rpcRequest{Type: msgInstallSnapshot, IS: &args}, snapshotCallTimeout, &reply)
	if err == nil && resp.IS != nil {
		reply = *resp.IS
	}
	return reply, err
}

// SendAppendEntriesAsync queues an AppendEntries for peer (requests to one
// peer are written in call order) and calls done with the reply or an error.
// In legacy mode it runs the RPC synchronously before returning.
func (t *TCPTransport) SendAppendEntriesAsync(peer string, args AppendEntriesArgs, done func(AppendEntriesReply, error)) {
	sc, err := t.stream(peer)
	if err != nil {
		done(AppendEntriesReply{}, err)
		return
	}
	if sc == nil { // legacy peer
		var reply AppendEntriesReply
		err := t.call(peer, msgAppendEntries, args, &reply)
		done(reply, err)
		return
	}
	sc.send(rpcRequest{Type: msgAppendEntries, AE: &args}, rpcCallTimeout, func(resp rpcResponse, err error) {
		var reply AppendEntriesReply
		if err == nil && resp.AE != nil {
			reply = *resp.AE
		}
		done(reply, err)
	})
}

// roundTrip sends req and waits for its reply (stream), or runs a legacy RPC
// decoding into legacyReply.
func (t *TCPTransport) roundTrip(peer string, req rpcRequest, timeout time.Duration, legacyReply interface{}) (rpcResponse, error) {
	sc, err := t.stream(peer)
	if err != nil {
		return rpcResponse{}, err
	}
	if sc == nil {
		var args interface{}
		switch req.Type {
		case msgRequestVote:
			args = *req.RV
		case msgAppendEntries:
			args = *req.AE
		default:
			args = *req.IS
		}
		return rpcResponse{}, t.call(peer, req.Type, args, legacyReply)
	}
	type result struct {
		resp rpcResponse
		err  error
	}
	ch := make(chan result, 1)
	sc.send(req, timeout, func(resp rpcResponse, err error) { ch <- result{resp, err} })
	r := <-ch
	return r.resp, r.err
}

// stream returns the live stream to peer, dialling one if needed.  It
// returns (nil, nil) when the peer is in legacy mode.
func (t *TCPTransport) stream(peer string) (*streamConn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errTransportClosed
	}
	if sc := t.streams[peer]; sc != nil {
		t.mu.Unlock()
		return sc, nil
	}
	if until, ok := t.legacy[peer]; ok && time.Now().Before(until) {
		t.mu.Unlock()
		return nil, nil
	}
	addr, ok := t.addrs[peer]
	dmu := t.dialMu[peer]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", peer)
	}

	dmu.Lock()
	defer dmu.Unlock()
	t.mu.Lock()
	if sc := t.streams[peer]; sc != nil { // dialled while we waited
		t.mu.Unlock()
		return sc, nil
	}
	t.mu.Unlock()

	c, err := t.dial(addr)
	if err != nil {
		return nil, err
	}
	// Handshake: hello → hello.  An older server closes the connection.
	c.SetDeadline(time.Now().Add(rpcDialTimeout))
	var ack [1]byte
	if _, err = c.Write([]byte{msgStreamHello}); err == nil {
		_, err = io.ReadFull(c, ack[:])
	}
	if err != nil || ack[0] != msgStreamHello {
		c.Close()
		t.mu.Lock()
		t.legacy[peer] = time.Now().Add(legacyRetryAfter)
		t.mu.Unlock()
		log.Printf("[raft-rpc] peer %s (%s) did not accept a stream (%v) — legacy RPCs for %v",
			peer, addr, err, legacyRetryAfter)
		return nil, nil
	}
	c.SetDeadline(time.Time{})
	sc := newStreamConn(t, peer, c)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		sc.fail(errTransportClosed)
		return nil, errTransportClosed
	}
	delete(t.legacy, peer)
	t.streams[peer] = sc
	t.mu.Unlock()
	go sc.readLoop()
	return sc, nil
}

func (t *TCPTransport) dial(addr string) (net.Conn, error) {
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
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return c, nil
}

// ── stream connection (client side) ──────────────────────────────────────────

// rpcRequest / rpcResponse are the stream-mode envelopes.  Exactly one of the
// pointer fields is set, as named by Type (a nil reply pointer decodes as the
// zero reply).
type rpcRequest struct {
	ID   uint64
	Type byte
	AE   *AppendEntriesArgs
	RV   *RequestVoteArgs
	IS   *InstallSnapshotArgs
}

type rpcResponse struct {
	ID   uint64
	Type byte
	AE   *AppendEntriesReply
	RV   *RequestVoteReply
	IS   *InstallSnapshotReply
}

type pendingCall struct {
	done  func(rpcResponse, error)
	timer *time.Timer
}

// streamConn is one multiplexed client connection.
type streamConn struct {
	t    *TCPTransport
	peer string
	c    net.Conn

	wmu sync.Mutex // serialises request writes (call order = wire order)
	bw  *bufio.Writer
	enc *gob.Encoder

	pmu     sync.Mutex
	pending map[uint64]*pendingCall
	nextID  uint64
	err     error // set once the connection failed
}

func newStreamConn(t *TCPTransport, peer string, c net.Conn) *streamConn {
	bw := bufio.NewWriterSize(c, 64<<10)
	return &streamConn{
		t: t, peer: peer, c: c,
		bw: bw, enc: gob.NewEncoder(bw),
		pending: make(map[uint64]*pendingCall),
	}
}

// send writes req and arranges for done to be called once with its reply,
// an error, or a timeout.  done never runs under the stream's locks.
func (sc *streamConn) send(req rpcRequest, timeout time.Duration, done func(rpcResponse, error)) {
	// Hold wmu across ID assignment and the write so IDs and wire order agree
	// with call order.
	sc.wmu.Lock()
	sc.pmu.Lock()
	if sc.err != nil {
		err := sc.err
		sc.pmu.Unlock()
		sc.wmu.Unlock()
		done(rpcResponse{}, err)
		return
	}
	sc.nextID++
	id := sc.nextID
	pc := &pendingCall{done: done}
	sc.pending[id] = pc
	pc.timer = time.AfterFunc(timeout, func() {
		sc.fail(fmt.Errorf("raft rpc to %s timed out after %v", sc.peer, timeout))
	})
	sc.pmu.Unlock()

	req.ID = id
	sc.c.SetWriteDeadline(time.Now().Add(timeout))
	err := sc.enc.Encode(&req)
	if err == nil {
		err = sc.bw.Flush()
	}
	sc.wmu.Unlock()
	if err != nil {
		sc.fail(fmt.Errorf("write to %s: %w", sc.peer, err))
	}
}

// readLoop dispatches replies to their pending calls until the connection
// fails.
func (sc *streamConn) readLoop() {
	dec := gob.NewDecoder(bufio.NewReaderSize(sc.c, 64<<10))
	for {
		var resp rpcResponse
		if err := dec.Decode(&resp); err != nil {
			sc.fail(fmt.Errorf("read from %s: %w", sc.peer, err))
			return
		}
		sc.pmu.Lock()
		pc := sc.pending[resp.ID]
		delete(sc.pending, resp.ID)
		sc.pmu.Unlock()
		if pc != nil {
			pc.timer.Stop()
			pc.done(resp, nil)
		}
	}
}

// fail closes the connection once and fails every pending call with err.
func (sc *streamConn) fail(err error) {
	sc.pmu.Lock()
	if sc.err != nil {
		sc.pmu.Unlock()
		return
	}
	sc.err = err
	pend := sc.pending
	sc.pending = nil
	sc.pmu.Unlock()

	sc.c.Close()
	sc.t.mu.Lock()
	if sc.t.streams[sc.peer] == sc {
		delete(sc.t.streams, sc.peer)
	}
	sc.t.mu.Unlock()
	for _, pc := range pend {
		pc.timer.Stop()
		pc.done(rpcResponse{}, err)
	}
}

// ── legacy mode (client side) ────────────────────────────────────────────────

// call sends one legacy RPC to peer and decodes the reply.
// A per-peer mutex serialises concurrent callers so the shared connection is
// never written from two goroutines at the same time.
func (t *TCPTransport) call(peer string, msgType byte, args, reply interface{}) error {
	t.mu.Lock()
	mu := t.callMu[peer]
	t.mu.Unlock()
	if mu != nil {
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
	if t.closed {
		t.mu.Unlock()
		return nil, errTransportClosed
	}
	if c, ok := t.conns[peer]; ok {
		t.mu.Unlock()
		return c, nil
	}
	addr, ok := t.addrs[peer]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", peer)
	}
	c, err := t.dial(addr)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conns[peer] = c
	t.mu.Unlock()
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
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, typeBuf); err != nil {
		return
	}
	if typeBuf[0] == msgStreamHello {
		s.serveStream(conn)
		return
	}
	s.serveLegacy(conn, typeBuf[0])
}

// serveLegacy handles the pre-stream framing: one RPC per round trip.
func (s *RPCServer) serveLegacy(conn net.Conn, msgType byte) {
	typeBuf := make([]byte, 1)
	for first := true; ; first = false {
		if !first {
			conn.SetDeadline(time.Now().Add(30 * time.Second))
			if _, err := conn.Read(typeBuf); err != nil {
				return // EOF or timeout — connection closed by peer
			}
			msgType = typeBuf[0]
		}

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

// serveStream handles a multiplexed stream.  Requests are read and dispatched
// in order on this goroutine; AppendEntries replies may be produced later
// (after the follower's fsync) by another goroutine, so replies go through a
// queue drained by a writer goroutine.
func (s *RPCServer) serveStream(conn net.Conn) {
	conn.SetDeadline(time.Time{})
	conn.SetWriteDeadline(time.Now().Add(rpcDialTimeout))
	if _, err := conn.Write([]byte{msgStreamHello}); err != nil {
		return
	}
	w := newReplyWriter(conn)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		w.run()
	}()
	defer w.close()

	async, _ := s.handler.(AsyncAppendHandler)
	dec := gob.NewDecoder(bufio.NewReaderSize(conn, 64<<10))
	for {
		conn.SetReadDeadline(time.Now().Add(streamIdleTimeout))
		var req rpcRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		id := req.ID
		switch req.Type {
		case msgAppendEntries:
			var args AppendEntriesArgs
			if req.AE != nil {
				args = *req.AE
			}
			respond := func(r AppendEntriesReply) {
				w.put(rpcResponse{ID: id, Type: msgAppendEntries, AE: &r})
			}
			if async != nil {
				async.HandleAppendEntriesAsync(args, respond)
			} else {
				respond(s.handler.HandleAppendEntries(args))
			}
		case msgRequestVote:
			var args RequestVoteArgs
			if req.RV != nil {
				args = *req.RV
			}
			r := s.handler.HandleRequestVote(args)
			w.put(rpcResponse{ID: id, Type: msgRequestVote, RV: &r})
		case msgInstallSnapshot:
			var args InstallSnapshotArgs
			if req.IS != nil {
				args = *req.IS
			}
			r := s.handler.HandleInstallSnapshot(args)
			w.put(rpcResponse{ID: id, Type: msgInstallSnapshot, IS: &r})
		default:
			log.Printf("[raft-rpc] unknown stream message type 0x%02x", req.Type)
			return
		}
	}
}

// replyWriter serialises stream replies onto the connection from any
// goroutine without blocking the caller (the follower's syncer answers
// AppendEntries through it).
type replyWriter struct {
	conn   net.Conn
	mu     sync.Mutex
	queue  []rpcResponse
	closed bool
	wake   chan struct{}
}

func newReplyWriter(conn net.Conn) *replyWriter {
	return &replyWriter{conn: conn, wake: make(chan struct{}, 1)}
}

func (w *replyWriter) put(r rpcResponse) {
	w.mu.Lock()
	if !w.closed {
		w.queue = append(w.queue, r)
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *replyWriter) close() {
	w.mu.Lock()
	w.closed = true
	w.queue = nil
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *replyWriter) run() {
	bw := bufio.NewWriterSize(w.conn, 64<<10)
	enc := gob.NewEncoder(bw)
	for range w.wake {
		w.mu.Lock()
		batch, closed := w.queue, w.closed
		w.queue = nil
		w.mu.Unlock()
		if closed {
			return
		}
		w.conn.SetWriteDeadline(time.Now().Add(rpcCallTimeout))
		for i := range batch {
			if err := enc.Encode(&batch[i]); err != nil {
				w.conn.Close()
				return
			}
		}
		if err := bw.Flush(); err != nil {
			w.conn.Close()
			return
		}
	}
}
