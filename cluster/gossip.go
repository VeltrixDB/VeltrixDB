package cluster

// gossip.go — real TCP gossip transport (stdlib only).
//
// Each node runs a gossip listener (Node.GossipAddr). Every gossip interval a
// node advances its own heartbeat counter and dials `fanout` random peers
// (rand.Shuffle over the membership, minus self). One gossip round is a single
// short-lived TCP connection:
//
//   1. Dialer sends its GossipDigest as one JSON object (json.Encoder).
//   2. Listener merges the digest, records a heartbeat for the sender, and
//      replies with its own GossipDigest.
//   3. Dialer merges the reply. The connection closes.
//
// The digest carries the sender's ID, fencing epoch, partition-map version and
// a per-node view: state, monotonic heartbeat counter, and addresses. A
// receiver records a heartbeat with the FailureDetector for any node whose
// counter advanced beyond what it had already seen — so liveness propagates
// transitively (A learns C is alive from B) without every node dialing every
// other node each round.
//
// Split-brain fencing: a digest with a stale epoch (see epoch.go) still counts
// as liveness evidence for its sender, but its membership metadata (gossip
// addresses) is ignored; a digest with a newer epoch advances the local epoch.
//
// Wire format: JSON — versioned by field name, so unknown fields from newer
// nodes are ignored and old/new nodes interoperate during rolling upgrades.

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// GossipDigest is the wire message exchanged in one gossip round.
type GossipDigest struct {
	SenderID   string                 `json:"sender"`
	Epoch      uint64                 `json:"epoch"`       // fencing epoch (epoch.go)
	MapVersion uint64                 `json:"map_version"` // partition map version
	Timestamp  int64                  `json:"ts"`          // sender wall clock, unix ns
	Nodes      map[string]DigestEntry `json:"nodes"`       // sender's membership view
}

// DigestEntry is one node's state within a gossip digest.
type DigestEntry struct {
	State      string `json:"state"`
	Heartbeat  uint64 `json:"hb"` // monotonic heartbeat/incarnation counter
	GossipAddr string `json:"gossip_addr,omitempty"`
	Address    string `json:"addr,omitempty"`
	Port       int    `json:"port,omitempty"`
	Rack       string `json:"rack,omitempty"` // failure domain (--rack-id); adopted like GossipAddr
}

// GossipTransportConfig configures the TCP gossip transport.
type GossipTransportConfig struct {
	ListenAddr     string        // gossip listener bind address, e.g. "127.0.0.1:7946" or ":0"; "" disables the listener
	DialTimeout    time.Duration // per-round dial + exchange deadline (default 2s)
	GossipInterval time.Duration // how often to gossip (default 1s)
	Fanout         int           // peers contacted per round (default 3)
}

// DefaultGossipTransportConfig returns sensible defaults (listener disabled
// until ListenAddr is set).
func DefaultGossipTransportConfig() *GossipTransportConfig {
	return &GossipTransportConfig{
		DialTimeout:    2 * time.Second,
		GossipInterval: 1 * time.Second,
		Fanout:         3,
	}
}

// GossipProtocol implements epidemic gossip/rumor spreading over TCP.
type GossipProtocol struct {
	mu              sync.RWMutex
	nodeID          string
	partitionMap    *PartitionMap
	failureDetector *FailureDetector
	gossipInterval  time.Duration
	dialTimeout     time.Duration
	fanout          int // Number of nodes to send gossip to
	listenAddr      string
	listener        net.Listener
	done            chan struct{}
	closeOnce       sync.Once
	heartbeat       atomic.Uint64     // this node's own heartbeat counter; +1 per gossip round
	seenCounters    map[string]uint64 // nodeID → highest heartbeat counter observed (guarded by mu)
}

// NewGossipProtocol creates a gossip protocol without a listener. Outbound
// gossip only reaches peers whose GossipAddr is known; with no listener this
// node cannot receive digests, so use NewGossipProtocolWithTransport for a
// full mesh member. Kept for bootstrap paths that wire the transport later.
func NewGossipProtocol(nodeID string, pm *PartitionMap, fd *FailureDetector) *GossipProtocol {
	return NewGossipProtocolWithTransport(nodeID, pm, fd, DefaultGossipTransportConfig())
}

// NewGossipProtocolWithTransport creates a gossip protocol with a TCP
// transport configured by cfg. Call StartListener (or Start, which starts the
// listener implicitly when ListenAddr is set) to begin serving.
func NewGossipProtocolWithTransport(nodeID string, pm *PartitionMap, fd *FailureDetector, cfg *GossipTransportConfig) *GossipProtocol {
	if cfg == nil {
		cfg = DefaultGossipTransportConfig()
	}
	interval := cfg.GossipInterval
	if interval <= 0 {
		interval = 1 * time.Second
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 2 * time.Second
	}
	fanout := cfg.Fanout
	if fanout <= 0 {
		fanout = 3
	}
	return &GossipProtocol{
		nodeID:          nodeID,
		partitionMap:    pm,
		failureDetector: fd,
		gossipInterval:  interval,
		dialTimeout:     dialTimeout,
		fanout:          fanout,
		listenAddr:      cfg.ListenAddr,
		done:            make(chan struct{}),
		seenCounters:    make(map[string]uint64),
	}
}

// StartListener binds the gossip listener and serves inbound gossip rounds in
// a background goroutine. It returns the bound address (useful with ":0") and
// registers it as this node's GossipAddr in the partition map. Idempotent:
// a second call returns the already-bound address.
func (gp *GossipProtocol) StartListener() (string, error) {
	gp.mu.Lock()
	if gp.listener != nil {
		addr := gp.listener.Addr().String()
		gp.mu.Unlock()
		return addr, nil
	}
	if gp.listenAddr == "" {
		gp.mu.Unlock()
		return "", fmt.Errorf("gossip: no ListenAddr configured")
	}
	ln, err := net.Listen("tcp", gp.listenAddr)
	if err != nil {
		gp.mu.Unlock()
		return "", fmt.Errorf("gossip listen %s: %w", gp.listenAddr, err)
	}
	gp.listener = ln
	gp.mu.Unlock()

	addr := ln.Addr().String()
	// Advertise the bound address so peers (and our own digests) know it.
	_ = gp.partitionMap.SetNodeGossipAddr(gp.nodeID, addr)

	go gp.acceptLoop(ln)
	return addr, nil
}

// acceptLoop serves inbound gossip connections until the listener closes.
func (gp *GossipProtocol) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-gp.done:
				return
			default:
			}
			log.Printf("[gossip] accept: %v", err)
			return
		}
		go gp.handleGossipConn(conn)
	}
}

// handleGossipConn performs the listener side of one gossip round: read the
// remote digest, merge it, reply with the local digest.
func (gp *GossipProtocol) handleGossipConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(gp.dialTimeout))

	var remote GossipDigest
	if err := json.NewDecoder(conn).Decode(&remote); err != nil {
		return // malformed or timed-out peer; drop silently
	}
	gp.mergeDigest(&remote)

	local := gp.buildDigest()
	_ = json.NewEncoder(conn).Encode(local)
}

// Start begins the gossip loop (and the listener, when configured).
func (gp *GossipProtocol) Start() {
	if gp.listenAddr != "" {
		if _, err := gp.StartListener(); err != nil {
			log.Printf("[gossip] listener: %v", err)
		}
	}
	go gp.backgroundGossiper()
}

// backgroundGossiper periodically sends gossip messages
func (gp *GossipProtocol) backgroundGossiper() {
	ticker := time.NewTicker(gp.gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gp.done:
			return
		case <-ticker.C:
			gp.sendGossip()
		}
	}
}

// sendGossip advances the local heartbeat counter and gossips with up to
// `fanout` peers chosen uniformly at random (rand.Shuffle).
func (gp *GossipProtocol) sendGossip() {
	gp.heartbeat.Add(1) // one incarnation tick per round

	type peerRef struct{ id, addr string }

	// Snapshot peer IDs + gossip addresses under the map lock; GossipAddr may
	// be updated concurrently by digest merges, so never read it off *Node
	// outside pm.mu.
	gp.partitionMap.mu.RLock()
	peers := make([]peerRef, 0, len(gp.partitionMap.Nodes))
	for _, node := range gp.partitionMap.Nodes {
		if node.ID != gp.nodeID {
			peers = append(peers, peerRef{id: node.ID, addr: node.GossipAddr})
		}
	}
	gp.partitionMap.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })

	for i := 0; i < gp.fanout && i < len(peers); i++ {
		peer := peers[i]
		go gp.sendGossipToPeer(peer.id, peer.addr)
	}
}

// sendGossipToPeer performs the dialer side of one gossip round with a single
// peer: dial its gossip listener, send our digest, merge its reply. A peer
// with no known GossipAddr is skipped (its liveness can still reach us
// transitively through other peers' digests).
func (gp *GossipProtocol) sendGossipToPeer(peerID, gossipAddr string) {
	if gossipAddr == "" {
		return
	}

	conn, err := net.DialTimeout("tcp", gossipAddr, gp.dialTimeout)
	if err != nil {
		return // peer unreachable — no heartbeat recorded; FD thresholds take over
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(gp.dialTimeout))

	if err := json.NewEncoder(conn).Encode(gp.buildDigest()); err != nil {
		return
	}

	var reply GossipDigest
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		return
	}
	if reply.SenderID != peerID {
		// The address answered as a different node (e.g. reused port); merge
		// anyway — the digest is self-describing.
		log.Printf("[gossip] peer at %s identified as %q (expected %q)", gossipAddr, reply.SenderID, peerID)
	}
	gp.mergeDigest(&reply)
}

// buildDigest snapshots this node's membership view for the wire.
func (gp *GossipProtocol) buildDigest() *GossipDigest {
	ownHB := gp.heartbeat.Load()

	gp.mu.RLock()
	counters := make(map[string]uint64, len(gp.seenCounters))
	for id, c := range gp.seenCounters {
		counters[id] = c
	}
	gp.mu.RUnlock()

	gp.partitionMap.mu.RLock()
	nodes := make(map[string]DigestEntry, len(gp.partitionMap.Nodes))
	for nodeID, node := range gp.partitionMap.Nodes {
		hb := counters[nodeID]
		if nodeID == gp.nodeID {
			hb = ownHB
		}
		nodes[nodeID] = DigestEntry{
			State:      node.State.Load().(NodeState).String(),
			Heartbeat:  hb,
			GossipAddr: node.GossipAddr,
			Address:    node.Address,
			Port:       node.Port,
			Rack:       node.Rack,
		}
	}
	epoch := gp.partitionMap.epoch
	version := gp.partitionMap.Version
	gp.partitionMap.mu.RUnlock()

	return &GossipDigest{
		SenderID:   gp.nodeID,
		Epoch:      epoch,
		MapVersion: version,
		Timestamp:  time.Now().UnixNano(),
		Nodes:      nodes,
	}
}

// mergeDigest folds a remote digest into local state:
//   - the sender itself gets a heartbeat (we just heard from it over TCP);
//   - any node whose heartbeat counter advanced beyond our recorded value
//     gets a heartbeat (transitive liveness);
//   - unknown-to-us nodes are ignored (membership changes travel through the
//     fenced AddNode/RemoveNode paths, not gossip);
//   - gossip addresses are adopted (self-reported entries win; third-party
//     entries only fill blanks) unless the digest's epoch is stale;
//   - a newer remote epoch advances ours.
//
// Lock order is fd.mu → pm.mu everywhere in this package, so heartbeats are
// recorded after all pm.mu work here is done.
func (gp *GossipProtocol) mergeDigest(remote *GossipDigest) {
	if remote == nil || remote.SenderID == gp.nodeID {
		return
	}

	localEpoch := gp.partitionMap.Epoch()
	staleEpoch := remote.Epoch < localEpoch
	if remote.Epoch > localEpoch {
		gp.partitionMap.AdvanceEpochTo(remote.Epoch)
	}

	// Pass 1 — counters: find nodes whose heartbeat counter advanced.
	advanced := make([]string, 0, len(remote.Nodes))
	gp.mu.Lock()
	for nodeID, entry := range remote.Nodes {
		if nodeID == gp.nodeID {
			continue // never heartbeat ourselves from a remote view
		}
		if entry.Heartbeat > gp.seenCounters[nodeID] {
			gp.seenCounters[nodeID] = entry.Heartbeat
			advanced = append(advanced, nodeID)
		}
	}
	gp.mu.Unlock()

	// Pass 2 — membership metadata (skipped entirely on a stale epoch: a
	// fenced-off node must not rewrite addresses in the newer topology).
	known := make(map[string]bool, len(remote.Nodes)+1)
	gp.partitionMap.mu.Lock()
	for nodeID, entry := range remote.Nodes {
		node, exists := gp.partitionMap.Nodes[nodeID]
		if !exists {
			continue
		}
		known[nodeID] = true
		if staleEpoch || nodeID == gp.nodeID {
			continue
		}
		if entry.GossipAddr != "" {
			if nodeID == remote.SenderID {
				node.GossipAddr = entry.GossipAddr // self-reported: authoritative (handles restarts on a new port)
			} else if node.GossipAddr == "" {
				node.GossipAddr = entry.GossipAddr // third-party: fill blanks only
			}
		}
		if entry.Rack != "" {
			if nodeID == remote.SenderID {
				node.Rack = entry.Rack // self-reported: authoritative (handles rack moves)
			} else if node.Rack == "" {
				node.Rack = entry.Rack // third-party: fill blanks only
			}
		}
	}
	_, senderKnown := gp.partitionMap.Nodes[remote.SenderID]
	gp.partitionMap.mu.Unlock()

	// Pass 3 — heartbeats (fd.mu → pm.mu), only for nodes in our membership.
	if gp.failureDetector == nil {
		return
	}
	if senderKnown && remote.SenderID != gp.nodeID {
		gp.failureDetector.RecordHeartbeat(remote.SenderID)
	}
	for _, nodeID := range advanced {
		if nodeID != remote.SenderID && known[nodeID] {
			gp.failureDetector.RecordHeartbeat(nodeID)
		}
	}
}

// Close stops the gossip protocol and its listener. Idempotent.
func (gp *GossipProtocol) Close() error {
	gp.closeOnce.Do(func() {
		close(gp.done)
		gp.mu.Lock()
		ln := gp.listener
		gp.listener = nil
		gp.mu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
	})
	return nil
}
