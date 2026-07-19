package cluster

import (
	"net"
	"testing"
	"time"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// gossipTestNode bundles the per-node cluster components of one in-process
// gossip instance.
type gossipTestNode struct {
	id   string
	pm   *PartitionMap
	fd   *FailureDetector
	gp   *GossipProtocol
	addr string // bound gossip listener address
}

// gossipFDConfig returns failure-detector timings sized for gossip tests:
// gossip every 25 ms, suspect after 150 ms of silence, fail after 400 ms.
func gossipFDConfig() *FailureDetectorConfig {
	return &FailureDetectorConfig{
		HeartbeatInterval:  20 * time.Millisecond,
		SuspectThreshold:   150 * time.Millisecond,
		FailureThreshold:   400 * time.Millisecond,
		MaxSuspectTime:     2 * time.Second,
		RecoveryInterval:   50 * time.Millisecond,
		MaxRecoveryRetries: 1,
		PingTimeout:        200 * time.Millisecond,
	}
}

func gossipTransportConfig() *GossipTransportConfig {
	return &GossipTransportConfig{
		ListenAddr:     "127.0.0.1:0",
		DialTimeout:    500 * time.Millisecond,
		GossipInterval: 25 * time.Millisecond,
		Fanout:         2,
	}
}

// closedPort returns a loopback TCP port that was just bound and released, so
// it is almost certainly not accepting connections.
func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// startGossipCluster spins up one full gossip instance (partition map, failure
// detector, gossip listener + gossiper) per id, on real loopback sockets, with
// every instance knowing every other instance's gossip address.
func startGossipCluster(t *testing.T, ids []string) map[string]*gossipTestNode {
	t.Helper()

	nodes := make(map[string]*gossipTestNode, len(ids))
	deadPort := closedPort(t) // Node.Port for all test nodes: never answers pings

	// Phase 1: create each instance and bind its gossip listener.
	for _, id := range ids {
		cfg := DefaultClusterConfig()
		cfg.ReplicationFactor = 1
		cfg.PartitionCount = 16
		pm := NewPartitionMap(cfg)
		if err := pm.AddNode(id, "127.0.0.1", deadPort); err != nil {
			t.Fatalf("AddNode(self=%s): %v", id, err)
		}

		fd := NewFailureDetector(pm, gossipFDConfig())
		fd.SetLocalNode(id)

		gp := NewGossipProtocolWithTransport(id, pm, fd, gossipTransportConfig())
		addr, err := gp.StartListener()
		if err != nil {
			t.Fatalf("StartListener(%s): %v", id, err)
		}

		nodes[id] = &gossipTestNode{id: id, pm: pm, fd: fd, gp: gp, addr: addr}
	}

	// Phase 2: register every peer (with its real gossip address) in every
	// instance's partition map. All AddNode calls happen on every map so all
	// instances share the same epoch.
	for _, n := range nodes {
		for _, peer := range nodes {
			if peer.id == n.id {
				continue
			}
			if err := n.pm.AddNode(peer.id, "127.0.0.1", deadPort); err != nil {
				t.Fatalf("AddNode(%s on %s): %v", peer.id, n.id, err)
			}
			if err := n.pm.SetNodeGossipAddr(peer.id, peer.addr); err != nil {
				t.Fatalf("SetNodeGossipAddr(%s on %s): %v", peer.id, n.id, err)
			}
		}
	}

	// Phase 3: start detectors and gossipers.
	for _, n := range nodes {
		n.fd.Start()
		n.gp.Start()
		fd, gp := n.fd, n.gp
		t.Cleanup(func() {
			gp.Close()
			fd.Close()
		})
	}
	return nodes
}

// ── Gossip transport tests ────────────────────────────────────────────────────

// TestGossip_HeartbeatsPropagate: three instances gossiping over loopback TCP.
// After well over the failure threshold, every instance still sees every peer
// as Active — possible only if real heartbeats are flowing over the sockets
// (the old implementation faked this without touching the network).
func TestGossip_HeartbeatsPropagate(t *testing.T) {
	t.Parallel()

	ids := []string{"gossip-a", "gossip-b", "gossip-c"}
	nodes := startGossipCluster(t, ids)

	// 600 ms > FailureThreshold (400 ms): silence would mean FAILED by now.
	time.Sleep(600 * time.Millisecond)

	for _, n := range nodes {
		for _, peerID := range ids {
			if peerID == n.id {
				continue
			}
			s, ok := nodeState(n.pm, peerID)
			if !ok {
				t.Fatalf("%s: peer %s missing from partition map", n.id, peerID)
			}
			if s != NodeStateActive {
				t.Errorf("%s sees %s as %s; want ACTIVE (heartbeats not propagating)", n.id, peerID, s)
			}
		}
		if failed := n.fd.GetFailureStats().FailedCount; failed != 0 {
			t.Errorf("%s: %d nodes marked failed in a healthy cluster", n.id, failed)
		}
	}
}

// TestGossip_FailureAndRecovery: killing one instance's listener + gossiper
// drives it SUSPECT then FAILED on the survivors within the configured
// thresholds; restarting it (new listener, new port) recovers it.
func TestGossip_FailureAndRecovery(t *testing.T) {
	t.Parallel()

	ids := []string{"gfr-a", "gfr-b", "gfr-c"}
	nodes := startGossipCluster(t, ids)
	a, c := nodes["gfr-a"], nodes["gfr-c"]

	// Let the cluster get healthy first.
	time.Sleep(200 * time.Millisecond)

	// Kill c: listener stops accepting, gossiper stops dialing out.
	c.gp.Close()
	c.fd.Close()

	// a must see c pass through SUSPECT and reach FAILED within thresholds
	// (suspect at 150 ms, failed at 400 ms; allow generous slack for -race).
	sawSuspect := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s, _ := nodeState(a.pm, c.id)
		if s == NodeStateSuspect {
			sawSuspect = true
		}
		if s == NodeStateFailed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s, _ := nodeState(a.pm, c.id); s != NodeStateFailed {
		t.Fatalf("a sees dead node c as %s; want FAILED", s)
	}
	if !sawSuspect {
		t.Errorf("c never observed in SUSPECT state before FAILED")
	}

	// Restart c with a fresh failure detector and a fresh gossip listener on a
	// NEW port. c still knows a and b, dials them, and its self-reported digest
	// entry carries the new gossip address — the survivors recover it.
	fd2 := NewFailureDetector(c.pm, gossipFDConfig())
	fd2.SetLocalNode(c.id)
	fd2.Start()
	gp2 := NewGossipProtocolWithTransport(c.id, c.pm, fd2, gossipTransportConfig())
	if _, err := gp2.StartListener(); err != nil {
		t.Fatalf("restart listener: %v", err)
	}
	gp2.Start()
	t.Cleanup(func() {
		gp2.Close()
		fd2.Close()
	})

	recovered := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s, _ := nodeState(a.pm, c.id)
		if s == NodeStateRecovering || s == NodeStateActive {
			recovered = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !recovered {
		s, _ := nodeState(a.pm, c.id)
		t.Errorf("a sees restarted node c as %s; want RECOVERING or ACTIVE", s)
	}
}

// ── pingNode tests ────────────────────────────────────────────────────────────

// TestFD_PingNode_LiveVsClosedPort: pingNode must succeed against a live TCP
// health endpoint (dial + PING/PONG exchange) and fail against a closed port.
func TestFD_PingNode_LiveVsClosedPort(t *testing.T) {
	t.Parallel()

	pm := newTestPM(t, "ping-n1")
	cfg := fastFDConfig()
	cfg.PingTimeout = 300 * time.Millisecond
	fd := NewFailureDetector(pm, cfg)

	// Live endpoint: accepts, reads the probe, answers PONG.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 16)
				_, _ = c.Read(buf)
				_, _ = c.Write([]byte("PONG\n"))
			}(conn)
		}
	}()
	livePort := ln.Addr().(*net.TCPAddr).Port

	if !fd.pingNode("127.0.0.1", livePort) {
		t.Errorf("pingNode against live health endpoint = false; want true")
	}
	if fd.pingNode("127.0.0.1", closedPort(t)) {
		t.Errorf("pingNode against closed port = true; want false")
	}
	// A listener that accepts but never responds is NOT healthy — the health
	// exchange requires a response byte.
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer silent.Close()
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			_ = conn // hold open, never write
		}
	}()
	if fd.pingNode("127.0.0.1", silent.Addr().(*net.TCPAddr).Port) {
		t.Errorf("pingNode against silent listener = true; want false (no health exchange)")
	}
}

// ── triggerRebalance regression test ──────────────────────────────────────────

// TestFD_TriggerRebalance_UsesConfiguredPartitionCount: regression for the bug
// where triggerRebalance passed the PartitionMigrations METRIC (a migration
// counter) to Rebalance as the partition COUNT — rebalancing to 0 partitions
// on a fresh cluster, or to whatever the counter happened to read.
func TestFD_TriggerRebalance_UsesConfiguredPartitionCount(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	cfg.PartitionCount = 32
	pm := NewPartitionMap(cfg)
	for i, id := range []string{"trb-1", "trb-2", "trb-3"} {
		if err := pm.AddNode(id, "127.0.0.1", 9000+i); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	// Poison the migration counter: the buggy code would Rebalance(7).
	pm.GetMetrics().PartitionMigrations.Add(7)

	fd := NewFailureDetector(pm, fastFDConfig())
	fd.triggerRebalance()

	pm.mu.RLock()
	got := len(pm.Partitions)
	pm.mu.RUnlock()
	if got != 32 {
		t.Fatalf("triggerRebalance produced %d partitions; want configured count 32", got)
	}

	// An explicit Rebalance updates the configured count; automatic rebalances
	// must follow it.
	if err := pm.Rebalance(48); err != nil {
		t.Fatalf("Rebalance(48): %v", err)
	}
	if pc := pm.PartitionCount(); pc != 48 {
		t.Fatalf("PartitionCount() = %d after Rebalance(48); want 48", pc)
	}
	fd.triggerRebalance()
	pm.mu.RLock()
	got = len(pm.Partitions)
	pm.mu.RUnlock()
	if got != 48 {
		t.Errorf("triggerRebalance after Rebalance(48) produced %d partitions; want 48", got)
	}
}
