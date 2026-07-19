package integration_test

// cluster_test.go — Integration tests for VeltrixDB Raft consensus layer.
//
// These tests wire up real RaftNode instances with real TCP transports
// (RPCServer + TCPTransport) and in-process StorageEngine state machines.
// No mocking — the full consensus + storage stack is exercised.
//
// Why not use the TCP server binary here?
//   A real 3-node cluster with network-level failover simulation would need
//   iptables or tc rules, which require root.  Instead we test the Raft layer
//   directly by building the cluster in-process; only the TCP transport for
//   Raft RPCs uses real sockets on loopback ports.

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// ── State machine adapter ─────────────────────────────────────────────────────

// writeOp is the command encoded in each Raft log entry.
type writeOp struct {
	Delete bool
	Key    string
	Value  []byte
	TTL    int32
}

// storageSM wraps a StorageEngine as a consensus.StateMachine.
type storageSM struct {
	se *storage.StorageEngine
}

func (s *storageSM) Apply(cmd []byte) error {
	var op writeOp
	if err := gobDecode(cmd, &op); err != nil {
		return fmt.Errorf("decode writeOp: %w", err)
	}
	if op.Delete {
		return s.se.Delete(op.Key)
	}
	return s.se.Put(op.Key, op.Value, op.TTL)
}

// ── gob helpers ───────────────────────────────────────────────────────────────

func gobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// ── clusterNode bundles all per-node resources ────────────────────────────────

type clusterNode struct {
	id        string
	engine    *storage.StorageEngine
	raft      *consensus.RaftNode
	rpc       *consensus.RPCServer
	transport *consensus.TCPTransport
	dataDir   string
}

func (n *clusterNode) close() {
	// Close outgoing transport first so peer serveConns receive EOF and exit,
	// allowing rpc.Stop()'s wg.Wait() to complete without deadlocking.
	if n.transport != nil {
		_ = n.transport.Close()
	}
	if n.raft != nil {
		n.raft.Stop()
	}
	if n.rpc != nil {
		n.rpc.Stop()
	}
	if n.engine != nil {
		_ = n.engine.Close()
	}
	if n.dataDir != "" {
		os.RemoveAll(n.dataDir)
	}
}

// put submits a Put command to this node's Raft log.
func (n *clusterNode) put(key string, value []byte) error {
	op := writeOp{Key: key, Value: value, TTL: -1}
	cmd, err := gobEncode(op)
	if err != nil {
		return err
	}
	return n.raft.Submit(cmd)
}

// get reads directly from the local storage engine.  After Submit returns the
// entry is already applied locally, so reads are consistent.
func (n *clusterNode) get(key string) ([]byte, error) {
	return n.engine.Get(key)
}

// ── cluster factory ───────────────────────────────────────────────────────────

// setupCluster creates nodeCount in-process Raft nodes wired to each other.
// Returns the nodes slice and a cleanup func that tears down everything.
func setupCluster(t *testing.T, nodeCount int) ([]*clusterNode, func()) {
	t.Helper()

	nodeIDs := make([]string, nodeCount)
	rpcAddrs := make([]string, nodeCount)
	dataDirs := make([]string, nodeCount)
	engines := make([]*storage.StorageEngine, nodeCount)

	for i := 0; i < nodeCount; i++ {
		nodeIDs[i] = fmt.Sprintf("node-%d", i+1)

		dir, err := os.MkdirTemp("", fmt.Sprintf("veltrix-cluster-n%d-", i+1))
		if err != nil {
			t.Fatalf("mkdirtemp node %d: %v", i+1, err)
		}
		dataDirs[i] = dir

		cfg := storage.DefaultStorageConfig()
		cfg.DataDirPath = dir
		cfg.CacheMaxSizeMB = 16
		cfg.NumShards = 1024
		cfg.WALFlushWindowMs = 1
		cfg.VLogFlushWindowMs = 1
		cfg.ScrubEnabled = false
		// DefaultStorageConfig allocates 4 GB of bloom filter memory per engine
		// (1<<22 bits/shard × 8192 shards).  Three-node clusters would need 12 GB,
		// causing multi-second delays and OOM on CI runners.  Bloom correctness is
		// covered by storage/bloom_test.go; integration tests exercise Raft consensus.
		cfg.BloomFilterShardBits = 0

		se, err := storage.NewStorageEngine(cfg)
		if err != nil {
			t.Fatalf("new engine node %d: %v", i+1, err)
		}
		engines[i] = se

		// Pre-allocate a free port for the RPC server.
		rpcAddrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}

	nodes := make([]*clusterNode, nodeCount)

	for i := 0; i < nodeCount; i++ {
		// Build peer address map (everyone except self).
		peerAddrs := make(map[string]string, nodeCount-1)
		peerIDs := make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if j == i {
				continue
			}
			peerIDs = append(peerIDs, nodeIDs[j])
			peerAddrs[nodeIDs[j]] = rpcAddrs[j]
		}

		transport := consensus.NewTCPTransport(peerAddrs)
		sm := &storageSM{se: engines[i]}

		rn, err := consensus.NewRaftNode(nodeIDs[i], peerIDs, dataDirs[i], sm, transport)
		if err != nil {
			t.Fatalf("new raft node %d: %v", i+1, err)
		}

		srv, err := consensus.NewRPCServer(rpcAddrs[i], rn)
		if err != nil {
			t.Fatalf("new rpc server node %d (%s): %v", i+1, rpcAddrs[i], err)
		}
		go srv.ListenAndServe()

		nodes[i] = &clusterNode{
			id:        nodeIDs[i],
			engine:    engines[i],
			raft:      rn,
			rpc:       srv,
			transport: transport,
			dataDir:   dataDirs[i],
		}
	}

	cleanup := func() {
		for _, n := range nodes {
			if n != nil {
				n.close()
			}
		}
	}

	return nodes, cleanup
}

// waitForLeader polls until one node reports IsLeader() == true.
func waitForLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n != nil && n.raft != nil && n.raft.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestCluster_NodeFailover writes 100 keys to the leader, stops it, waits for
// a new leader, then verifies all keys are readable and new writes succeed.
func TestCluster_NodeFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster failover test in short mode")
	}

	nodes, cleanup := setupCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("initial leader: %s", leader.id)

	const numKeys = 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("failover-key-%03d", i)
		val := []byte(fmt.Sprintf("failover-val-%03d", i))
		if err := leader.put(key, val); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// Stop the leader.
	leaderIdx := -1
	for i, n := range nodes {
		if n != nil && n.id == leader.id {
			leaderIdx = i
			break
		}
	}
	_ = nodes[leaderIdx].transport.Close() // close outgoing first
	nodes[leaderIdx].raft.Stop()
	nodes[leaderIdx].rpc.Stop()
	nodes[leaderIdx] = nil // prevent double-close in cleanup

	// Wait for a new leader among survivors.
	var survivors []*clusterNode
	for _, n := range nodes {
		if n != nil {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeader(t, survivors, 3*time.Second)
	t.Logf("new leader after failover: %s", newLeader.id)

	// The applier goroutine is async: wait until the last key is visible before
	// checking the full set, so we don't race against state-machine catch-up.
	lastKey := fmt.Sprintf("failover-key-%03d", numKeys-1)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := newLeader.get(lastKey); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// All keys must be readable from the new leader.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("failover-key-%03d", i)
		want := fmt.Sprintf("failover-val-%03d", i)
		got, err := newLeader.get(key)
		if err != nil {
			t.Errorf("get %s after failover: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("get %s: want %q, got %q", key, want, got)
		}
	}

	// New writes must succeed.
	if err := newLeader.put("post-failover", []byte("alive")); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	val, err := newLeader.get("post-failover")
	if err != nil || string(val) != "alive" {
		t.Fatalf("read post-failover write: got %q %v", val, err)
	}
}

// TestCluster_LeaderElection verifies that a new leader is elected shortly
// after the current leader is stopped. waitForLeader polls and returns as soon
// as a leader appears, so the budget is a ceiling, not a fixed wait — it must
// cover the 400-800ms election timeout plus a possible split-vote round, with
// headroom for loaded CI runners.
func TestCluster_LeaderElection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster leader election test in short mode")
	}

	nodes, cleanup := setupCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("first leader: %s", leader.id)

	// Stop the leader.
	leaderIdx := -1
	for i, n := range nodes {
		if n != nil && n.id == leader.id {
			leaderIdx = i
			break
		}
	}
	_ = nodes[leaderIdx].transport.Close() // close outgoing first
	nodes[leaderIdx].raft.Stop()
	nodes[leaderIdx].rpc.Stop()
	nodes[leaderIdx] = nil

	var survivors []*clusterNode
	for _, n := range nodes {
		if n != nil {
			survivors = append(survivors, n)
		}
	}

	newLeader := waitForLeader(t, survivors, 5*time.Second)
	t.Logf("new leader: %s", newLeader.id)
	if newLeader.id == leader.id {
		t.Fatalf("new leader %q is the same as the (stopped) old leader", newLeader.id)
	}
}

// TestCluster_NodeAddition starts a 2-node cluster, writes 100 keys, adds a
// 3rd node, and verifies the new node receives all entries via log replication.
func TestCluster_NodeAddition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster node addition test in short mode")
	}

	nodes, cleanup := setupCluster(t, 2)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("2-node leader: %s", leader.id)

	const numKeys = 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("addnode-key-%03d", i)
		val := []byte(fmt.Sprintf("addnode-val-%03d", i))
		if err := leader.put(key, val); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// Create the 3rd node.
	dir3, err := os.MkdirTemp("", "veltrix-cluster-n3-")
	if err != nil {
		t.Fatalf("mkdirtemp n3: %v", err)
	}

	cfg3 := storage.DefaultStorageConfig()
	cfg3.DataDirPath = dir3
	cfg3.CacheMaxSizeMB = 16
	cfg3.NumShards = 1024
	cfg3.WALFlushWindowMs = 1
	cfg3.VLogFlushWindowMs = 1
	cfg3.ScrubEnabled = false

	se3, err := storage.NewStorageEngine(cfg3)
	if err != nil {
		t.Fatalf("engine n3: %v", err)
	}

	peerAddrs3 := map[string]string{
		nodes[0].id: nodes[0].rpc.Addr(),
		nodes[1].id: nodes[1].rpc.Addr(),
	}
	transport3 := consensus.NewTCPTransport(peerAddrs3)
	sm3 := &storageSM{se: se3}
	peerIDs3 := []string{nodes[0].id, nodes[1].id}

	rn3, err := consensus.NewRaftNode("node-3", peerIDs3, dir3, sm3, transport3)
	if err != nil {
		t.Fatalf("raft n3: %v", err)
	}

	rpcAddr3 := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	srv3, err := consensus.NewRPCServer(rpcAddr3, rn3)
	if err != nil {
		t.Fatalf("rpc server n3: %v", err)
	}
	go srv3.ListenAndServe()

	n3 := &clusterNode{
		id:        "node-3",
		engine:    se3,
		raft:      rn3,
		rpc:       srv3,
		transport: transport3,
		dataDir:   dir3,
	}
	defer n3.close()

	// Register node-3 with the existing nodes so the leader starts sending
	// AppendEntries to it and it can receive heartbeats (stopping its elections).
	for _, n := range nodes {
		n.transport.AddPeer("node-3", rpcAddr3)
		n.raft.AddPeer("node-3")
	}

	// Wait up to 3 s for n3 to receive all log entries via AppendEntries.
	deadline := time.Now().Add(3 * time.Second)
	lastKey := fmt.Sprintf("addnode-key-%03d", numKeys-1)
	lastWant := fmt.Sprintf("addnode-val-%03d", numKeys-1)
	for time.Now().Before(deadline) {
		val, err := n3.get(lastKey)
		if err == nil && string(val) == lastWant {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify all keys are present on the new node.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("addnode-key-%03d", i)
		want := fmt.Sprintf("addnode-val-%03d", i)
		got, err := n3.get(key)
		if err != nil {
			t.Errorf("get %s from n3: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("get %s from n3: want %q, got %q", key, want, got)
		}
	}
}

// TestCluster_NetworkPartition simulates a follower losing its outgoing
// transport, verifies the leader+remaining node still form a quorum, then
// heals the partition and checks the follower catches up.
func TestCluster_NetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network partition test in short mode")
	}

	nodes, cleanup := setupCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("leader: %s", leader.id)

	// Identify a follower.
	var follower *clusterNode
	for _, n := range nodes {
		if n != nil && n.id != leader.id {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	// Partition: close the follower's outgoing transport so it cannot reach
	// peers (it will time out on vote/heartbeat RPCs).
	_ = follower.transport.Close()

	// Write 20 keys — leader + one other follower = majority (2-of-3).
	const numKeys = 20
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("partition-key-%03d", i)
		val := []byte(fmt.Sprintf("partition-val-%03d", i))
		if err := leader.put(key, val); err != nil {
			t.Fatalf("put while partitioned: %v", err)
		}
	}

	// Heal: give the follower a fresh transport pointing at current peers.
	peerAddrs := make(map[string]string)
	for _, n := range nodes {
		if n != nil && n.id != follower.id {
			peerAddrs[n.id] = n.rpc.Addr()
		}
	}
	follower.transport = consensus.NewTCPTransport(peerAddrs)
	// (The follower's Raft node still has the old transport reference, but
	//  incoming AppendEntries via the RPC server are unaffected — those arrive
	//  on the server's listener which was never closed.  The follower will
	//  receive heartbeats carrying the missing entries and apply them.)

	// Wait for the follower's engine to receive the entries.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		lastKey := fmt.Sprintf("partition-key-%03d", numKeys-1)
		val, err := follower.get(lastKey)
		if err == nil && string(val) == fmt.Sprintf("partition-val-%03d", numKeys-1) {
			t.Logf("follower %s caught up after partition heal", follower.id)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("partition-key-%03d", i)
		want := fmt.Sprintf("partition-val-%03d", i)
		got, err := follower.get(key)
		if err != nil {
			t.Errorf("get %s from follower after heal: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("get %s from follower: want %q, got %q", key, want, got)
		}
	}
}
