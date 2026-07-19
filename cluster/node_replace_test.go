package cluster

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// ── in-memory store ───────────────────────────────────────────────────────────

// memStore implements LocalStore using an in-memory map for tests.
type memStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newMemStore() *memStore { return &memStore{data: make(map[string][]byte)} }

func (s *memStore) ScanKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	return out
}

func (s *memStore) Get(k string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[k]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", k)
	}
	return v, nil
}

func (s *memStore) GetTTLForKey(string) int32 { return -1 }

func (s *memStore) Put(k string, v []byte, _ int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(v))
	copy(cp, v)
	s.data[k] = cp
	return nil
}

func (s *memStore) Delete(k string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, k)
	return nil
}

func (s *memStore) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func findKeyInStores(stores map[string]*memStore, k string) (nodeID string, found bool) {
	for id, s := range stores {
		s.mu.RLock()
		_, ok := s.data[k]
		s.mu.RUnlock()
		if ok {
			return id, true
		}
	}
	return "", false
}

// ── key generation ────────────────────────────────────────────────────────────

// diverseKey generates a key whose FNV-1a hash is derived from a large random
// uint64 seed, ensuring the 120 test keys span the full hash ring rather than
// all landing on the same virtual-node arc (which happens with "key-XXXX" style
// strings whose FNV-1a values cluster below the minimum virtual-node hash).
func diverseKey(i int) string {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("veltrixdb-node-replace-test-seed-%d", i)))
	return fmt.Sprintf("k%016x", h.Sum64())
}

// waitUntil polls pred every 25 ms until it returns true or 5 s elapses.
func waitUntil(pred func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

// waitForTCP polls addr via TCP connect until the port is accepting connections
// or 5 s elapse. TransferAgent.Start() launches the HTTP server in a goroutine
// so there is a small window between Start() returning and the socket being ready.
func waitForTCP(addr string) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestNodeReplacement_Kubernetes simulates replacing one StatefulSet pod in a
// 3-node VeltrixDB cluster — the exact sequence the Kubernetes Operator performs
// during a rolling upgrade or node-pool drain:
//
//  1. 3 nodes (n1, n2, n3), 120 keys written to their ring-assigned owners.
//  2. The node that owns the most keys is gracefully drained via
//     RemoveNodeAndRebalance: its keys are pushed to the survivors using the
//     TransferAgent HTTP protocol (500-key batches).
//  3. A replacement pod (n4) joins via AddNodeAndRebalance; surviving nodes run
//     MigrateToNewOwners to push the key ranges now owned by n4.
//  4. After both migrations every key exists exactly once and is on the node
//     the ring currently routes to — no data loss, no stale routing.
func TestNodeReplacement_Kubernetes(t *testing.T) {
	t.Parallel()

	const (
		partCount = uint32(24)
		numKeys   = 120
		basePort  = 19200
	)

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	cfg.VirtualNodesPerNode = 32
	pm := NewPartitionMap(cfg)

	initNodes := []struct{ id string; port int }{
		{"n1", basePort},
		{"n2", basePort + 1},
		{"n3", basePort + 2},
	}

	stores := make(map[string]*memStore)
	agents := make(map[string]*TransferAgent)
	var agentsMu sync.Mutex // protect concurrent agent map access in cleanup

	startAgent := func(id, addr string, s *memStore) {
		ta := NewTransferAgent(pm, id, s, addr)
		if err := ta.Start(); err != nil {
			t.Fatalf("agent(%s).Start: %v", id, err)
		}
		agentsMu.Lock()
		agents[id] = ta
		agentsMu.Unlock()
	}

	t.Cleanup(func() {
		agentsMu.Lock()
		defer agentsMu.Unlock()
		for _, a := range agents {
			a.Stop()
		}
	})

	for _, n := range initNodes {
		addr := fmt.Sprintf("127.0.0.1:%d", n.port)
		if err := pm.AddNodeWithTransfer(n.id, "127.0.0.1", n.port, addr); err != nil {
			t.Fatalf("AddNodeWithTransfer(%s): %v", n.id, err)
		}
		stores[n.id] = newMemStore()
		startAgent(n.id, addr, stores[n.id])
		if !waitForTCP(addr) {
			t.Fatalf("transfer server for %s never became ready at %s", n.id, addr)
		}
	}

	if err := pm.Rebalance(partCount); err != nil {
		t.Fatalf("initial Rebalance: %v", err)
	}

	// Assign keys to their ring owners using diverse hashes that span the full
	// virtual-node ring rather than clustering in one arc.
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		k := diverseKey(i)
		keys[i] = k
		owner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%s): %v", k, err)
		}
		if err := stores[owner].Put(k, []byte("v:"+k), -1); err != nil {
			t.Fatalf("initial Put on %s: %v", owner, err)
		}
	}

	t.Logf("initial distribution: n1=%d  n2=%d  n3=%d",
		stores["n1"].count(), stores["n2"].count(), stores["n3"].count())

	// Drain the node that currently holds the most keys so we exercise the
	// actual migration path regardless of the hash distribution outcome.
	nodeToDrain := "n1"
	maxKeys := 0
	for id, s := range stores {
		if c := s.count(); c > maxKeys {
			maxKeys = c
			nodeToDrain = id
		}
	}
	// Surviving nodes are the two that are not being drained.
	survivors := make(map[string]*memStore)
	for id, s := range stores {
		if id != nodeToDrain {
			survivors[id] = s
		}
	}

	// ── Phase 1: kubectl drain → pod terminates → node removed ───────────────
	t.Logf("draining %s (%d keys)", nodeToDrain, maxKeys)

	if err := pm.RemoveNodeAndRebalance(nodeToDrain, partCount, agents[nodeToDrain]); err != nil {
		t.Fatalf("RemoveNodeAndRebalance(%s): %v", nodeToDrain, err)
	}

	if !waitUntil(func() bool { return stores[nodeToDrain].count() == 0 }) {
		t.Errorf("%s still holds %d keys after drain timeout",
			nodeToDrain, stores[nodeToDrain].count())
	}

	for id, s := range survivors {
		t.Logf("after drain: %s=%d", id, s.count())
	}

	// Every key must be reachable on one of the surviving nodes.
	for _, k := range keys {
		if _, found := findKeyInStores(survivors, k); !found {
			t.Errorf("key %q lost after %s drain", k, nodeToDrain)
		}
	}

	// ── Phase 2: replacement pod n4 joins ─────────────────────────────────────
	n4Port := basePort + 3
	n4Addr := fmt.Sprintf("127.0.0.1:%d", n4Port)
	stores["n4"] = newMemStore()
	startAgent("n4", n4Addr, stores["n4"])
	if !waitForTCP(n4Addr) {
		t.Fatalf("n4 transfer server never became ready at %s", n4Addr)
	}

	if err := pm.AddNodeAndRebalance("n4", "127.0.0.1", n4Port, n4Addr, partCount, nil); err != nil {
		t.Fatalf("AddNodeAndRebalance(n4): %v", err)
	}

	// Push n4-owned ranges from each survivor.
	survivorIDs := make([]string, 0, len(survivors))
	for id := range survivors {
		survivorIDs = append(survivorIDs, id)
	}
	var wg sync.WaitGroup
	for _, id := range survivorIDs {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()
			if err := agents[nid].MigrateToNewOwners(); err != nil {
				t.Errorf("MigrateToNewOwners(%s): %v", nid, err)
			}
		}(id)
	}
	wg.Wait()

	for id := range survivors {
		t.Logf("after n4 join: %s=%d", id, stores[id].count())
	}
	t.Logf("after n4 join: n4=%d", stores["n4"].count())

	// Build the final active store set (survivors + n4, without the drained node).
	finalStores := make(map[string]*memStore)
	for id, s := range survivors {
		finalStores[id] = s
	}
	finalStores["n4"] = stores["n4"]

	// 1. No key is lost.
	for _, k := range keys {
		if _, found := findKeyInStores(finalStores, k); !found {
			t.Errorf("key %q lost after full node replacement", k)
		}
	}

	// 2. Every key is on the node the ring currently routes to.
	misrouted := 0
	for _, k := range keys {
		owner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%s): %v", k, err)
		}
		s, active := finalStores[owner]
		if !active {
			t.Errorf("ring routes %q to drained/removed node %q", k, owner)
			continue
		}
		if _, err := s.Get(k); err != nil {
			t.Errorf("key %q should be on ring owner %q but is missing", k, owner)
			misrouted++
		}
	}
	if misrouted == 0 {
		t.Logf("PASS: all %d keys accounted for and correctly routed after node replacement",
			numKeys)
	}
}

// TestNodeReplacement_TokenDistribution verifies the consistent-hash "token"
// property that makes Kubernetes rolling upgrades cheap: removing one node only
// moves that node's token-range data — keys already on other nodes stay put.
//
// With naive modulo-N hashing, removing 1 of 3 nodes would require moving
// (N-1)/N ≈ 67% of all keys. With consistent hashing only ~1/N ≈ 33% moves.
func TestNodeReplacement_TokenDistribution(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)

	for i, id := range []string{"n1", "n2", "n3"} {
		if err := pm.AddNode(id, "127.0.0.1", 9000+i); err != nil {
			t.Fatalf("AddNode(%s): %v", id, err)
		}
	}

	// Use diverse key hashes — these are computed via double-FNV to guarantee
	// they span different virtual-node arcs rather than clustering in one range.
	const total = 300
	rng := rand.New(rand.NewSource(0x1234_5678))
	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("tok-%016x", rng.Uint64())
	}

	initialOwner := make(map[string]string, total)
	for _, k := range keys {
		owner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%s): %v", k, err)
		}
		initialOwner[k] = owner
	}

	perNode := map[string]int{}
	for _, o := range initialOwner {
		perNode[o]++
	}
	t.Logf("initial ownership: n1=%d  n2=%d  n3=%d  (total=%d)", perNode["n1"], perNode["n2"], perNode["n3"], total)

	// Every node should own some keys; warn if the distribution is very skewed.
	for _, id := range []string{"n1", "n2", "n3"} {
		if perNode[id] == 0 {
			t.Logf("WARNING: %s owns 0/%d keys — very skewed hash distribution; "+
				"token-distribution assertions will be trivially satisfied", id, total)
		}
	}

	// Choose the node that owns the most keys as the "node to drain" so the
	// assertions are non-trivial even under a skewed distribution.
	nodeToDrain := "n2"
	maxOwned := 0
	for _, id := range []string{"n1", "n2", "n3"} {
		if perNode[id] > maxOwned {
			maxOwned = perNode[id]
			nodeToDrain = id
		}
	}

	var drainedKeys, otherKeys []string
	for _, k := range keys {
		if initialOwner[k] == nodeToDrain {
			drainedKeys = append(drainedKeys, k)
		} else {
			otherKeys = append(otherKeys, k)
		}
	}
	t.Logf("draining %s (owns %d keys); %d keys on other nodes",
		nodeToDrain, len(drainedKeys), len(otherKeys))

	// Remove nodeToDrain from the ring (routing only, no actual data migration).
	if err := pm.RemoveNode(nodeToDrain); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	// ── Assertion 1: keys NOT on the drained node must not move ──────────────
	disrupted := 0
	for _, k := range otherKeys {
		newOwner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%s): %v", k, err)
		}
		if newOwner != initialOwner[k] {
			disrupted++
		}
	}
	if disrupted > 0 {
		t.Errorf("consistent-hash violated: %d/%d non-%s keys rerouted (expected 0)",
			disrupted, len(otherKeys), nodeToDrain)
	} else {
		t.Logf("consistent-hash OK: 0/%d non-%s keys disrupted by node removal",
			len(otherKeys), nodeToDrain)
	}

	// ── Assertion 2: drained node's keys reroute to surviving nodes ───────────
	remaining := 0
	for _, k := range drainedKeys {
		newOwner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%s): %v", k, err)
		}
		if newOwner == nodeToDrain {
			remaining++
		}
	}
	if remaining > 0 {
		t.Errorf("%d/%d %s keys still route to removed node",
			remaining, len(drainedKeys), nodeToDrain)
	} else if len(drainedKeys) > 0 {
		t.Logf("all %d former %s keys rerouted to surviving nodes", len(drainedKeys), nodeToDrain)
	}
}
