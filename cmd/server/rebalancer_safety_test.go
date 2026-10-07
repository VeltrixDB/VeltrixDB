package main

// rebalancer_safety_test.go — node failure must never make the auto-
// rebalancer delete data (CLAUDE.md invariant 62). Before the fix, a FAILED
// event made every node run MigrateToNewOwners, which sent each key to its
// single ring owner and deleted it locally: a 3-node raft cluster lost about
// two thirds of its keys on each survivor when the leader was killed, and a
// replicated RF=3 cluster did the same although every node is a replica of
// every key.

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// freeClientPort returns a client port P with P+offRaft bindable, so the
// derived raft listener of buildRaftCoordinator can start.
func freeClientPort(t *testing.T, used map[int]bool) int {
	t.Helper()
	for tries := 0; tries < 200; tries++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		l.Close()
		if used[p] || used[p+offRaft] {
			continue
		}
		l2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+offRaft))
		if err != nil {
			continue
		}
		l2.Close()
		used[p], used[p+offRaft] = true, true
		return p
	}
	t.Fatal("no free client/raft port pair")
	return 0
}

// rebalanceNode is one node's partition map + transfer agent wired with the
// production setupRebalancer.
type rebalanceNode struct {
	id   string
	eng  *storage.StorageEngine
	pm   *cluster.PartitionMap
	ta   *cluster.TransferAgent
	stop func()
}

func startRebalanceNodes(t *testing.T, deploy deployMode, ids []string, engs []*storage.StorageEngine) []*rebalanceNode {
	t.Helper()
	nodes := make([]*rebalanceNode, len(ids))
	for i, id := range ids {
		pm := cluster.NewPartitionMap(cluster.DefaultClusterConfig()) // RF=3
		for j, other := range ids {
			if err := pm.AddNode(other, "127.0.0.1", 7800+j); err != nil {
				t.Fatal(err)
			}
		}
		if err := pm.Rebalance(pm.PartitionCount()); err != nil {
			t.Fatal(err)
		}
		ta := cluster.NewTransferAgent(pm, id, engs[i], "127.0.0.1:0")
		if err := ta.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ta.Stop)
		pm.SetNodeTransferAddr(id, ta.BoundAddr())
		nodes[i] = &rebalanceNode{id: id, eng: engs[i], pm: pm, ta: ta}
	}
	for _, n := range nodes {
		n.stop = setupRebalancer(deploy, n.pm, n.ta, n.id, true) // --auto-rebalance=true (the default)
		t.Cleanup(n.stop)
	}
	return nodes
}

// failNode marks id FAILED on every other node's map (what the failure
// detector does when its heartbeats stop) and stops its transfer listener.
func failNode(t *testing.T, nodes []*rebalanceNode, id string) {
	t.Helper()
	for _, n := range nodes {
		if n.id == id {
			n.ta.Stop()
			continue
		}
		if err := n.pm.UpdateNodeState(id, cluster.NodeStateSuspect); err != nil {
			t.Fatal(err)
		}
		if err := n.pm.UpdateNodeState(id, cluster.NodeStateFailed); err != nil {
			t.Fatal(err)
		}
	}
}

func missingKeys(eng *storage.StorageEngine, keys []string) []string {
	var miss []string
	for _, k := range keys {
		if _, err := eng.Get(k); err != nil {
			miss = append(miss, k)
		}
	}
	return miss
}

// (a) 3-node raft cluster, --auto-rebalance=true: load keys through the
// leader, kill the leader, let the rebalancer see the FAILED event → every
// survivor still holds every key and the new leader serves them all.
func TestRebalancer_RaftLeaderFailureKeepsAllKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process raft cluster")
	}
	ids := []string{"rbs-raft-1", "rbs-raft-2", "rbs-raft-3"}
	used := map[int]bool{}
	ports := []int{freeClientPort(t, used), freeClientPort(t, used), freeClientPort(t, used)}
	var peerAll []peerSpec
	for i, id := range ids {
		peerAll = append(peerAll, peerSpec{id: id, clientAddr: fmt.Sprintf("127.0.0.1:%d", ports[i]), host: "127.0.0.1", clientPort: ports[i]})
	}

	saved := pCurrentEngine
	t.Cleanup(func() { pCurrentEngine = saved })
	engs := make([]*storage.StorageEngine, 3)
	coords := make([]*coordinator, 3)
	cleanups := make([]func(), 3)
	for i, id := range ids {
		engs[i] = newFSMTestEngine(t)
		pCurrentEngine = engs[i]
		var peers []peerSpec
		for _, p := range peerAll {
			if p.id != id {
				peers = append(peers, p)
			}
		}
		c, cleanup, _, err := buildCoordinator(clusterParams{
			mode: modeRaft, nodeID: id, clientAddr: peerAll[i].clientAddr,
			dataDir: t.TempDir(), peers: peers, replFactor: 3,
		})
		if err != nil {
			t.Fatal(err)
		}
		coords[i], cleanups[i] = c, cleanup
	}
	stopped := make([]bool, 3)
	t.Cleanup(func() {
		for i, c := range cleanups {
			if !stopped[i] {
				c()
			}
		}
	})

	leader := func(skip int) int {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			for i, c := range coords {
				if i != skip && c.raft.IsLeader() {
					return i
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("no raft leader elected")
		return -1
	}
	li := leader(-1)

	const n = 600
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("raftreb:%04d", i)
		if err := coords[li].Put(keys[i], []byte("v"+keys[i]), -1); err != nil {
			t.Fatalf("put via leader: %v", err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		done := true
		for _, e := range engs {
			if len(missingKeys(e, keys)) != 0 {
				done = false
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("raft log did not apply on every node")
		}
		time.Sleep(50 * time.Millisecond)
	}

	nodes := startRebalanceNodes(t, modeRaft, ids, engs)

	// Kill the leader.
	cleanups[li]()
	stopped[li] = true
	failNode(t, nodes, ids[li])

	// The rebalancer would have fired after its debounce; also try the
	// migration directly, as an embedding program might.
	migErrs := map[string]error{}
	for i, nd := range nodes {
		if i != li {
			migErrs[nd.id] = nd.ta.MigrateToNewOwners()
		}
	}
	time.Sleep(rebalanceDebounce + 2*time.Second)

	for i, e := range engs {
		if i == li {
			continue
		}
		if miss := missingKeys(e, keys); len(miss) != 0 {
			t.Fatalf("survivor %s lost %d/%d keys after leader failure (e.g. %q)", ids[i], len(miss), n, miss[0])
		}
	}
	for id, err := range migErrs {
		if !errors.Is(err, cluster.ErrMigrationRaftMode) {
			t.Fatalf("MigrateToNewOwners on raft node %s: err=%v, want ErrMigrationRaftMode", id, err)
		}
	}
	nl := leader(li)
	for _, k := range keys {
		v, err := coords[nl].Get(k)
		if err != nil || string(v) != "v"+k {
			t.Fatalf("new leader %s: GET %s = %q, %v", ids[nl], k, v, err)
		}
	}
}

// (b) Replicated mode, 3 nodes, RF=3: every node is a replica of every key.
// A node failure (and its recovery) makes the auto-rebalancer move and
// delete nothing.
func TestRebalancer_ReplicatedRF3FailureNoDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-engine integration test")
	}
	ids := []string{"rbs-repl-1", "rbs-repl-2", "rbs-repl-3"}
	engs := []*storage.StorageEngine{newFSMTestEngine(t), newFSMTestEngine(t), newFSMTestEngine(t)}
	const n = 600
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("replreb:%04d", i)
		for _, e := range engs { // what a replicated write leaves behind
			if err := e.Put(keys[i], []byte("v"), -1); err != nil {
				t.Fatal(err)
			}
		}
	}
	nodes := startRebalanceNodes(t, modeReplicated, ids, engs)
	before := nodes[0].pm.GetMetrics().PartitionMigrations.Load() + nodes[1].pm.GetMetrics().PartitionMigrations.Load()

	failNode(t, nodes, ids[2])
	time.Sleep(rebalanceDebounce + 2*time.Second)
	for i := 0; i < 2; i++ {
		if miss := missingKeys(engs[i], keys); len(miss) != 0 {
			t.Fatalf("%s lost %d/%d keys after %s failed (e.g. %q)", ids[i], len(miss), n, ids[2], miss[0])
		}
	}
	// Recovery (RECOVERING → ACTIVE) moves nothing either.
	for i := 0; i < 2; i++ {
		_ = nodes[i].pm.UpdateNodeState(ids[2], cluster.NodeStateRecovering)
		_ = nodes[i].pm.UpdateNodeState(ids[2], cluster.NodeStateActive)
	}
	time.Sleep(rebalanceDebounce + 2*time.Second)
	for i, e := range engs {
		if miss := missingKeys(e, keys); len(miss) != 0 {
			t.Fatalf("%s lost %d/%d keys after recovery (e.g. %q)", ids[i], len(miss), n, miss[0])
		}
	}
	after := nodes[0].pm.GetMetrics().PartitionMigrations.Load() + nodes[1].pm.GetMetrics().PartitionMigrations.Load()
	if after != before {
		t.Fatalf("%d key batches migrated on failure/recovery, want 0", after-before)
	}
}

// --auto-rebalance in raft mode is a no-op and the agent refuses inbound
// batches.
func TestSetupRebalancer_RaftModeDisablesMigration(t *testing.T) {
	pm := cluster.NewPartitionMap(cluster.DefaultClusterConfig())
	_ = pm.AddNode("rbs-solo", "127.0.0.1", 7900)
	ta := cluster.NewTransferAgent(pm, "rbs-solo", newMemLocalStore(), "127.0.0.1:0")
	stop := setupRebalancer(modeRaft, pm, ta, "rbs-solo", true)
	defer stop()
	if !ta.RaftManaged() {
		t.Fatal("raft mode did not mark the transfer agent raft-managed")
	}
	if err := ta.MigrateToNewOwners(); !errors.Is(err, cluster.ErrMigrationRaftMode) {
		t.Fatalf("err=%v, want ErrMigrationRaftMode", err)
	}
	if !strings.Contains(cluster.ErrMigrationRaftMode.Error(), "raft") {
		t.Fatal("error text should name raft mode")
	}
}

// memLocalStore is a minimal cluster.LocalStore.
type memLocalStore struct{ m map[string][]byte }

func newMemLocalStore() *memLocalStore { return &memLocalStore{m: map[string][]byte{}} }
func (s *memLocalStore) ScanKeys() []string {
	out := make([]string, 0, len(s.m))
	for k := range s.m {
		out = append(out, k)
	}
	return out
}
func (s *memLocalStore) Get(k string) ([]byte, error) {
	if v, ok := s.m[k]; ok {
		return v, nil
	}
	return nil, errors.New("not found")
}
func (s *memLocalStore) GetTTLForKey(string) int32             { return -1 }
func (s *memLocalStore) Put(k string, v []byte, _ int32) error { s.m[k] = v; return nil }
func (s *memLocalStore) Delete(k string) error                 { delete(s.m, k); return nil }
