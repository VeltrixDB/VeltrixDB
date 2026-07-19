package cluster

import (
	"fmt"
	"sync"
	"testing"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// newPM3 returns a PartitionMap pre-loaded with three active nodes (n1, n2, n3).
// ReplicationFactor is 1 so Rebalance succeeds with any number of nodes ≥ 1.
func newPM3(t *testing.T) *PartitionMap {
	t.Helper()
	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	for i, id := range []string{"n1", "n2", "n3"} {
		if err := pm.AddNode(id, "127.0.0.1", 9000+i); err != nil {
			t.Fatalf("AddNode(%s): %v", id, err)
		}
	}
	return pm
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestPartitionMap_AssignPartitions: after Rebalance all requested partitions
// are present and each has a PrimaryNode.
func TestPartitionMap_AssignPartitions(t *testing.T) {
	t.Parallel()

	pm := newPM3(t)
	const partCount uint32 = 12

	if err := pm.Rebalance(partCount); err != nil {
		t.Fatalf("Rebalance: %v", err)
	}

	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if uint32(len(pm.Partitions)) != partCount {
		t.Errorf("expected %d partitions, got %d", partCount, len(pm.Partitions))
	}
	for id, p := range pm.Partitions {
		if p.PrimaryNode == "" {
			t.Errorf("partition %d has no PrimaryNode", id)
		}
	}
}

// TestPartitionMap_Rebalance: adding a 4th node and rebalancing distributes
// partitions more evenly (max imbalance ≤ ceil(total/nodes)).
func TestPartitionMap_Rebalance(t *testing.T) {
	t.Parallel()

	pm := newPM3(t)
	const partCount uint32 = 12
	if err := pm.Rebalance(partCount); err != nil {
		t.Fatalf("initial Rebalance: %v", err)
	}

	// Add a 4th node.
	if err := pm.AddNode("n4", "127.0.0.1", 9003); err != nil {
		t.Fatalf("AddNode(n4): %v", err)
	}
	if err := pm.Rebalance(partCount); err != nil {
		t.Fatalf("second Rebalance: %v", err)
	}

	// Count partitions per node.
	counts := make(map[string]int)
	pm.mu.RLock()
	for _, p := range pm.Partitions {
		counts[p.PrimaryNode]++
	}
	pm.mu.RUnlock()

	// With 4 nodes and 12 partitions we expect 3 each (perfectly balanced).
	// Allow ceil(12/4)=3 ± 1 as acceptable skew.
	expected := int(partCount) / 4
	for nodeID, c := range counts {
		if c < expected-1 || c > expected+1 {
			t.Errorf("node %s has %d partitions; expected ~%d (±1)", nodeID, c, expected)
		}
	}
}

// TestPartitionMap_RemoveNode: removing a node causes its partitions to be
// reassigned to the remaining nodes.
func TestPartitionMap_RemoveNode(t *testing.T) {
	t.Parallel()

	pm := newPM3(t)
	const partCount uint32 = 9
	if err := pm.Rebalance(partCount); err != nil {
		t.Fatalf("Rebalance: %v", err)
	}

	if err := pm.RemoveNode("n3"); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if err := pm.Rebalance(partCount); err != nil {
		t.Fatalf("post-removal Rebalance: %v", err)
	}

	// n3 must not appear as PrimaryNode in any partition.
	pm.mu.RLock()
	for id, p := range pm.Partitions {
		if p.PrimaryNode == "n3" {
			t.Errorf("partition %d still assigned to removed node n3", id)
		}
		for _, r := range p.ReplicaNodes {
			if r == "n3" {
				t.Errorf("partition %d still has n3 as replica", id)
			}
		}
	}
	pm.mu.RUnlock()
}

// TestPartitionMap_ConsistentHashing: GetNodeForKey returns the same node for
// the same key (before any topology change).
func TestPartitionMap_ConsistentHashing(t *testing.T) {
	t.Parallel()

	pm := newPM3(t)
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}

	// Gather first mapping.
	firstOwner := make(map[string]string)
	for _, k := range keys {
		owner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%q): %v", k, err)
		}
		firstOwner[k] = owner
	}

	// Call again — must be identical.
	for _, k := range keys {
		owner, err := pm.GetNodeForKey(k)
		if err != nil {
			t.Fatalf("GetNodeForKey(%q) second call: %v", k, err)
		}
		if owner != firstOwner[k] {
			t.Errorf("key %q: first owner %q, second owner %q", k, firstOwner[k], owner)
		}
	}
}

// TestPartitionMap_Version: Version increments on every topology change.
func TestPartitionMap_Version(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	v0 := pm.GetVersion()

	if err := pm.AddNode("x", "127.0.0.1", 9000); err != nil {
		t.Fatal(err)
	}
	v1 := pm.GetVersion()
	if v1 <= v0 {
		t.Errorf("version did not increase after AddNode: %d → %d", v0, v1)
	}

	if err := pm.Rebalance(4); err != nil {
		t.Fatal(err)
	}
	v2 := pm.GetVersion()
	if v2 <= v1 {
		t.Errorf("version did not increase after Rebalance: %d → %d", v1, v2)
	}

	if err := pm.RemoveNode("x"); err != nil {
		t.Fatal(err)
	}
	v3 := pm.GetVersion()
	if v3 <= v2 {
		t.Errorf("version did not increase after RemoveNode: %d → %d", v2, v3)
	}
}

// TestPartitionMap_GetOwner: GetPartition returns the expected PrimaryNode
// after a controlled Rebalance.
func TestPartitionMap_GetOwner(t *testing.T) {
	t.Parallel()

	pm := newPM3(t)
	const partCount uint32 = 6
	if err := pm.Rebalance(partCount); err != nil {
		t.Fatalf("Rebalance: %v", err)
	}

	// Every partition returned by GetPartition must have a non-empty PrimaryNode.
	for i := uint32(0); i < partCount; i++ {
		p, err := pm.GetPartition(i)
		if err != nil {
			t.Errorf("GetPartition(%d): %v", i, err)
			continue
		}
		if p.PrimaryNode == "" {
			t.Errorf("GetPartition(%d) returned empty PrimaryNode", i)
		}
		// PrimaryNode must be one of the registered nodes.
		pm.mu.RLock()
		_, exists := pm.Nodes[p.PrimaryNode]
		pm.mu.RUnlock()
		if !exists {
			t.Errorf("GetPartition(%d) PrimaryNode %q not in partition map", i, p.PrimaryNode)
		}
	}

	// Requesting a non-existent partition must return an error.
	_, err := pm.GetPartition(partCount + 100)
	if err == nil {
		t.Error("expected error for non-existent partition, got nil")
	}
}

// TestPartitionMap_Concurrent: 50 goroutines doing GetNodeForKey concurrently;
// no data race (run with -race).
func TestPartitionMap_Concurrent(t *testing.T) {
	t.Parallel()

	pm := newPM3(t)
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for _, k := range keys {
				if _, err := pm.GetNodeForKey(k); err != nil {
					// Ring is not empty (3 nodes added above) — fail the test.
					t.Errorf("goroutine %d GetNodeForKey(%q): %v", gid, k, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
