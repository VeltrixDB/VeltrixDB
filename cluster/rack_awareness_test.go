package cluster

import (
	"fmt"
	"testing"
)

// buildRackedMap creates a partition map with the given nodeID→rack layout.
func buildRackedMap(t *testing.T, rf int, racks map[string]string) *PartitionMap {
	t.Helper()
	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = rf
	pm := NewPartitionMap(cfg)
	for id, rack := range racks {
		if err := pm.AddNode(id, "127.0.0.1", 9000); err != nil {
			t.Fatalf("AddNode(%s): %v", id, err)
		}
		if rack != "" {
			if err := pm.SetNodeRack(id, rack); err != nil {
				t.Fatalf("SetNodeRack(%s): %v", id, err)
			}
		}
	}
	return pm
}

// rackOf returns the rack layout used by most tests: 4 nodes across 2 zones.
func twoZoneLayout() map[string]string {
	return map[string]string{
		"node-a1": "zone-a", "node-a2": "zone-a",
		"node-b1": "zone-b", "node-b2": "zone-b",
	}
}

func replicaRacks(t *testing.T, pm *PartitionMap, p *PartitionInfo) map[string]int {
	t.Helper()
	racks := map[string]int{}
	racks[pm.GetNodeRack(p.PrimaryNode)]++
	for _, r := range p.ReplicaNodes {
		racks[pm.GetNodeRack(r)]++
	}
	return racks
}

func TestRebalance_RackAware_RF2SpansBothZones(t *testing.T) {
	pm := buildRackedMap(t, 2, twoZoneLayout())
	if err := pm.Rebalance(64); err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	for i := uint32(0); i < 64; i++ {
		p := pm.Partitions[i]
		if got := 1 + len(p.ReplicaNodes); got != 2 {
			t.Fatalf("partition %d: %d replicas, want 2", i, got)
		}
		racks := replicaRacks(t, pm, p)
		if len(racks) != 2 {
			t.Fatalf("partition %d: both copies in one rack: %v (primary=%s replicas=%v)",
				i, racks, p.PrimaryNode, p.ReplicaNodes)
		}
	}
}

func TestRebalance_RackAware_RFExceedsRacksFallsBack(t *testing.T) {
	// RF=3 but only 2 racks: must still place 3 distinct nodes, spanning both racks.
	pm := buildRackedMap(t, 3, twoZoneLayout())
	if err := pm.Rebalance(32); err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	for i := uint32(0); i < 32; i++ {
		p := pm.Partitions[i]
		all := append([]string{p.PrimaryNode}, p.ReplicaNodes...)
		if len(all) != 3 {
			t.Fatalf("partition %d: %d replicas, want 3", i, len(all))
		}
		seen := map[string]bool{}
		for _, id := range all {
			if seen[id] {
				t.Fatalf("partition %d: duplicate node %s", i, id)
			}
			seen[id] = true
		}
		if racks := replicaRacks(t, pm, p); len(racks) != 2 {
			t.Fatalf("partition %d: want copies in both racks, got %v", i, racks)
		}
	}
}

func TestRebalance_NoRacks_PlainRoundRobinPreserved(t *testing.T) {
	// Rackless cluster must produce the identical table the pre-rack code did:
	// primary = sorted node i%n, backups = following ring order.
	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 2
	pm := NewPartitionMap(cfg)
	for _, id := range []string{"n1", "n2", "n3"} {
		if err := pm.AddNode(id, "127.0.0.1", 9000); err != nil {
			t.Fatal(err)
		}
	}
	if err := pm.Rebalance(9); err != nil {
		t.Fatal(err)
	}
	sorted := []string{"n1", "n2", "n3"}
	for i := uint32(0); i < 9; i++ {
		p := pm.Partitions[i]
		wantPrimary := sorted[int(i)%3]
		wantBackup := sorted[(int(i)+1)%3]
		if p.PrimaryNode != wantPrimary || len(p.ReplicaNodes) != 1 || p.ReplicaNodes[0] != wantBackup {
			t.Fatalf("partition %d: got %s/%v, want %s/[%s]",
				i, p.PrimaryNode, p.ReplicaNodes, wantPrimary, wantBackup)
		}
	}
}

func TestRebalance_RackAware_Deterministic(t *testing.T) {
	mk := func() *PartitionMap {
		pm := buildRackedMap(t, 2, twoZoneLayout())
		if err := pm.Rebalance(128); err != nil {
			t.Fatalf("Rebalance: %v", err)
		}
		return pm
	}
	a, b := mk(), mk()
	for i := uint32(0); i < 128; i++ {
		pa, pb := a.Partitions[i], b.Partitions[i]
		if pa.PrimaryNode != pb.PrimaryNode || fmt.Sprint(pa.ReplicaNodes) != fmt.Sprint(pb.ReplicaNodes) {
			t.Fatalf("partition %d differs across identical maps: %s/%v vs %s/%v",
				i, pa.PrimaryNode, pa.ReplicaNodes, pb.PrimaryNode, pb.ReplicaNodes)
		}
	}
}

func TestRebalance_RackAware_PrimarySkewUnchanged(t *testing.T) {
	pm := buildRackedMap(t, 2, twoZoneLayout())
	if err := pm.Rebalance(256); err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	counts := map[string]int{}
	for _, p := range pm.Partitions {
		counts[p.PrimaryNode]++
	}
	min, max := 1<<30, 0
	for _, c := range counts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	if max-min > 1 {
		t.Fatalf("primary skew %d (min=%d max=%d counts=%v), want ≤1", max-min, min, max, counts)
	}
}

func TestGetReplicasForKey_RackAwareAndDeduped(t *testing.T) {
	pm := buildRackedMap(t, 2, twoZoneLayout())
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("user:%04d", i)
		reps, err := pm.GetReplicasForKey(key)
		if err != nil {
			t.Fatalf("GetReplicasForKey(%s): %v", key, err)
		}
		if len(reps) != 2 {
			t.Fatalf("key %s: %d replicas %v, want 2 distinct", key, len(reps), reps)
		}
		if reps[0] == reps[1] {
			t.Fatalf("key %s: duplicate replica %v", key, reps)
		}
		if pm.GetNodeRack(reps[0]) == pm.GetNodeRack(reps[1]) {
			t.Fatalf("key %s: both replicas in rack %s", key, pm.GetNodeRack(reps[0]))
		}
	}
}

func TestGossip_PropagatesRack(t *testing.T) {
	// Node B learns node A's rack from A's self-reported digest entry.
	cfg := DefaultClusterConfig()
	pmA := NewPartitionMap(cfg)
	pmB := NewPartitionMap(cfg)
	for _, pm := range []*PartitionMap{pmA, pmB} {
		if err := pm.AddNode("A", "127.0.0.1", 9000); err != nil {
			t.Fatal(err)
		}
		if err := pm.AddNode("B", "127.0.0.1", 9001); err != nil {
			t.Fatal(err)
		}
	}
	if err := pmA.SetNodeRack("A", "zone-a"); err != nil {
		t.Fatal(err)
	}

	gpA := NewGossipProtocol("A", pmA, nil)
	gpB := NewGossipProtocol("B", pmB, nil)

	digest := gpA.buildDigest()
	if digest.Nodes["A"].Rack != "zone-a" {
		t.Fatalf("digest missing self rack: %+v", digest.Nodes["A"])
	}
	gpB.mergeDigest(digest)
	if got := pmB.GetNodeRack("A"); got != "zone-a" {
		t.Fatalf("B's view of A's rack = %q, want zone-a", got)
	}
}
