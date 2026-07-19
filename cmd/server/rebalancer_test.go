package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
)

// TestAutoRebalancer_MigratesOnNodeJoin proves the previously-unwired path:
// a membership change on the partition map now triggers ring rebalance AND
// physical key migration to the new owner, end-to-end over the transfer HTTP
// protocol.
func TestAutoRebalancer_MigratesOnNodeJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-engine integration test")
	}

	cfg := cluster.DefaultClusterConfig()

	// Node 1: the pre-existing node holding all keys.
	pm1 := cluster.NewPartitionMap(cfg)
	if err := pm1.AddNode("node-1", "127.0.0.1", 7101); err != nil {
		t.Fatal(err)
	}
	eng1 := newFSMTestEngine(t)
	ta1 := cluster.NewTransferAgent(pm1, "node-1", eng1, "127.0.0.1:0")
	if err := ta1.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ta1.Stop)
	pm1.SetNodeTransferAddr("node-1", ta1.BoundAddr())

	stop := startAutoRebalancer(pm1, ta1, "node-1")
	t.Cleanup(stop)

	// Seed keys while node-1 is the only owner.
	const n = 200
	for i := 0; i < n; i++ {
		if err := eng1.Put(fmt.Sprintf("reb:key:%03d", i), []byte("v"), -1); err != nil {
			t.Fatal(err)
		}
	}

	// Node 2 joins: its own map mirrors the two-node topology so epoch fencing
	// accepts batches from node-1.
	pm2 := cluster.NewPartitionMap(cfg)
	if err := pm2.AddNode("node-1", "127.0.0.1", 7101); err != nil {
		t.Fatal(err)
	}
	eng2 := newFSMTestEngine(t)
	ta2 := cluster.NewTransferAgent(pm2, "node-2", eng2, "127.0.0.1:0")
	if err := ta2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ta2.Stop)
	if err := pm2.AddNode("node-2", "127.0.0.1", 7102); err != nil {
		t.Fatal(err)
	}

	// The join event on node-1's map — this is what gossip delivers in prod.
	if err := pm1.AddNodeWithTransfer("node-2", "127.0.0.1", 7102, ta2.BoundAddr()); err != nil {
		t.Fatal(err)
	}

	// Wait until migration completes: placement stabilises when node-1 holds
	// only keys it owns and the totals add up (deletes fire after delivery).
	placementDone := func() bool {
		k1, k2 := eng1.ScanKeys(), eng2.ScanKeys()
		if len(k2) == 0 || len(k1)+len(k2) != n {
			return false
		}
		for _, k := range k1 {
			if owner, err := pm1.GetNodeForKey(k); err != nil || owner != "node-1" {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(rebalanceDebounce + 20*time.Second)
	for time.Now().Before(deadline) && !placementDone() {
		time.Sleep(200 * time.Millisecond)
	}

	moved := len(eng2.ScanKeys())
	remaining := len(eng1.ScanKeys())
	if moved == 0 {
		t.Fatal("no keys migrated to node-2 after join — auto-rebalance did not fire")
	}
	if moved+remaining != n {
		t.Fatalf("key loss during migration: node1=%d node2=%d want total=%d", remaining, moved, n)
	}

	// Every migrated key must now be owned by node-2 per the ring, and every
	// remaining key by node-1 — i.e. data placement matches routing.
	for _, k := range eng2.ScanKeys() {
		owner, err := pm1.GetNodeForKey(k)
		if err != nil || owner != "node-2" {
			t.Fatalf("key %q on node-2 but ring owner=%s err=%v", k, owner, err)
		}
	}
	for _, k := range eng1.ScanKeys() {
		owner, err := pm1.GetNodeForKey(k)
		if err != nil || owner != "node-1" {
			t.Fatalf("key %q on node-1 but ring owner=%s err=%v", k, owner, err)
		}
	}
	t.Logf("migrated %d/%d keys to node-2; %d stayed on node-1", moved, n, remaining)
}
