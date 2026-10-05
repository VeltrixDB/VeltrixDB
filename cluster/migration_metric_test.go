package cluster

import (
	"fmt"
	"testing"
)

// TestMigration_CountsKeyBatches: every key batch a destination acknowledges
// increments ClusterMetrics.PartitionMigrations
// (veltrixdb_cluster_partition_migrations_total), which used to stay 0.
func TestMigration_CountsKeyBatches(t *testing.T) {
	pm := NewPartitionMap(DefaultClusterConfig())
	_ = pm.AddNode("src", "127.0.0.1", 7411)
	srcStore, dstStore := newMemStore(), newMemStore()
	src := NewTransferAgent(pm, "src", srcStore, "127.0.0.1:0")
	dst := NewTransferAgent(pm, "dst", dstStore, "127.0.0.1:0")
	if err := dst.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dst.Stop)
	const n = 3000 // > transferBatchSize: several batches
	for i := 0; i < n; i++ {
		_ = srcStore.Put(fmt.Sprintf("k%05d", i), []byte("v"), -1)
	}
	if err := pm.AddNodeWithTransfer("dst", "127.0.0.1", 7412, dst.BoundAddr()); err != nil {
		t.Fatal(err)
	}
	if err := pm.Rebalance(pm.PartitionCount()); err != nil {
		t.Fatal(err)
	}
	before := pm.GetMetrics().PartitionMigrations.Load()
	if err := src.MigrateToNewOwners(); err != nil {
		t.Fatal(err)
	}
	moved := len(dstStore.ScanKeys())
	if moved == 0 {
		t.Fatal("nothing migrated")
	}
	want := uint64((moved + transferBatchSize - 1) / transferBatchSize)
	if got := pm.GetMetrics().PartitionMigrations.Load() - before; got != want {
		t.Fatalf("PartitionMigrations += %d, want %d (%d keys in batches of %d)", got, want, moved, transferBatchSize)
	}
}
