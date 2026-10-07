package cluster

// migration_rf_test.go — MigrateToNewOwners honours the replication factor
// and never runs in raft mode (CLAUDE.md invariant 62). Before the fix it
// sent every key to its single ring owner and deleted it locally, so with
// RF=3 a node dropped two thirds of the keys it was supposed to keep.

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
)

type rfCluster struct {
	t      *testing.T
	pm     *PartitionMap
	stores map[string]*memStore
	agents map[string]*TransferAgent
}

// newRFCluster builds a shared partition map with the given members and
// replication factor, one memStore + started TransferAgent per member. The
// agents are created after the membership exists, so it is their baseline.
func newRFCluster(t *testing.T, rf int, ids ...string) *rfCluster {
	t.Helper()
	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = rf
	c := &rfCluster{t: t, pm: NewPartitionMap(cfg), stores: map[string]*memStore{}, agents: map[string]*TransferAgent{}}
	for i, id := range ids {
		if err := c.pm.AddNode(id, "127.0.0.1", 7600+i); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		c.startAgent(id)
	}
	return c
}

func (c *rfCluster) startAgent(id string) {
	c.t.Helper()
	s := newMemStore()
	ta := NewTransferAgent(c.pm, id, s, "127.0.0.1:0")
	if err := ta.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(ta.Stop)
	registerTransferAddr(c.t, id, ta.BoundAddr())
	c.stores[id] = s
	c.agents[id] = ta
}

// seed writes every key to each of its current replicas — what replicated
// writes leave behind.
func (c *rfCluster) seed(keys []string) {
	c.t.Helper()
	for _, k := range keys {
		reps, err := c.pm.GetReplicasForKey(k)
		if err != nil {
			c.t.Fatal(err)
		}
		for _, id := range reps {
			_ = c.stores[id].Put(k, []byte("v:"+k), -1)
		}
	}
}

// migrateAll runs MigrateToNewOwners on the given members concurrently.
func (c *rfCluster) migrateAll(ids ...string) []error {
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			errs[i] = c.agents[id].MigrateToNewOwners()
		}(i, id)
	}
	wg.Wait()
	return errs
}

// assertExactPlacement: every key is on exactly its current replica set
// among the given live stores, with the right value.
func (c *rfCluster) assertExactPlacement(keys []string, live []string) {
	c.t.Helper()
	for _, k := range keys {
		reps, err := c.pm.GetReplicasForKey(k)
		if err != nil {
			c.t.Fatal(err)
		}
		sort.Strings(reps)
		var on []string
		for _, id := range live {
			if v, err := c.stores[id].Get(k); err == nil {
				if string(v) != "v:"+k {
					c.t.Fatalf("key %q on %s has value %q", k, id, v)
				}
				on = append(on, id)
			}
		}
		sort.Strings(on)
		if strings.Join(on, ",") != strings.Join(reps, ",") {
			c.t.Fatalf("key %q on %v, want exactly its replicas %v", k, on, reps)
		}
	}
}

func rfKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = diverseKey(i)
	}
	return keys
}

// (d) MigrateToNewOwners refuses in raft mode and deletes nothing; a
// raft-managed receiver rejects inbound batches.
func TestMigrate_RefusesInRaftMode(t *testing.T) {
	c := newRFCluster(t, 1, "rft-a", "rft-b")
	keys := rfKeys(200)
	for _, k := range keys {
		_ = c.stores["rft-a"].Put(k, []byte("v:"+k), -1)
	}
	c.agents["rft-a"].SetRaftManaged(true)
	if err := c.agents["rft-a"].MigrateToNewOwners(); !errors.Is(err, ErrMigrationRaftMode) {
		t.Fatalf("raft-managed MigrateToNewOwners: err=%v, want ErrMigrationRaftMode", err)
	}
	if got := c.stores["rft-a"].count(); got != len(keys) {
		t.Fatalf("raft-managed source holds %d keys, want all %d", got, len(keys))
	}
	if got := c.stores["rft-b"].count(); got != 0 {
		t.Fatalf("raft-managed source sent %d keys", got)
	}

	// The receiving side: a raft-managed node refuses the batch, so the
	// sender keeps every key.
	c.agents["rft-a"].SetRaftManaged(false)
	c.agents["rft-b"].SetRaftManaged(true)
	if err := c.agents["rft-a"].MigrateToNewOwners(); err == nil {
		t.Fatal("batch into a raft-managed node was accepted")
	}
	if got := c.stores["rft-a"].count(); got != len(keys) {
		t.Fatalf("source deleted keys a raft-managed receiver refused: %d left of %d", got, len(keys))
	}
	if got := c.stores["rft-b"].count(); got != 0 {
		t.Fatalf("raft-managed receiver stored %d keys", got)
	}
}

// (b, library level) 3 nodes, RF=3: every node is a replica of every key. A
// node failure followed by a migration pass moves and deletes nothing.
func TestMigrate_RF3ThreeNodes_FailureMovesNothing(t *testing.T) {
	c := newRFCluster(t, 3, "rf3-a", "rf3-b", "rf3-c")
	keys := rfKeys(300)
	c.seed(keys)
	if err := c.pm.UpdateNodeState("rf3-c", NodeStateFailed); err != nil {
		t.Fatal(err)
	}
	if err := c.pm.Rebalance(c.pm.PartitionCount()); err != nil {
		t.Fatal(err)
	}
	c.agents["rf3-c"].Stop() // the failed node is really unreachable
	before := c.pm.GetMetrics().PartitionMigrations.Load()
	errs := c.migrateAll("rf3-a", "rf3-b")
	for _, id := range []string{"rf3-a", "rf3-b", "rf3-c"} {
		if got := c.stores[id].count(); got != len(keys) {
			t.Fatalf("%s holds %d keys after failure, want all %d", id, got, len(keys))
		}
	}
	if got := c.pm.GetMetrics().PartitionMigrations.Load() - before; got != 0 {
		t.Fatalf("%d batches sent after a failure with RF=3 on 3 nodes, want 0", got)
	}
	for _, err := range errs {
		if err != nil {
			t.Fatalf("migration after failure: %v", err)
		}
	}
}

// (c) 4 nodes, RF=3: a node joins, then a node is removed. Each key ends on
// exactly its RF replicas, no key is lost, pinned keys stay everywhere.
func TestMigrate_RF3FourNodes_AddRemoveExactPlacement(t *testing.T) {
	c := newRFCluster(t, 3, "rf4-a", "rf4-b", "rf4-c")
	keys := rfKeys(600)
	c.seed(keys)
	for _, id := range []string{"rf4-a", "rf4-b", "rf4-c"} {
		_ = c.stores[id].Put("@vecns/docs", []byte(`{"dim":4}`), -1)
	}

	// Join: rf4-d's agent sees the 4-node membership as its baseline (it
	// holds nothing yet).
	if err := c.pm.AddNode("rf4-d", "127.0.0.1", 7699); err != nil {
		t.Fatal(err)
	}
	c.startAgent("rf4-d")
	if err := c.pm.Rebalance(c.pm.PartitionCount()); err != nil {
		t.Fatal(err)
	}
	all := []string{"rf4-a", "rf4-b", "rf4-c", "rf4-d"}
	for i, err := range c.migrateAll(all...) {
		if err != nil {
			t.Fatalf("migrate %s after join: %v", all[i], err)
		}
	}
	c.assertExactPlacement(keys, all)
	if n := c.stores["rf4-d"].count(); n < len(keys)/2 {
		t.Fatalf("joined node holds only %d keys", n)
	}
	for _, id := range all {
		if _, err := c.stores[id].Get("@vecns/docs"); err != nil {
			t.Fatalf("pinned key missing on %s after join", id)
		}
	}

	// A second pass with no membership change is a no-op.
	before := c.pm.GetMetrics().PartitionMigrations.Load()
	c.migrateAll(all...)
	if got := c.pm.GetMetrics().PartitionMigrations.Load() - before; got != 0 {
		t.Fatalf("idempotent pass sent %d batches", got)
	}

	// Remove rf4-b: the departing node evacuates to every current replica
	// and the survivors fill in the replicas that newly need keys.
	if err := c.pm.RemoveNode("rf4-b"); err != nil {
		t.Fatal(err)
	}
	if err := c.pm.Rebalance(c.pm.PartitionCount()); err != nil {
		t.Fatal(err)
	}
	for i, err := range c.migrateAll(all...) {
		if err != nil {
			t.Fatalf("migrate %s after removal: %v", all[i], err)
		}
	}
	live := []string{"rf4-a", "rf4-c", "rf4-d"}
	c.assertExactPlacement(keys, live)
	if n := c.stores["rf4-b"].count(); n != 1 { // only the pinned key
		t.Fatalf("removed node still holds %d keys, want only the pinned one", n)
	}
}

// (c) Deletion only after every replica acknowledged: with one new replica
// unreachable, keys that must also go there are not deleted at the source;
// once it is reachable a retry converges.
func TestMigrate_DeletesOnlyAfterEveryReplicaAcks(t *testing.T) {
	c := newRFCluster(t, 3, "ack-a", "ack-b", "ack-c")
	keys := rfKeys(600)
	c.seed(keys)

	if err := c.pm.AddNode("ack-d", "127.0.0.1", 7698); err != nil {
		t.Fatal(err)
	}
	if err := c.pm.Rebalance(c.pm.PartitionCount()); err != nil {
		t.Fatal(err)
	}
	// ack-d is in the membership but not listening yet.
	registerTransferAddr(t, "ack-d", "127.0.0.1:1")
	snapshot := map[string]map[string]bool{}
	for _, id := range []string{"ack-a", "ack-b", "ack-c"} {
		snapshot[id] = map[string]bool{}
		for _, k := range c.stores[id].ScanKeys() {
			snapshot[id][k] = true
		}
	}
	c.migrateAll("ack-a", "ack-b", "ack-c")
	for _, id := range []string{"ack-a", "ack-b", "ack-c"} {
		for k := range snapshot[id] {
			if _, err := c.stores[id].Get(k); err == nil {
				continue
			}
			reps, _ := c.pm.GetReplicasForKey(k)
			for _, r := range reps {
				if r == "ack-d" {
					t.Fatalf("%s deleted %q although replica ack-d never acknowledged it", id, k)
				}
			}
		}
	}
	for _, k := range keys {
		if _, ok := findKeyInStores(c.stores, k); !ok {
			t.Fatalf("key %q lost while a replica was down", k)
		}
	}

	// ack-d comes up; the next pass converges.
	c.startAgent("ack-d")
	all := []string{"ack-a", "ack-b", "ack-c", "ack-d"}
	for i, err := range c.migrateAll(all...) {
		if err != nil {
			t.Fatalf("retry on %s: %v", all[i], err)
		}
	}
	c.assertExactPlacement(keys, all)
}

// Derived keys and their record leave together: the record is deleted only
// after its vector, so a node never deletes a record whose vector it still
// holds unsent (invariant 55/60), and placement follows RoutingKey.
func TestMigrate_RF_DerivedKeysFollowRecord(t *testing.T) {
	c := newRFCluster(t, 1, "drv-a", "drv-b")
	var keys []string
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("doc%03d", i)
		keys = append(keys, id, "@vec/ns/"+id, "@txt/ns/"+id)
	}
	for _, k := range keys {
		_ = c.stores["drv-a"].Put(k, []byte("v:"+k), -1)
	}
	if err := c.agents["drv-a"].MigrateToNewOwners(); err != nil {
		t.Fatal(err)
	}
	c.assertExactPlacement(keys, []string{"drv-a", "drv-b"})
}
