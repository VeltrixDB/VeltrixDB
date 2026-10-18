package storage

// replication_recovery_test.go — the storage half of replicated-mode replica
// recovery across a primary restart (invariant 61): the catch-up feed
// (ScanChanges) must still carry every delete and only this node's own writes
// after a crash or a clean restart.

import (
	"sort"
	"testing"
	"time"
)

func openRecoveryEngine(t *testing.T, dir string) *StorageEngine {
	t.Helper()
	se, err := NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	return se
}

// feed returns ScanChanges(since, localOnly) as key → "PUT:value" / "DEL".
func feed(t *testing.T, se *StorageEngine, sinceUs int64, localOnly bool) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := se.ScanChanges(sinceUs, 7, localOnly, func(evs []CDCEvent) error {
		if len(evs) > 7 {
			t.Errorf("page of %d events, want <= 7", len(evs))
		}
		for _, ev := range evs {
			if _, dup := out[ev.Key]; dup {
				t.Errorf("key %q emitted twice", ev.Key)
			}
			if ev.Op == "DEL" {
				out[ev.Key] = "DEL"
			} else {
				out[ev.Key] = "PUT:" + string(ev.Value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func keysOf(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// writeRecoveryWorkload: local puts / deletes plus writes applied from a peer.
func writeRecoveryWorkload(t *testing.T, se *StorageEngine) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"live", "del", "peer-overwritten", "peer-deleted"} {
		must(se.Put(k, []byte("v-"+k), 0))
	}
	must(se.Delete("del"))
	must(se.PutReplicated("peer-put", []byte("pv"), 0))
	must(se.PutReplicated("peer-overwritten", []byte("pv2"), 0))
	must(se.DeleteReplicated("peer-deleted"))
	must(se.PutReplicated("peer-then-local", []byte("x"), 0))
	must(se.Put("peer-then-local", []byte("mine"), 0)) // local write clears the origin bit
	must(se.PutReplicated("peer-then-local-del", []byte("x"), 0))
	must(se.Delete("peer-then-local-del"))
}

func wantRecoveryFeed(t *testing.T, label string, got map[string]string) {
	t.Helper()
	want := map[string]string{
		"live":                "PUT:v-live",
		"del":                 "DEL",
		"peer-then-local":     "PUT:mine",
		"peer-then-local-del": "DEL",
	}
	if len(got) != len(want) {
		t.Fatalf("%s: local feed keys = %v, want %v", label, keysOf(got), keysOf(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: feed[%q] = %q, want %q", label, k, got[k], v)
		}
	}
}

// TestScanChanges_LocalOnlySkipsPeerWrites: the catch-up feed carries only
// writes this node originated — never a key whose latest write (put or
// delete) was applied from a replication peer — and keeps doing so across a
// crash restart (the origin bit is in the WAL).  Without it a replica that
// came back re-sent its stale copies of its peers' keys to them and undid
// their deletes (found by TestReplicatedRecovery_*).
func TestScanChanges_LocalOnlySkipsPeerWrites(t *testing.T) {
	dir := t.TempDir()
	se := openRecoveryEngine(t, dir)
	writeRecoveryWorkload(t, se)
	wantRecoveryFeed(t, "live engine", feed(t, se, 0, true))
	if all := feed(t, se, 0, false); len(all) != 7 {
		t.Errorf("unfiltered feed has %d keys (%v), want 7", len(all), keysOf(all))
	}

	abandonEngineForCrashTest(se) // crash: WAL replayed as written
	se2 := openRecoveryEngine(t, dir)
	defer se2.Close()
	wantRecoveryFeed(t, "after crash restart", feed(t, se2, 0, true))
}

// TestCheckpoint_KeepsTombstonesReplicasNeed: in replicated mode (a replica
// watermark reported) a clean shutdown keeps every tombstone the reaper would
// still keep — with its timestamp and origin — so the catch-up feed of a
// restarted primary still carries the deletes a down replica missed.  Before,
// the checkpoint dropped every tombstone: after a clean restart the deletes
// were gone from the feed and the replica kept the deleted keys forever.
func TestCheckpoint_KeepsTombstonesReplicasNeed(t *testing.T) {
	dir := t.TempDir()
	se := openRecoveryEngine(t, dir)
	writeRecoveryWorkload(t, se)
	before := feed(t, se, 0, true)
	delTs := se.mustEntry(t, "del").WriteTimestampUs
	// A replica that has acked nothing yet (persisted watermark from before
	// the workload, as the replication engine reports at startup).
	se.SetReplicaWatermark("r3", 1)
	if err := se.Close(); err != nil {
		t.Fatal(err)
	}

	se2 := openRecoveryEngine(t, dir)
	wantRecoveryFeed(t, "after clean restart", feed(t, se2, 0, true))
	if got := feed(t, se2, 0, true); got["del"] != before["del"] {
		t.Errorf("del: %q before, %q after", before["del"], got["del"])
	}
	if e := se2.mustEntry(t, "del"); e.WriteTimestampUs != delTs || !e.IsTombstone() {
		t.Errorf("tombstone after restart: ts %d tomb=%v, want ts %d", e.WriteTimestampUs, e.IsTombstone(), delTs)
	}
	if e := se2.mustEntry(t, "peer-deleted"); e.Flags&FlagReplicaApplied == 0 {
		t.Error("peer delete lost its origin bit through the checkpoint")
	}
	if _, err := se2.Get("del"); err == nil {
		t.Error("deleted key readable after restart")
	}
	if err := se2.Close(); err != nil { // no watermark reported this run
		t.Fatal(err)
	}

	// Without replicas (single node / raft) the checkpoint drops every
	// tombstone, as before.
	se3 := openRecoveryEngine(t, dir)
	defer se3.Close()
	got := feed(t, se3, 0, false)
	for k, v := range got {
		if v == "DEL" {
			t.Errorf("tombstone %q kept by a checkpoint with no replica watermark", k)
		}
	}
}

// TestCheckpointKeepsTombstone_FollowsReapRule: the checkpoint keeps exactly what
// the reaper keeps — a tombstone every replica acknowledged and past the grace
// period is dropped; one a replica still needs is kept until 2 × grace.
func TestCheckpointKeepsTombstone_FollowsReapRule(t *testing.T) {
	se := openRecoveryEngine(t, t.TempDir())
	defer se.Close()
	grace := se.config.GCGracePeriodSec * 1_000_000
	now := time.Now().UnixMicro()
	if se.checkpointKeepsTombstone(now-2*grace, now) {
		t.Error("kept with no replicas tracked")
	}
	se.SetReplicaWatermark("r1", now-grace-10) // below the tombstones below
	cases := []struct {
		ts   int64
		keep bool
	}{
		{now - 1, true},            // younger than grace
		{now - grace - 5, true},    // replica watermark below it
		{now - grace - 20, false},  // replica acked it, grace passed
		{now - 2*grace - 1, false}, // past 2 × grace: reaped regardless
	}
	for _, c := range cases {
		if got := se.checkpointKeepsTombstone(c.ts, now); got != c.keep {
			t.Errorf("age %ds: keep=%v, want %v", (now-c.ts)/1_000_000, got, c.keep)
		}
	}
	if got := se.TombstoneRetention(); got != 2*time.Duration(se.config.GCGracePeriodSec)*time.Second {
		t.Errorf("TombstoneRetention = %v", got)
	}
}

func (se *StorageEngine) mustEntry(t *testing.T, key string) IndexEntry {
	t.Helper()
	h := fnv64a(key)
	shard := &se.index.shards[uint16(h&(numShards-1))]
	shard.mu.RLock()
	e, ok := shard.entries.get(key, h) // tombstones included
	shard.mu.RUnlock()
	if !ok {
		t.Fatalf("no index entry for %q", key)
	}
	return e
}
