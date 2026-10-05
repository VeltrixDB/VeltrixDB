package storage

import (
	"os"
	"testing"
	"time"
)

// TestTombstoneReap_WaitsForReplicaWatermark: the defragmenter's tombstone
// reaper must keep a tombstone past its grace period while a replica's
// acknowledgement watermark is below it, and reap it once the watermark
// passes.  (Before the coordinator was wired in, reaping used the grace
// period alone and the first reap removed the tombstone.)
func TestTombstoneReap_WaitsForReplicaWatermark(t *testing.T) {
	dir, err := os.MkdirTemp("", "veltrix-tomb-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	cfg := testStorageConfig(dir)
	cfg.GCGracePeriodSec = 1
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { se.Close() })

	before := time.Now().UnixMicro()
	se.SetReplicaWatermark("r1", before-1) // r1 has not seen the delete
	if err := se.Put("k", []byte("v"), -1); err != nil {
		t.Fatal(err)
	}
	if err := se.Delete("k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond) // past the 1 s grace period, < 2 s cap

	tombstoned := func() bool {
		e, _, ok := se.index.get("k")
		return ok && e.IsTombstone()
	}
	se.defrag.reapExpiredTombstones()
	if !tombstoned() {
		t.Fatal("tombstone reaped although replica r1 has not acknowledged it")
	}

	se.SetReplicaWatermark("r1", time.Now().UnixMicro())
	se.defrag.reapExpiredTombstones()
	if tombstoned() {
		t.Fatal("tombstone not reaped after every replica acknowledged it")
	}
}

// TestCanReapTombstone_Rule: grace period is the floor, replica watermarks
// gate reaping after it, and 2 × grace is the upper bound.
func TestCanReapTombstone_Rule(t *testing.T) {
	const grace = int64(100) // seconds
	ts := int64(1_000_000_000)
	us := func(sec int64) int64 { return ts + sec*1_000_000 }

	tc := NewTombstoneCoordinator()
	if canReapTombstone(tc, ts, us(50), grace) {
		t.Error("reaped inside the grace period")
	}
	if !canReapTombstone(tc, ts, us(150), grace) {
		t.Error("no replicas tracked: grace period alone must apply")
	}
	if !canReapTombstone(nil, ts, us(150), grace) {
		t.Error("nil coordinator: grace period alone must apply")
	}
	tc.SetReplicaWatermark("r1", ts-1)
	if canReapTombstone(tc, ts, us(150), grace) {
		t.Error("reaped while r1's watermark is below the tombstone")
	}
	if !canReapTombstone(tc, ts, us(200), grace) {
		t.Error("not reaped at 2 × grace (upper bound)")
	}
	tc.SetReplicaWatermark("r1", ts)
	if !canReapTombstone(tc, ts, us(150), grace) {
		t.Error("not reaped after r1 acknowledged it")
	}
}
