package storage

// raft_applied_test.go — the raft applied-index marker and the replay cut
// (raft_applied.go, CLAUDE.md invariant 65).

import (
	"fmt"
	"path/filepath"
	"testing"
)

func openApplied(t *testing.T, applied bool, mut func(*StorageConfig), dirs ...string) *StorageEngine {
	t.Helper()
	cfg := testStorageConfig(dirs...)
	cfg.RaftAppliedIndex = applied
	if mut != nil {
		mut(cfg)
	}
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	<-se.ReplayDone
	return se
}

func mustPut(t *testing.T, se *StorageEngine, k, v string) {
	t.Helper()
	if err := se.Put(k, []byte(v), -1); err != nil {
		t.Fatalf("put %s: %v", k, err)
	}
}

func wantValue(t *testing.T, se *StorageEngine, k, v string) {
	t.Helper()
	got, err := se.Get(k)
	if v == "" {
		if err == nil {
			t.Fatalf("%s = %q, want absent", k, got)
		}
		return
	}
	if err != nil || string(got) != v {
		t.Fatalf("%s = %q (%v), want %q", k, got, err, v)
	}
}

func wantApplied(t *testing.T, se *StorageEngine, want AppliedIndexState) {
	t.Helper()
	got, ok := se.RaftAppliedState()
	if !ok || got != want {
		t.Fatalf("RaftAppliedState = %+v ok=%v, want %+v", got, ok, want)
	}
}

// TestRaftApplied_CrashCutsWritesAfterMarker: writes made after the newest
// durable marker (an entry being applied when the node crashed) are dropped
// by replay — counters, lists and multi-key batches alike — and stay dropped
// after later markers and later crashes (dead ranges), until a clean
// shutdown's checkpoint removes them for good.
func TestRaftApplied_CrashCutsWritesAfterMarker(t *testing.T) {
	for _, tc := range []struct {
		name string
		dirs int
		mut  func(*StorageConfig)
	}{
		{"binary-1disk", 1, nil},
		{"binary-3disks", 3, nil},
		{"text-wal", 1, func(c *StorageConfig) { c.WALFormat = "text" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dirs []string
			root := t.TempDir()
			for i := 0; i < tc.dirs; i++ {
				dirs = append(dirs, filepath.Join(root, fmt.Sprint("d", i)))
			}
			se := openApplied(t, true, tc.mut, dirs...)
			if _, ok := se.RaftAppliedState(); ok {
				t.Fatal("fresh engine reports an applied index")
			}
			// Entries 1..3 applied, then the marker.
			mustPut(t, se, "a", "1")
			if _, err := se.Increment("ctr", 5, -1); err != nil {
				t.Fatal(err)
			}
			if _, _, err := se.RPush("list", []byte("x")); err != nil {
				t.Fatal(err)
			}
			if err := se.SetRaftAppliedState(AppliedIndexState{Index: 3, Term: 1}, true); err != nil {
				t.Fatal(err)
			}
			// Entries 4..6 "in flight" at the crash: all durable, no marker.
			mustPut(t, se, "a", "9")
			if _, err := se.Increment("ctr", 5, -1); err != nil {
				t.Fatal(err)
			}
			if _, _, err := se.RPush("list", []byte("y")); err != nil {
				t.Fatal(err)
			}
			var batch []MultiPutRequest
			for i := 0; i < 64; i++ {
				batch = append(batch, MultiPutRequest{Key: fmt.Sprintf("b%02d", i), Value: []byte("v"), TTL: -1})
			}
			for _, err := range se.MultiPut(batch) {
				if err != nil {
					t.Fatal(err)
				}
			}
			abandonEngineForCrashTest(se)

			se = openApplied(t, true, tc.mut, dirs...)
			wantApplied(t, se, AppliedIndexState{Index: 3, Term: 1})
			if _, dropped := se.RaftAppliedCutStats(); dropped < 67 {
				t.Fatalf("cut dropped %d records, want ≥ 67", dropped)
			}
			wantValue(t, se, "a", "1")
			wantValue(t, se, "ctr", "5")
			if n := se.LLen("list"); n != 1 {
				t.Fatalf("list length %d after the cut, want 1", n)
			}
			for i := 0; i < 64; i++ {
				wantValue(t, se, fmt.Sprintf("b%02d", i), "")
			}
			for _, k := range se.ScanKeys() {
				if k == appliedMarkerKey {
					t.Fatal("the marker key is visible in the keyspace")
				}
			}

			// Re-apply 4 differently, record it, crash again: the records
			// dropped by the first cut must not come back.
			mustPut(t, se, "a", "4")
			if err := se.SetRaftAppliedState(AppliedIndexState{Index: 4, Term: 2}, true); err != nil {
				t.Fatal(err)
			}
			abandonEngineForCrashTest(se)
			se = openApplied(t, true, tc.mut, dirs...)
			wantApplied(t, se, AppliedIndexState{Index: 4, Term: 2})
			wantValue(t, se, "a", "4")
			wantValue(t, se, "ctr", "5")
			wantValue(t, se, "b00", "")
			if n := se.LLen("list"); n != 1 {
				t.Fatalf("list length %d after the second crash, want 1 (dropped records came back)", n)
			}

			// A clean shutdown checkpoints: the marker survives, the
			// dropped records are gone, and the dead ranges are forgotten.
			if err := se.Close(); err != nil {
				t.Fatal(err)
			}
			se = openApplied(t, true, tc.mut, dirs...)
			wantApplied(t, se, AppliedIndexState{Index: 4, Term: 2})
			wantValue(t, se, "a", "4")
			wantValue(t, se, "b00", "")
			if _, dropped := se.RaftAppliedCutStats(); dropped != 0 {
				t.Fatalf("after a clean restart the cut dropped %d records, want 0", dropped)
			}
			if n := len(se.applied.m.dead); n != 0 {
				t.Fatalf("%d dead ranges after a clean restart, want 0", n)
			}
			se.Close()
		})
	}
}

// TestRaftApplied_NotRaftModeKeepsEverything: an engine opened without
// RaftAppliedIndex (standalone / replicated, or a raft data dir reopened in
// another mode) replays every record, never shows the marker key and reports
// no applied state.
func TestRaftApplied_NotRaftModeKeepsEverything(t *testing.T) {
	dir := t.TempDir()
	se := openApplied(t, true, nil, dir)
	mustPut(t, se, "a", "1")
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 1, Term: 1}, true); err != nil {
		t.Fatal(err)
	}
	mustPut(t, se, "a", "2")
	abandonEngineForCrashTest(se)

	se = openApplied(t, false, nil, dir)
	defer se.Close()
	if _, ok := se.RaftAppliedState(); ok {
		t.Fatal("non-raft engine reports an applied index")
	}
	wantValue(t, se, "a", "2")
	if _, err := se.Get(appliedMarkerKey); err == nil {
		t.Fatal("marker key is readable")
	}
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 9}, true); err != nil {
		t.Fatalf("SetRaftAppliedState on a non-raft engine: %v (want a no-op)", err)
	}
}

// TestRaftApplied_UpgradeFromUnmarkedEngine: a data dir written without any
// marker (v1.16, or a node that never applied anything) reports no applied
// state and replays everything; the first marker then cuts from there on.
func TestRaftApplied_UpgradeFromUnmarkedEngine(t *testing.T) {
	dir := t.TempDir()
	se := openApplied(t, false, nil, dir)
	mustPut(t, se, "old", "v")
	abandonEngineForCrashTest(se)

	se = openApplied(t, true, nil, dir)
	if _, ok := se.RaftAppliedState(); ok {
		t.Fatal("unmarked engine reports an applied index")
	}
	wantValue(t, se, "old", "v")
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 10, Term: 3}, false); err != nil {
		t.Fatal(err)
	}
	mustPut(t, se, "x", "1") // same WAL, after the async marker: flushes it
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 11, Term: 3}, true); err != nil {
		t.Fatal(err)
	}
	mustPut(t, se, "y", "in-flight")
	abandonEngineForCrashTest(se)
	se = openApplied(t, true, nil, dir)
	defer se.Close()
	wantApplied(t, se, AppliedIndexState{Index: 11, Term: 3})
	wantValue(t, se, "old", "v")
	wantValue(t, se, "x", "1")
	wantValue(t, se, "y", "")
}

// TestRaftApplied_RestoreMarkedInProgress: BeginRaftRestore is durable before
// the restore's writes; a crash mid-restore reopens with Restoring set (so
// raft restores again) and the partial restore cut away; completing the
// restore clears it.
func TestRaftApplied_RestoreMarkedInProgress(t *testing.T) {
	dir := t.TempDir()
	se := openApplied(t, true, nil, dir)
	mustPut(t, se, "k", "before")
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 5, Term: 1}, true); err != nil {
		t.Fatal(err)
	}
	if err := se.BeginRaftRestore(); err != nil {
		t.Fatal(err)
	}
	r := se.NewRestorer()
	if err := r.Put(SnapshotKV{Key: "k", Value: []byte("restored")}); err != nil {
		t.Fatal(err)
	}
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	abandonEngineForCrashTest(se)

	se = openApplied(t, true, nil, dir)
	wantApplied(t, se, AppliedIndexState{Index: 5, Term: 1, Restoring: true})
	wantValue(t, se, "k", "before")
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 40, Term: 2}, true); err != nil {
		t.Fatal(err)
	}
	abandonEngineForCrashTest(se)
	se = openApplied(t, true, nil, dir)
	defer se.Close()
	wantApplied(t, se, AppliedIndexState{Index: 40, Term: 2})
}

// TestRaftApplied_CheckpointRunsThroughGate: a runtime Checkpoint runs inside
// the gate the raft node installs, and its checkpoint carries the marker, so
// the next start is cut exactly at the checkpointed state.
func TestRaftApplied_CheckpointRunsThroughGate(t *testing.T) {
	dir := t.TempDir()
	se := openApplied(t, true, nil, dir)
	gated := 0
	se.SetCheckpointGate(func(fn func()) { gated++; fn() })
	mustPut(t, se, "a", "1")
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 2, Term: 1}, true); err != nil {
		t.Fatal(err)
	}
	if err := se.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if gated != 1 {
		t.Fatalf("checkpoint ran through the gate %d times, want 1", gated)
	}
	// A clean close: the checkpoint written at close carries it as well.
	if err := se.Close(); err != nil {
		t.Fatal(err)
	}
	se = openApplied(t, true, nil, dir)
	defer se.Close()
	wantApplied(t, se, AppliedIndexState{Index: 2, Term: 1})
	wantValue(t, se, "a", "1")
}

func TestRaftApplied_MarkerEncoding(t *testing.T) {
	m := appliedMarker{index: 1 << 40, term: 7, flags: appliedFlagRestoring,
		dead: []versionRange{{1, 5}, {9, 12}}}
	got, ok := decodeAppliedMarker(m.encode())
	if !ok || got.index != m.index || got.term != m.term || got.flags != m.flags || len(got.dead) != 2 || got.dead[1] != m.dead[1] {
		t.Fatalf("round trip: %+v ok=%v", got, ok)
	}
	b := m.encode()
	b[8] ^= 1
	if _, ok := decodeAppliedMarker(b); ok {
		t.Fatal("corrupted marker decoded")
	}
	if _, ok := decodeAppliedMarker([]byte("user value")); ok {
		t.Fatal("arbitrary value decoded as a marker")
	}
}
