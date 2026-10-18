package storage

// raft_applied_downgrade_test.go — a raft node downgraded to a build without
// applied-index markers (v1.16) and upgraded again must not trust the marker
// copy the old build rewrote (raft_applied.go, isAppliedMarker).

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// oldBuildRewrite emulates what v1.16 does to a new-build wal.log: replay
// every record as a key (the marker included), apply its own writes, and
// checkpoint — every live key rewritten at the checkpoint version. kvSep
// controls how the marker key comes out: header-only (its value moved to the
// VLog, as v1.16's KV-separation replay does) or with the value inline.
// extra are the old build's own writes. crashed skips the checkpoint: the
// old build's writes are appended to the new build's file as they are.
func oldBuildRewrite(t *testing.T, dir string, kvSep, crashed bool, extra ...WALEntry) {
	t.Helper()
	walPath := filepath.Join(dir, "wal.log")
	ents, err := replayWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	var maxVer uint64
	for _, e := range ents {
		if e.version > maxVer {
			maxVer = e.version
		}
	}
	var buf []byte
	if crashed {
		buf, err = os.ReadFile(walPath)
		if err != nil {
			t.Fatal(err)
		}
		for i := range extra {
			maxVer++
			extra[i].Version = maxVer
			buf = appendWALRecordFor(buf, &extra[i], false)
		}
		if err := os.WriteFile(walPath, buf, 0644); err != nil {
			t.Fatal(err)
		}
		return
	}
	ckptVer := maxVer + uint64(len(extra)) // the old build's writes bumped it
	for _, e := range ents {
		we := WALEntry{Timestamp: e.timestampNs, Key: e.key, KeyLen: uint32(len(e.key)), Version: ckptVer,
			ValueLen: e.valueLen, Checksum: e.crc, VLogOffset: e.vlogOffset, Packed: e.packed,
			DiskValueLen: e.diskLen, XformFlags: e.xflags, TTLExpiryUs: e.ttlExpiryUs, IsTombstone: e.isTombstone}
		if e.key == appliedMarkerKey && kvSep {
			// v1.16 re-appended the inline value to its VLog: header-only now.
			we.VLogOffset = 1 << 20
			we.DiskValueLen = uint32(len(e.value))
		} else if e.vlogOffset == 0 {
			we.Value = e.value
		}
		buf = appendWALRecordFor(buf, &we, false)
	}
	for i := range extra {
		extra[i].Version = ckptVer
		buf = appendWALRecordFor(buf, &extra[i], false)
	}
	if err := os.WriteFile(walPath, buf, 0644); err != nil {
		t.Fatal(err)
	}
}

func inlinePut(k, v string) WALEntry {
	return WALEntry{Timestamp: time.Now().UnixNano(), Key: k, KeyLen: uint32(len(k)), Value: []byte(v),
		ValueLen: uint32(len(v)), Checksum: computeCRC32C([]byte(v))}
}

func TestRaftApplied_StaleMarkerAfterDowngradeIsNotTrusted(t *testing.T) {
	for _, tc := range []struct {
		name        string
		kvSep       bool
		crashed     bool
		oldWrites   bool
		wantApplied bool   // the new build trusts a marker
		wantCtr     string // ctr after the re-upgrade
	}{
		// v1.16 applied INCRs and stopped cleanly: its checkpoint rewrote the
		// marker; trusting it would re-apply those INCRs. No applied state:
		// raft resets / restores and applies the log once.
		{"clean-kvsep", true, false, true, false, "20"},
		{"clean-inline", false, false, true, false, "20"},
		// v1.16 crashed: the new build's marker record is intact and its
		// version still matches; v1.16's writes are above it and are cut.
		{"crashed", true, true, true, true, "10"},
		// v1.16 wrote nothing and stopped cleanly: the rewritten copy is at
		// the marker's own version and still describes the engine exactly.
		{"clean-no-writes", false, false, false, true, "10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			se := openApplied(t, true, nil, dir)
			mustPut(t, se, "ctr", "10")
			if err := se.SetRaftAppliedState(AppliedIndexState{Index: 10, Term: 1}, true); err != nil {
				t.Fatal(err)
			}
			if err := se.Close(); err != nil {
				t.Fatal(err)
			}

			var extra []WALEntry
			if tc.oldWrites {
				extra = append(extra, inlinePut("ctr", "20"))
			}
			oldBuildRewrite(t, dir, tc.kvSep, tc.crashed, extra...)

			se = openApplied(t, true, nil, dir)
			defer se.Close()
			st, ok := se.RaftAppliedState()
			if ok != tc.wantApplied {
				t.Fatalf("RaftAppliedState = %+v ok=%v, want ok=%v", st, ok, tc.wantApplied)
			}
			if ok && st.Index != 10 {
				t.Fatalf("applied index %d, want 10", st.Index)
			}
			wantValue(t, se, "ctr", tc.wantCtr)
			if _, err := se.Get(appliedMarkerKey); err == nil {
				t.Fatal("the marker key is readable as a key after the re-upgrade")
			}
			for _, k := range se.ScanKeys() {
				if k == appliedMarkerKey {
					t.Fatal("the marker key is in the keyspace after the re-upgrade")
				}
			}

			// The new build's own checkpoints keep a marker trusted.
			if err := se.SetRaftAppliedState(AppliedIndexState{Index: 12, Term: 2}, false); err != nil {
				t.Fatal(err)
			}
			if err := se.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			abandonEngineForCrashTest(se)
			se2 := openApplied(t, true, nil, dir)
			wantApplied(t, se2, AppliedIndexState{Index: 12, Term: 2})
			if err := se2.Close(); err != nil {
				t.Fatal(err)
			}
			se3 := openApplied(t, true, nil, dir)
			wantApplied(t, se3, AppliedIndexState{Index: 12, Term: 2})
			se3.Close()
		})
	}
}

// TestRaftApplied_RestorerSkipsMarkerKey: a v1.16 snapshot carries the marker
// as a key; restoring it must not put the reserved key in the keyspace.
func TestRaftApplied_RestorerSkipsMarkerKey(t *testing.T) {
	se := openApplied(t, true, nil, t.TempDir())
	defer se.Close()
	r := se.NewRestorer()
	if err := r.Put(SnapshotKV{Key: appliedMarkerKey, Value: []byte("stale")}); err != nil {
		t.Fatal(err)
	}
	if err := r.Put(SnapshotKV{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	wantValue(t, se, "k", "v")
	if _, err := se.Get(appliedMarkerKey); err == nil {
		t.Fatal("restored the marker key")
	}
}
