package storage

// raft_clock_test.go — the raft clock and the version tokens derived from it
// (raft_clock.go, CLAUDE.md invariant 66).

import (
	"errors"
	"testing"
	"time"
)

// TestRaftClock_NextApplyTime: E = max(Ts, E+1) — strictly increasing in log
// order whatever the leaders' clocks; the floor is the first stamped E;
// legacy commands (Ts = 0) get the legacy stamp before the clock starts and
// E+1 after, with the local clock as "now".
func TestRaftClock_NextApplyTime(t *testing.T) {
	se := openApplied(t, true, nil, t.TempDir())
	defer se.Close()

	at := se.NextRaftApplyTime(0)
	if at.StampUs != legacyStampUs || at.NowUs < time.Now().Add(-time.Minute).UnixMicro() {
		t.Fatalf("legacy before start: %+v", at)
	}
	if l, f := se.RaftClock(); l != 0 || f != 0 {
		t.Fatalf("a legacy command started the clock: %d %d", l, f)
	}
	steps := []struct{ ts, want int64 }{
		{1_000_000, 1_000_000}, // starts the clock; floor
		{1_000_500, 1_000_500}, // leader clock moves on
		{900_000, 1_000_501},   // new leader's clock is behind: clamped
		{1_000_501, 1_000_502}, // equal: still strictly increasing
		{2_000_000, 2_000_000},
	}
	for i, s := range steps {
		at := se.NextRaftApplyTime(s.ts)
		if at.StampUs != s.want || at.NowUs != s.want {
			t.Fatalf("step %d: ts=%d → %+v, want %d", i, s.ts, at, s.want)
		}
	}
	if l, f := se.RaftClock(); l != 2_000_000 || f != 1_000_000 {
		t.Fatalf("clock = %d floor = %d", l, f)
	}
	if at := se.NextRaftApplyTime(0); at.StampUs != 2_000_001 {
		t.Fatalf("legacy after start: %+v, want stamp 2000001", at)
	}
}

// TestRaftClock_VersionToken: below the floor → LegacyVersion, absent /
// tombstoned / expired at the decision time → 0, else the stamp; without a
// floor (standalone, replicated, pre-clock raft) the raw stamp.
func TestRaftClock_VersionToken(t *testing.T) {
	live := IndexEntry{WriteTimestampUs: 5_000}
	ttl := IndexEntry{WriteTimestampUs: 5_000, Flags: FlagHasTTL, TTLExpiryUs: 9_000}
	dead := IndexEntry{WriteTimestampUs: 5_000, Flags: FlagTombstone}
	for _, c := range []struct {
		name       string
		e          IndexEntry
		now, floor int64
		want       uint64
	}{
		{"live", live, 1, 0, 5_000},
		{"live above floor", live, 1, 4_000, 5_000},
		{"legacy below floor", live, 1, 6_000, LegacyVersion},
		{"tombstone", dead, 1, 0, 0},
		{"ttl live at now", ttl, 8_999, 0, 5_000},
		{"ttl expired at now", ttl, 9_000, 0, 0},
		{"zero stamp", IndexEntry{}, 1, 0, LegacyVersion},
	} {
		if got := versionToken(c.e, c.now, c.floor); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}

// TestRaftClock_ApplyTimeDrivesWrites: under SetApplyTime every write path
// stamps the command's time and converts TTLs against its NowUs, and every
// decision inside the command (TXN guard, SETNX, INCR) is taken at NowUs —
// not the local clock.
func TestRaftClock_ApplyTimeDrivesWrites(t *testing.T) {
	se := openApplied(t, true, nil, t.TempDir())
	defer se.Close()
	base := time.Now().Add(-time.Hour).UnixMicro() // an hour behind the local clock

	apply := func(ts int64, fn func()) ApplyTime {
		at := se.NextRaftApplyTime(ts)
		se.SetApplyTime(at)
		defer se.ClearApplyTime()
		fn()
		return at
	}
	at1 := apply(base, func() {
		if err := se.Put("k", []byte("v"), 10); err != nil {
			t.Fatal(err)
		}
		if _, err := se.Increment("ctr", 1, -1); err != nil {
			t.Fatal(err)
		}
		if errs := se.MultiPut([]MultiPutRequest{{Key: "m", Value: []byte("x"), TTL: -1}}); errs[0] != nil {
			t.Fatal(errs[0])
		}
	})
	for _, k := range []string{"k", "ctr", "m"} {
		e, _, _ := se.index.get(k)
		if e.WriteTimestampUs != at1.StampUs {
			t.Fatalf("%s stamped %d, want the command's %d", k, e.WriteTimestampUs, at1.StampUs)
		}
	}
	if e, _, _ := se.index.get("k"); e.TTLExpiryUs != base+10_000_000 {
		t.Fatalf("TTL deadline %d, want the command's time + 10 s = %d", e.TTLExpiryUs, base+10_000_000)
	}
	// By the local clock "k" expired long ago; at the next command's time it
	// is still live — so the guard holds, SETNX finds it, and nothing
	// tombstoned it meanwhile (expiry cutoff = the raft clock).
	if v := se.KeyVersion("k"); v != 0 {
		t.Fatalf("a read by the local clock sees k live (version %d)", v)
	}
	var txnErr error
	var nx SetNXResult
	apply(base+1_000_000, func() {
		txn := se.BeginTxn()
		txn.SetIf("k", []byte("v2"), 10, uint64(at1.StampUs))
		txnErr = txn.Commit()
		nx, _ = se.SetIfNotExists("ctr", []byte("9"), -1)
	})
	if txnErr != nil {
		t.Fatalf("guard judged by the local clock: %v", txnErr)
	}
	if nx != SetNXExists {
		t.Fatalf("SETNX on a live key: %v", nx)
	}
	// Past the deadline at the command's time: absent, so "expect 0" holds.
	apply(base+20_000_000, func() {
		txn := se.BeginTxn()
		txn.SetIf("k", []byte("v3"), -1, 0)
		txnErr = txn.Commit()
	})
	if txnErr != nil {
		t.Fatalf("expect-absent on an expired key: %v", txnErr)
	}
	if v, err := se.Get("k"); err != nil || string(v) != "v3" {
		t.Fatalf("k = %q (%v)", v, err)
	}
}

// TestRaftClock_MultiPutAtPerRequestTimes: each request of a coalesced run is
// stamped and TTL'd as of its own command.
func TestRaftClock_MultiPutAtPerRequestTimes(t *testing.T) {
	se := openApplied(t, true, nil, t.TempDir())
	defer se.Close()
	t1, t2 := se.NextRaftApplyTime(1_000_000_000), se.NextRaftApplyTime(1_000_000_500)
	errs := se.MultiPutAt([]MultiPutRequest{
		{Key: "a", Value: []byte("1"), TTL: 5},
		{Key: "b", Value: []byte("2"), TTL: -1},
	}, []ApplyTime{t1, t2})
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	a, _, _ := se.index.get("a")
	b, _, _ := se.index.get("b")
	if a.WriteTimestampUs != t1.StampUs || b.WriteTimestampUs != t2.StampUs {
		t.Fatalf("stamps %d %d, want %d %d", a.WriteTimestampUs, b.WriteTimestampUs, t1.StampUs, t2.StampUs)
	}
	if a.TTLExpiryUs != t1.NowUs+5_000_000 {
		t.Fatalf("a deadline %d", a.TTLExpiryUs)
	}
}

// TestRaftClock_SurvivesRestartAndCrash: the clock is recorded in the
// applied-index marker, comes back on open (clean or crash), and replay
// installs records in WAL order even when a later record carries a smaller
// stamp (a legacy write after a stamped one).
func TestRaftClock_SurvivesRestartAndCrash(t *testing.T) {
	dir := t.TempDir()
	se := openApplied(t, true, nil, dir)
	at := se.NextRaftApplyTime(5_000_000_000)
	se.SetApplyTime(at)
	mustPut(t, se, "k", "stamped")
	se.ClearApplyTime()
	se.SetApplyTime(se.NextRaftApplyTime(0)) // legacy: E+1
	se.ClearApplyTime()
	se.SetApplyTime(ApplyTime{StampUs: legacyStampUs, NowUs: time.Now().UnixMicro()})
	mustPut(t, se, "k", "later-smaller-stamp")
	se.ClearApplyTime()
	if err := se.SetRaftAppliedState(AppliedIndexState{Index: 3, Term: 1}, true); err != nil {
		t.Fatal(err)
	}
	wantL, wantF := se.RaftClock()
	abandonEngineForCrashTest(se) // crash: no checkpoint, full replay

	se2 := openApplied(t, true, nil, dir)
	if l, f := se2.RaftClock(); l != wantL || f != wantF {
		t.Fatalf("after crash: clock %d floor %d, want %d %d", l, f, wantL, wantF)
	}
	wantValue(t, se2, "k", "later-smaller-stamp")
	if err := se2.Close(); err != nil {
		t.Fatal(err)
	}
	se3 := openApplied(t, true, nil, dir) // clean: from the checkpoint
	defer se3.Close()
	if l, f := se3.RaftClock(); l != wantL || f != wantF {
		t.Fatalf("after clean restart: clock %d floor %d, want %d %d", l, f, wantL, wantF)
	}
	wantValue(t, se3, "k", "later-smaller-stamp")
}

// TestRaftClock_MarkerTrailer: the clock trailer round-trips, and a marker
// without it still decodes (clock 0).
func TestRaftClock_MarkerTrailer(t *testing.T) {
	m := appliedMarker{index: 9, term: 2, dead: []versionRange{{1, 2}}, clock: 1 << 50, floor: 1 << 49}
	got, ok := decodeAppliedMarker(m.encode())
	if !ok || got.clock != m.clock || got.floor != m.floor || got.index != 9 || len(got.dead) != 1 {
		t.Fatalf("round trip: %+v ok=%v", got, ok)
	}
	m.clock, m.floor = 0, 0
	got, ok = decodeAppliedMarker(m.encode())
	if !ok || got.clock != 0 || got.index != 9 {
		t.Fatalf("no trailer: %+v ok=%v", got, ok)
	}
}

// TestRaftClock_ExpiryNotTombstonedPastClock: once the raft clock runs, a key
// expired by the local clock but not at the clock is hidden from reads but
// not tombstoned (Get, TTL scan), and a write that replaced an expired entry
// is never tombstoned by a scan that saw the old one.
func TestRaftClock_ExpiryNotTombstonedPastClock(t *testing.T) {
	se := openApplied(t, true, nil, t.TempDir())
	defer se.Close()
	clock := time.Now().Add(-time.Minute).UnixMicro()
	se.SetApplyTime(se.NextRaftApplyTime(clock))
	if err := se.Put("k", []byte("v"), 1); err != nil { // expires at clock + 1 s
		t.Fatal(err)
	}
	se.ClearApplyTime()
	if _, err := se.Get("k"); !errors.Is(err, ErrKeyExpired) {
		t.Fatalf("Get by the local clock: %v, want ErrKeyExpired", err)
	}
	if e, _, _ := se.index.get("k"); e.IsTombstone() {
		t.Fatal("tombstoned past the raft clock")
	}
	if n := len(se.index.expiredTTL(se.expiryCutoffUs())); n != 0 {
		t.Fatalf("TTL scan at the cutoff found %d keys", n)
	}
	// markExpired re-checks under the lock.
	if err := se.Put("k", []byte("fresh"), -1); err != nil {
		t.Fatal(err)
	}
	if se.index.markExpired("k", time.Now().UnixMicro(), time.Now().UnixMicro()) {
		t.Fatal("markExpired tombstoned a live immortal write")
	}
	wantValue(t, se, "k", "fresh")
}
