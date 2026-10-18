package replication

// state_test.go — durable per-replica recovery state (state.go): a replica
// that was down while its primary restarted must still receive every write
// and delete it missed, and only the recovery worker may return it to SYNC
// (invariant 61).

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeStore is the primary's engine as the catch-up source sees it: the
// current value or tombstone of each key and its write timestamp.
type fakeStore struct {
	mu    sync.Mutex
	rows  map[string]storeRow
	froms []int64 // sinceNs of every catch-up call
	gate  chan struct{}
}

type storeRow struct {
	val  string
	tomb bool
	ts   int64
}

func newFakeStore() *fakeStore { return &fakeStore{rows: map[string]storeRow{}} }

func (s *fakeStore) write(re *ReplicationEngine, seq uint64, key, val string, tomb bool) {
	ts := time.Now().UnixNano()
	s.mu.Lock()
	s.rows[key] = storeRow{val: val, tomb: tomb, ts: ts}
	s.mu.Unlock()
	op := &WriteOperation{SeqNum: seq, Key: key, Value: []byte(val), Timestamp: ts, IsTombstone: tomb, NodeID: "primary"}
	if err := re.OnLocalWrite(op); err != nil {
		panic(err)
	}
}

func (s *fakeStore) catchUp(sinceNs int64, emit func([]*WriteOperation) error) error {
	s.mu.Lock()
	s.froms = append(s.froms, sinceNs)
	gate := s.gate
	var ops []*WriteOperation
	for k, r := range s.rows {
		if r.ts >= sinceNs {
			ops = append(ops, &WriteOperation{Key: k, Value: []byte(r.val), IsTombstone: r.tomb, Timestamp: r.ts})
		}
	}
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if len(ops) == 0 {
		return nil
	}
	return emit(ops)
}

// replicaView applies the ops the replica received, in arrival order.
func replicaView(ft *flakyTransport) map[string]storeRow {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	out := map[string]storeRow{}
	for _, op := range ft.ops {
		out[op.Key] = storeRow{val: string(op.Value), tomb: op.IsTombstone}
	}
	return out
}

func assertReplicaMatches(t *testing.T, s *fakeStore, ft *flakyTransport) {
	t.Helper()
	view := replicaView(ft)
	s.mu.Lock()
	defer s.mu.Unlock()
	bad := 0
	for k, r := range s.rows {
		got, ok := view[k]
		switch {
		case r.tomb && ok && !got.tomb:
			bad++
			if bad < 4 {
				t.Errorf("key %q deleted on the primary but %q on the replica", k, got.val)
			}
		case !r.tomb && (!ok || got.tomb || got.val != r.val):
			bad++
			if bad < 4 {
				t.Errorf("key %q = %q on the primary, replica has %+v (present=%v)", k, r.val, got, ok)
			}
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d keys differ on the replica", bad, len(s.rows))
	}
}

// startPrimary builds a primary with one replica r1 on ft, the durable state
// at statePath and s as catch-up source.  Not started.
func startPrimary(t *testing.T, statePath string, s *fakeStore, ft *flakyTransport, tweak func(*ReplicationConfig)) *ReplicationEngine {
	t.Helper()
	cfg := fastCfg(QuorumConsistency)
	cfg.StatePath = statePath
	cfg.RecoveryProbeInterval = 20 * time.Millisecond
	cfg.RecoveryMaxBackoff = 50 * time.Millisecond
	if tweak != nil {
		tweak(cfg)
	}
	re := NewReplicationEngine("primary", cfg)
	re.SetCatchUpSource(s.catchUp)
	if err := re.AddReplica("r1", "127.0.0.1", 1); err != nil {
		t.Fatal(err)
	}
	if err := re.SetReplicaTransport("r1", ft); err != nil {
		t.Fatal(err)
	}
	return re
}

func readStateFile(t *testing.T, path string) persistedState {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var st persistedState
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// TestRecoveryState_ReplicaDownAcrossPrimaryRestarts: the replica is down
// while the primary acknowledges puts and deletes and restarts (once, then
// twice).  When it comes back it must hold every write and every delete.
// With the state in memory only (StatePath "") the restarted primary took it
// as caught up and it never received them.
func TestRecoveryState_ReplicaDownAcrossPrimaryRestarts(t *testing.T) {
	for _, restarts := range []int{1, 2} {
		t.Run(fmt.Sprintf("restarts=%d", restarts), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "replication_state.json")
			s := newFakeStore()
			ft := &flakyTransport{}
			re := startPrimary(t, path, s, ft, nil)
			re.Start()
			seq := uint64(0)
			next := func() uint64 { seq++; return seq }

			for i := 0; i < 50; i++ {
				s.write(re, next(), fmt.Sprintf("k%d", i), "old", false)
			}
			if !waitPendingEmpty(re, 2*time.Second) {
				t.Fatal("baseline never fully acked")
			}
			ft.setDown(true)
			for i := 0; i < 50; i++ {
				s.write(re, next(), fmt.Sprintf("k%d", i), "", true) // delete
				s.write(re, next(), fmt.Sprintf("n%d", i), fmt.Sprintf("v%d", i), false)
			}
			if !waitReplicaState(re, "r1", "FAILED", 2*time.Second) {
				t.Fatal("replica never FAILED")
			}
			for r := 0; r < restarts; r++ {
				re.Close() // persists the watermark (the crash case is covered below)
				seq = 0    // a restarted coordinator numbers ops from 1 again
				re = startPrimary(t, path, s, ft, nil)
				if st := replicaState(re, "r1"); st != "FAILED" || !re.GetReplicaLag()["r1"].ResyncPending {
					t.Fatalf("restart %d: replica %s resyncPending=%v, want FAILED with a pending catch-up", r+1, st, re.GetReplicaLag()["r1"].ResyncPending)
				}
				re.Start()
				s.write(re, next(), fmt.Sprintf("after-%d", r), "x", false)
				time.Sleep(100 * time.Millisecond) // recovery probes fail: replica still down
			}
			t.Cleanup(func() { re.Close() })

			ft.setDown(false)
			if !waitReplicaState(re, "r1", "SYNC", 5*time.Second) {
				t.Fatalf("replica never recovered (state %s)", replicaState(re, "r1"))
			}
			assertReplicaMatches(t, s, ft)
		})
	}
}

// TestRecoveryState_CrashUsesLastPersistedWatermark: a primary that crashes
// (no Close) restarts from the last state written by the lag monitor; the
// watermark is amortised, so writes after it are re-sent, never lost.
func TestRecoveryState_CrashUsesLastPersistedWatermark(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication_state.json")
	s := newFakeStore()
	ft := &flakyTransport{}
	re := startPrimary(t, path, s, ft, nil)
	re.Start()
	s.write(re, 1, "a", "1", false)
	if !waitPendingEmpty(re, 2*time.Second) {
		t.Fatal("not acked")
	}
	re.updateLag(time.Now()) // one lag-monitor tick: W ≈ now
	w1 := readStateFile(t, path).Replicas["r1"].WatermarkNs
	ft.setDown(true)
	s.write(re, 2, "a", "", true)
	s.write(re, 3, "b", "2", false)
	// Crash: no Close, no further tick.  Stop the background workers so the
	// "dead" engine does not keep writing the file.
	close(re.done)

	re2 := startPrimary(t, path, s, ft, nil)
	t.Cleanup(func() { re2.Close() })
	re2.mu.RLock()
	from := re2.replicaStates["r1"].resyncFromNs
	re2.mu.RUnlock()
	// A lag tick may have persisted again before the "crash"; either way the
	// catch-up must start at or before the first write the replica missed.
	s.mu.Lock()
	missed := s.rows["a"].ts
	s.mu.Unlock()
	if from < w1 || from > missed {
		t.Fatalf("restart resyncs r1 from %d, want within [persisted %d, first missed write %d]", from, w1, missed)
	}
	ft.setDown(false)
	re2.Start()
	if !waitReplicaState(re2, "r1", "SYNC", 5*time.Second) {
		t.Fatal("replica never recovered")
	}
	assertReplicaMatches(t, s, ft)
}

// TestRecoveryState_OnlyRecoveryReturnsToSync: after a restart the replica
// gets no live batch and stays SYNC_PENDING until the catch-up finished and
// every retained op was replayed (invariant 61); the watermark — in memory
// and on disk — stays at the catch-up start until then, so a crash
// mid-catch-up does not mark the replica caught up.
func TestRecoveryState_OnlyRecoveryReturnsToSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication_state.json")
	s := newFakeStore()
	ft := &flakyTransport{}
	re := startPrimary(t, path, s, ft, nil)
	re.Start()
	ft.setDown(true)
	s.write(re, 1, "missed", "m", false)
	if !waitReplicaState(re, "r1", "FAILED", 2*time.Second) {
		t.Fatal("replica never FAILED")
	}
	re.Close()
	w := readStateFile(t, path).Replicas["r1"].WatermarkNs

	ft.setDown(false)
	s.gate = make(chan struct{})
	re2 := startPrimary(t, path, s, ft, nil)
	t.Cleanup(func() { re2.Close() })
	re2.Start()
	if !waitReplicaState(re2, "r1", "SYNC_PENDING", 2*time.Second) {
		t.Fatalf("state %s, want SYNC_PENDING while the catch-up runs", replicaState(re2, "r1"))
	}
	s.write(re2, 1, "live", "l", false) // during the catch-up: retained, not sent live
	time.Sleep(50 * time.Millisecond)
	if n := len(ft.keys()); n != 0 {
		t.Fatalf("replica received %d ops before its catch-up finished", n)
	}
	re2.updateLag(time.Now())
	if got := readStateFile(t, path).Replicas["r1"].WatermarkNs; got > w {
		t.Fatalf("persisted watermark advanced to %d during the catch-up (start %d)", got, w)
	}
	if st := replicaState(re2, "r1"); st != "SYNC_PENDING" {
		t.Fatalf("state %s during the catch-up", st)
	}
	close(s.gate)
	if !waitReplicaState(re2, "r1", "SYNC", 5*time.Second) {
		t.Fatal("replica never returned to SYNC")
	}
	got := ft.keys()
	if got[len(got)-1] != "live" {
		t.Fatalf("delivery order %v: the retained live op must be replayed after the catch-up", got)
	}
	assertReplicaMatches(t, s, ft)
}

// TestRecoveryState_PastTombstoneRetention: a replica whose watermark is older
// than TombstoneRetention is not caught up from the watermark (deletes since
// may have been reaped from the feed): it gets the whole feed, and is flagged
// loudly (FullResyncs, CatchUpGaps, DeleteGap).  Within retention, the
// catch-up starts at the watermark minus catchUpSkew.
func TestRecoveryState_PastTombstoneRetention(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  time.Duration
		full bool
	}{
		{"within", 10 * time.Minute, false},
		{"past", 3 * time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "replication_state.json")
			w := time.Now().Add(-tc.age).UnixNano()
			if err := saveState(path, &persistedState{Version: stateFormatVersion, NodeID: "primary",
				Replicas: map[string]persistedReplica{"r1": {WatermarkNs: w}}}); err != nil {
				t.Fatal(err)
			}
			s := newFakeStore()
			s.rows["ancient"] = storeRow{val: "a", ts: w - int64(time.Hour)}
			ft := &flakyTransport{}
			re := startPrimary(t, path, s, ft, func(c *ReplicationConfig) { c.TombstoneRetention = time.Hour })
			t.Cleanup(func() { re.Close() })
			re.Start()
			if !waitReplicaState(re, "r1", "SYNC", 5*time.Second) {
				t.Fatal("replica never recovered")
			}
			s.mu.Lock()
			from := s.froms[0]
			s.mu.Unlock()
			m := re.GetMetrics()
			lag := re.GetReplicaLag()["r1"]
			if tc.full {
				if from != 0 || m.FullResyncs.Load() != 1 || m.CatchUpGaps.Load() != 1 || !lag.DeleteGap {
					t.Fatalf("from=%d fullResyncs=%d gaps=%d deleteGap=%v, want a flagged full resync",
						from, m.FullResyncs.Load(), m.CatchUpGaps.Load(), lag.DeleteGap)
				}
				assertReplicaMatches(t, s, ft) // the whole feed, "ancient" included
			} else {
				if from != w-int64(catchUpSkew) || m.FullResyncs.Load() != 0 || lag.DeleteGap {
					t.Fatalf("from=%d (want %d) fullResyncs=%d deleteGap=%v", from, w-int64(catchUpSkew), m.FullResyncs.Load(), lag.DeleteGap)
				}
			}
		})
	}
}

// TestRecoveryState_CorruptFileResyncsEverything: an unreadable state file
// means unknown progress — every replica is caught up from the whole feed.
func TestRecoveryState_CorruptFileResyncsEverything(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication_state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := newFakeStore()
	s.rows["old"] = storeRow{val: "o", ts: 5}
	ft := &flakyTransport{}
	re := startPrimary(t, path, s, ft, nil)
	t.Cleanup(func() { re.Close() })
	re.Start()
	if !waitReplicaState(re, "r1", "SYNC", 5*time.Second) {
		t.Fatal("replica never recovered")
	}
	assertReplicaMatches(t, s, ft)
	if st := readStateFile(t, path); st.Version != stateFormatVersion {
		t.Fatalf("state file not rewritten: %+v", st)
	}
}

// TestRecoveryState_FirstStartAndAmortisedPersist: with no state file the
// replica starts SYNC (nothing to catch up), the file is written at once,
// and steady-state writes never write it — only lag-monitor ticks do.
func TestRecoveryState_FirstStartAndAmortisedPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replication_state.json")
	s := newFakeStore()
	ft := &flakyTransport{}
	re := startPrimary(t, path, s, ft, nil)
	t.Cleanup(func() { re.Close() })
	if st := replicaState(re, "r1"); st != "SYNC" {
		t.Fatalf("first start: state %s, want SYNC", st)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not written by AddReplica: %v", err)
	}
	base := re.GetMetrics().StatePersists.Load()
	re.Start()
	for i := 1; i <= 2000; i++ {
		s.write(re, uint64(i), fmt.Sprintf("k%d", i%100), "v", false)
	}
	if !waitPendingEmpty(re, 5*time.Second) {
		t.Fatal("writes not acked")
	}
	if n := re.GetMetrics().StatePersists.Load() - base; n > 2 {
		t.Fatalf("%d state writes for 2000 ops: persistence must be per lag tick, not per op", n)
	}
}

func waitPendingEmpty(re *ReplicationEngine, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		re.mu.RLock()
		n := len(re.pendingWrites)
		re.mu.RUnlock()
		if n == 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

type nopTransport struct{}

func (nopTransport) Send([]*WriteOperation) error { return nil }
func (nopTransport) Close()                       {}

// BenchmarkQuorumWrites_StatePersistence measures steady-state quorum writes
// (OnLocalWrite + WaitForReplication, two replicas on an instant transport,
// 64 writers) with the durable recovery state off and on.  The state is
// written by the lag monitor once per second, never per op, so the two arms
// should be equal within noise.
func BenchmarkQuorumWrites_StatePersistence(b *testing.B) {
	for _, arm := range []string{"memory-only", "persisted"} {
		b.Run(arm, func(b *testing.B) {
			cfg := fastCfg(QuorumConsistency)
			cfg.BatchSize = 100
			cfg.FlushIntervalMs = 1
			if arm == "persisted" {
				cfg.StatePath = filepath.Join(b.TempDir(), "replication_state.json")
			}
			re := NewReplicationEngine("primary", cfg)
			for _, id := range []string{"r1", "r2"} {
				if err := re.AddReplica(id, "127.0.0.1", 1); err != nil {
					b.Fatal(err)
				}
				if err := re.SetReplicaTransport(id, nopTransport{}); err != nil {
					b.Fatal(err)
				}
			}
			re.Start()
			defer re.Close()
			var seq uint64
			var mu sync.Mutex
			b.SetParallelism(8) // 8 × GOMAXPROCS writers
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					mu.Lock()
					seq++
					s := seq
					mu.Unlock()
					op := &WriteOperation{SeqNum: s, Key: "k", Value: []byte("v"), Timestamp: time.Now().UnixNano()}
					if err := re.OnLocalWrite(op); err != nil {
						b.Fatal(err)
					}
					if err := re.WaitForReplication(s, 2, 10000); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(re.GetMetrics().StatePersists.Load()), "state-writes")
		})
	}
}
