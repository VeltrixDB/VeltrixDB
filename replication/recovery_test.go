package replication

// recovery_test.go — FAILED-replica recovery, retention, lag and watermarks.

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// flakyTransport fails every Send/Ping while down and records delivered ops.
type flakyTransport struct {
	mu    sync.Mutex
	down  bool
	ops   []*WriteOperation
	pings int
}

func (f *flakyTransport) setDown(d bool) { f.mu.Lock(); f.down = d; f.mu.Unlock() }

func (f *flakyTransport) Send(ops []*WriteOperation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errors.New("connection refused")
	}
	f.ops = append(f.ops, ops...)
	return nil
}

func (f *flakyTransport) Ping() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pings++
	if f.down {
		return errors.New("connection refused")
	}
	return nil
}

func (f *flakyTransport) Close() {}

func (f *flakyTransport) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.ops))
	for i, op := range f.ops {
		out[i] = op.Key
	}
	return out
}

func replicaState(re *ReplicationEngine, id string) string {
	return re.GetReplicaLag()[id].State
}

func waitReplicaState(re *ReplicationEngine, id, want string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if replicaState(re, id) == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func newFlakyEngine(t *testing.T, level ConsistencyLevel) (*ReplicationEngine, *flakyTransport) {
	t.Helper()
	re := NewReplicationEngine("primary", fastCfg(level))
	if err := re.AddReplica("r1", "127.0.0.1", 22000); err != nil {
		t.Fatal(err)
	}
	ft := &flakyTransport{}
	if err := re.SetReplicaTransport("r1", ft); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { re.Close() })
	return re, ft
}

// TestReplication_FailedReplicaRecovers: a replica marked FAILED by a failed
// send is re-probed by the running engine, receives every write it missed
// (including writes made while it was FAILED, which live batches skip) and
// returns to SYNC.  Before recovery existed it stayed FAILED forever.
func TestReplication_FailedReplicaRecovers(t *testing.T) {
	re, ft := newFlakyEngine(t, EventualConsistency)
	re.Start()

	ft.setDown(true)
	if err := re.OnLocalWrite(newOp(1, "k1", []byte("v1"))); err != nil {
		t.Fatal(err)
	}
	if !waitReplicaState(re, "r1", "FAILED", 2*time.Second) {
		t.Fatalf("replica not FAILED after a failed send (state %s)", replicaState(re, "r1"))
	}
	// Written while FAILED: not sent live, must be replayed on recovery.
	if err := re.OnLocalWrite(newOp(2, "k2", []byte("v2"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	ft.setDown(false)
	if !waitReplicaState(re, "r1", "SYNC", 5*time.Second) {
		t.Fatalf("FAILED replica never recovered (state %s)", replicaState(re, "r1"))
	}
	got := ft.keys()
	if len(got) < 2 || got[0] != "k1" || got[1] != "k2" {
		t.Fatalf("recovered replica received %v, want [k1 k2] in order", got)
	}
	re.mu.RLock()
	pending := len(re.pendingWrites)
	re.mu.RUnlock()
	if pending != 0 {
		t.Errorf("retention buffer holds %d ops after full recovery, want 0", pending)
	}
	if n := re.GetMetrics().ReplicaRecoveries.Load(); n != 1 {
		t.Errorf("ReplicaRecoveries = %d, want 1", n)
	}
}

// TestReplication_RecoveryBackoff: each failed probe doubles the reconnect
// delay up to RecoveryMaxBackoff; probes are not sent before it elapses.
func TestReplication_RecoveryBackoff(t *testing.T) {
	re, ft := newFlakyEngine(t, EventualConsistency)
	re.config.RecoveryProbeInterval = 100 * time.Millisecond
	re.config.RecoveryMaxBackoff = 350 * time.Millisecond
	ft.setDown(true)
	re.replicateBatch([]*WriteOperation{newOp(1, "k", nil)})
	time.Sleep(20 * time.Millisecond) // eventual: the send runs in a goroutine
	if s := replicaState(re, "r1"); s != "FAILED" {
		t.Fatalf("state %s, want FAILED", s)
	}

	now := time.Now()
	re.recoverFailedReplicas(now) // backoff not elapsed yet
	if ft.pings != 0 {
		t.Fatalf("probed %d times before the first backoff elapsed", ft.pings)
	}
	var backoffs []time.Duration
	for i := 0; i < 4; i++ {
		now = now.Add(time.Hour)
		re.recoverFailedReplicas(now)
		re.mu.RLock()
		backoffs = append(backoffs, re.replicaStates["r1"].backoff)
		re.mu.RUnlock()
	}
	want := []time.Duration{200, 350, 350, 350}
	for i := range want {
		if backoffs[i] != want[i]*time.Millisecond {
			t.Fatalf("backoffs %v, want %v ms", backoffs, want)
		}
	}
	if ft.pings != 4 {
		t.Errorf("pings = %d, want 4", ft.pings)
	}
	if s := replicaState(re, "r1"); s != "FAILED" {
		t.Errorf("state %s after failed probes, want FAILED", s)
	}
}

// TestReplication_RecoveryUsesCatchUpSourceAfterEviction: when the retention
// buffer overflows while a replica is FAILED, recovery first streams the
// catch-up source from (at or before) the oldest dropped write, goes through
// SYNC_PENDING, replays what is still retained and ends in SYNC.
func TestReplication_RecoveryUsesCatchUpSourceAfterEviction(t *testing.T) {
	re, ft := newFlakyEngine(t, EventualConsistency)
	re.config.MaxRetainedOps = 5
	ft.setDown(true)
	first := newOp(1, "k1", nil)
	re.OnLocalWrite(first)
	re.replicateBatch([]*WriteOperation{first})
	time.Sleep(20 * time.Millisecond)
	for i := uint64(2); i <= 20; i++ {
		re.OnLocalWrite(newOp(i, "k", nil))
	}
	re.mu.RLock()
	r := re.replicaStates["r1"]
	needs, from := r.needsResync, r.resyncFromNs
	re.mu.RUnlock()
	if !needs || from > first.Timestamp {
		t.Fatalf("needsResync=%v from=%d, want true and <= %d", needs, from, first.Timestamp)
	}

	var sawState string
	var since int64
	re.SetCatchUpSource(func(s int64, emit func([]*WriteOperation) error) error {
		since = s
		sawState = replicaState(re, "r1")
		return emit([]*WriteOperation{{Key: "from-feed", Timestamp: time.Now().UnixNano()}})
	})
	ft.setDown(false)
	re.recoverFailedReplicas(time.Now().Add(time.Hour))

	if s := replicaState(re, "r1"); s != "SYNC" {
		t.Fatalf("state %s after recovery, want SYNC", s)
	}
	if sawState != "SYNC_PENDING" {
		t.Errorf("state during catch-up = %q, want SYNC_PENDING", sawState)
	}
	if since > first.Timestamp {
		t.Errorf("catch-up started at %d, after the oldest dropped write %d", since, first.Timestamp)
	}
	got := ft.keys()
	if len(got) == 0 || got[0] != "from-feed" {
		t.Fatalf("catch-up page not delivered first: %v", got)
	}
	if len(got) != 1+5 {
		t.Errorf("delivered %d ops, want 1 feed op + 5 retained", len(got))
	}
}

// TestReplication_LagNanosecondsReported: the lag monitor publishes the age of
// the oldest un-acked write per replica and as the ReplicaLagNs maximum
// (veltrixdb_replication_lag_nanoseconds), which used to be always 0.
func TestReplication_LagNanosecondsReported(t *testing.T) {
	re, ft := newFlakyEngine(t, EventualConsistency)
	ft.setDown(true)
	re.Start()
	if err := re.OnLocalWrite(newOp(1, "k", []byte("v"))); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && re.GetMetrics().ReplicaLagNs.Load() < int64(500*time.Millisecond) {
		time.Sleep(50 * time.Millisecond)
	}
	if lag := re.GetMetrics().ReplicaLagNs.Load(); lag < int64(500*time.Millisecond) {
		t.Fatalf("ReplicaLagNs = %d, want >= 500ms for a write un-acked for over a second", lag)
	}
	if lag := re.GetReplicaLag()["r1"].LagNs; lag <= 0 {
		t.Errorf("per-replica LagNs = %d, want > 0", lag)
	}
	if n := re.GetMetrics().ReplicasFailed.Load(); n != 1 {
		t.Errorf("ReplicasFailed = %d, want 1", n)
	}

	// Caught up → lag back to 0.
	ft.setDown(false)
	if !waitReplicaState(re, "r1", "SYNC", 5*time.Second) {
		t.Fatal("replica did not recover")
	}
	re.updateLag(time.Now())
	if lag := re.GetMetrics().ReplicaLagNs.Load(); lag != 0 {
		t.Errorf("ReplicaLagNs = %d after catch-up, want 0", lag)
	}
}

// TestReplication_WaitForReplicationPerOp: an ack of a later sequence number
// must not satisfy a wait for an earlier one (concurrent writers can enqueue
// seq N+1 before seq N).  The old check used LastAckSeqNum >= seq.
func TestReplication_WaitForReplicationPerOp(t *testing.T) {
	re, _ := newFlakyEngine(t, QuorumConsistency)
	op5, op6 := newOp(5, "a", nil), newOp(6, "b", nil)
	re.OnLocalWrite(op5)
	re.OnLocalWrite(op6)
	if err := re.replicateBatch([]*WriteOperation{op6}); err != nil {
		t.Fatal(err)
	}
	if err := re.WaitForReplication(6, 2, 100); err != nil {
		t.Fatalf("seq 6 was acked: %v", err)
	}
	if err := re.WaitForReplication(5, 2, 30); err == nil {
		t.Fatal("WaitForReplication(5) succeeded although only seq 6 was acked")
	}
}

// TestReplication_EventualClearsRetention: in eventual mode acked ops leave
// the retention buffer (they used to stay in pendingWrites forever).
func TestReplication_EventualClearsRetention(t *testing.T) {
	re, _ := newFlakyEngine(t, EventualConsistency)
	op := newOp(1, "k", nil)
	re.OnLocalWrite(op)
	re.replicateBatch([]*WriteOperation{op})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		re.mu.RLock()
		n := len(re.pendingWrites)
		re.mu.RUnlock()
		if n == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("acked op still retained in pendingWrites")
}

// TestReplication_WatermarkObserver: watermarks trail the oldest un-acked
// write of a replica, and advance to ~now (minus the margin) once caught up.
func TestReplication_WatermarkObserver(t *testing.T) {
	re, ft := newFlakyEngine(t, EventualConsistency)
	marks := map[string]int64{}
	re.SetWatermarkObserver(func(id string, us int64) { marks[id] = us })

	ft.setDown(true)
	op := newOp(1, "k", nil)
	re.OnLocalWrite(op)
	re.replicateBatch([]*WriteOperation{op})
	time.Sleep(20 * time.Millisecond)
	re.updateLag(time.Now())
	if wm := marks["r1"]; wm >= op.Timestamp/1000 {
		t.Fatalf("watermark %d not below the un-acked write %d", wm, op.Timestamp/1000)
	}

	ft.setDown(false)
	re.recoverFailedReplicas(time.Now().Add(time.Hour))
	now := time.Now()
	re.updateLag(now)
	want := (now.UnixNano() - int64(watermarkMargin)) / 1000
	if wm := marks["r1"]; wm != want {
		t.Fatalf("caught-up watermark %d, want %d (now - margin)", wm, want)
	}
}

// TestReplicationClient_Ping: Ping round-trips an empty batch through a real
// ReplicationServer without applying anything.
func TestReplicationClient_Ping(t *testing.T) {
	applied := 0
	srv, err := NewReplicationServer("127.0.0.1:0", func(*WriteOperation) error { applied++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	go srv.ListenAndServe()
	c := NewReplicationClient(srv.Addr())
	err = c.Ping()
	c.Close() // Stop waits for open connections
	if err != nil {
		srv.Stop()
		t.Fatalf("Ping: %v", err)
	}
	srv.Stop()
	if applied != 0 {
		t.Errorf("Ping applied %d ops", applied)
	}
	c2 := NewReplicationClient(srv.Addr())
	defer c2.Close()
	if err := c2.Ping(); err == nil {
		t.Error("Ping succeeded against a stopped server")
	}
}

// TestReplication_WriteQueueFullStillReplicates: an op that finds the write
// queue full was already written locally, so it must still reach replicas.
// Before the fix OnLocalWrite dropped it from the retention buffer and
// returned "write queue full": every replica missed that write forever.
func TestReplication_WriteQueueFullStillReplicates(t *testing.T) {
	re, ft := newFlakyEngine(t, EventualConsistency)
	// Not started: nothing drains the queue, so it can be filled exactly.
	for i := 0; i < cap(re.writeQueue); i++ {
		re.writeQueue <- newOp(uint64(100000+i), "filler", nil)
	}
	if err := re.OnLocalWrite(newOp(1, "overflowed", []byte("v"))); err != nil {
		t.Fatalf("OnLocalWrite on a full queue: %v (the local write already happened)", err)
	}
	if st := replicaState(re, "r1"); st != "LAG" {
		t.Fatalf("replica state %s after an overflow, want LAG", st)
	}
	if n := re.metrics.WriteQueueOverflows.Load(); n != 1 {
		t.Fatalf("WriteQueueOverflows = %d, want 1", n)
	}
	select {
	case <-re.aeKick:
	default:
		t.Fatal("overflow did not request an anti-entropy run")
	}
	re.runAntiEntropy()
	found := false
	for _, k := range ft.keys() {
		if k == "overflowed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("overflowed op never reached the replica (got %d ops)", len(ft.keys()))
	}
}
