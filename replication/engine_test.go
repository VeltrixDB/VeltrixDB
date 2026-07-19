package replication

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── Mock replica store ────────────────────────────────────────────────────────

// mockStore records every WriteOperation applied to it.
type mockStore struct {
	mu  sync.Mutex
	ops []*WriteOperation
}

func (s *mockStore) apply(op *WriteOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops = append(s.ops, op)
	return nil
}

func (s *mockStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ops)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// newOp creates a minimal WriteOperation with the given sequence number.
func newOp(seqNum uint64, key string, value []byte) *WriteOperation {
	return &WriteOperation{
		SeqNum:    seqNum,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Version:   1,
		NodeID:    "primary",
	}
}

// fastCfg returns a ReplicationConfig with tight flush intervals for tests.
func fastCfg(level ConsistencyLevel) *ReplicationConfig {
	return &ReplicationConfig{
		ConsistencyLevel:    level,
		ReplicationMode:     AsyncReplication,
		ReplicationFactor:   3,
		BatchSize:           10,
		FlushIntervalMs:     5,
		MaxLagBytesToSync:   1024 * 1024,
		VectorClockEnabled:  true,
		AntiEntropyInterval: 30 * time.Second, // avoid spurious anti-entropy noise in tests
		ReadRepairEnabled:   false,
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestReplication_AsyncWrite: OnLocalWrite returns immediately without waiting.
// We measure that the call completes in under a reasonable threshold.
func TestReplication_AsyncWrite(t *testing.T) {
	t.Parallel()

	re := NewReplicationEngine("primary", fastCfg(EventualConsistency))
	re.Start()
	defer re.Close()

	op := newOp(1, "k1", []byte("v1"))

	start := time.Now()
	if err := re.OnLocalWrite(op); err != nil {
		t.Fatalf("OnLocalWrite: %v", err)
	}
	elapsed := time.Since(start)

	// Async path must be non-blocking — 50 ms is a generous ceiling.
	if elapsed > 50*time.Millisecond {
		t.Errorf("async write took %v; expected < 50ms", elapsed)
	}
}

// TestReplication_QuorumWrite: replicas are added with no real TCP backend
// (test mode — sendReplicationRPC falls back to no-op); after the flush
// window the ops are marked replicated.
func TestReplication_QuorumWrite(t *testing.T) {
	t.Parallel()

	re := NewReplicationEngine("primary", fastCfg(QuorumConsistency))
	// Add two replicas without a real TCP server — no client is registered
	// for them; the engine uses the no-op path (test / single-node mode).
	// We use AddReplicaWithReplPort with port 0 so the engine registers a
	// client toward a non-existent address. To keep the test self-contained
	// without a live TCP server we mutate the client map to nil after adding.
	if err := re.AddReplica("r1", "127.0.0.1", 19001); err != nil {
		t.Fatal(err)
	}
	if err := re.AddReplica("r2", "127.0.0.1", 19002); err != nil {
		t.Fatal(err)
	}
	// Remove TCP clients so the engine uses the no-op metric-only path.
	re.mu.Lock()
	delete(re.clients, "r1")
	delete(re.clients, "r2")
	re.mu.Unlock()

	re.Start()
	defer re.Close()

	op := newOp(42, "quorum-key", []byte("quorum-val"))
	if err := re.OnLocalWrite(op); err != nil {
		t.Fatalf("OnLocalWrite: %v", err)
	}

	// Give the background worker time to flush the batch.
	time.Sleep(50 * time.Millisecond)

	re.mu.RLock()
	_, pending := re.pendingWrites[42]
	re.mu.RUnlock()

	// The operation should have been processed (stored in pendingWrites during flush).
	// We only verify that the engine did not panic and metrics incremented.
	m := re.GetMetrics()
	if m.ReplicatedWrites.Load() == 0 {
		t.Error("expected ReplicatedWrites > 0 after flush")
	}
	_ = pending // pendingWrites presence after flush is an implementation detail
}

// TestReplication_StrongWrite: verify that the replication engine handles
// StrongConsistency mode — the engine calls wg.Wait() for all replicas.
// We assert the metrics path works and ReplicatedWrites increments.
func TestReplication_StrongWrite(t *testing.T) {
	t.Parallel()

	re := NewReplicationEngine("primary", fastCfg(StrongConsistency))
	if err := re.AddReplica("r1", "127.0.0.1", 19011); err != nil {
		t.Fatal(err)
	}
	// Remove TCP client so sendReplicationRPC uses the no-op path.
	re.mu.Lock()
	delete(re.clients, "r1")
	re.mu.Unlock()

	re.Start()
	defer re.Close()

	op := newOp(7, "strong-key", []byte("strong-val"))
	if err := re.OnLocalWrite(op); err != nil {
		t.Fatalf("OnLocalWrite: %v", err)
	}

	// Allow the background flush to complete.
	time.Sleep(50 * time.Millisecond)

	m := re.GetMetrics()
	if m.ReplicatedWrites.Load() == 0 {
		t.Error("expected ReplicatedWrites > 0 for StrongConsistency mode")
	}
}

// TestReplication_VectorClock: each UpdateVersion call increments the clock.
func TestReplication_VectorClock(t *testing.T) {
	t.Parallel()

	vv := &VersionVector{Clock: make(map[string]uint64)}

	nodeID := "node-vc"
	vv.UpdateVersion(nodeID)
	if vv.Clock[nodeID] != 1 {
		t.Errorf("expected clock[%s]=1 after 1 update, got %d", nodeID, vv.Clock[nodeID])
	}
	vv.UpdateVersion(nodeID)
	if vv.Clock[nodeID] != 2 {
		t.Errorf("expected clock[%s]=2 after 2 updates, got %d", nodeID, vv.Clock[nodeID])
	}

	// Multiple nodes — independent clocks.
	vv.UpdateVersion("node-a")
	vv.UpdateVersion("node-b")
	vv.UpdateVersion("node-a")
	if vv.Clock["node-a"] != 2 {
		t.Errorf("expected clock[node-a]=2, got %d", vv.Clock["node-a"])
	}
	if vv.Clock["node-b"] != 1 {
		t.Errorf("expected clock[node-b]=1, got %d", vv.Clock["node-b"])
	}
}

// TestReplication_AntiEntropy: anti-entropy worker increments AntiEntropyRuns
// after its ticker fires.
func TestReplication_AntiEntropy(t *testing.T) {
	t.Parallel()

	cfg := fastCfg(EventualConsistency)
	cfg.AntiEntropyInterval = 20 * time.Millisecond // very short for test
	re := NewReplicationEngine("primary", cfg)
	re.Start()
	defer re.Close()

	// Add a lagging replica with no TCP client (no-op path).
	if err := re.AddReplica("lag-r1", "127.0.0.1", 19021); err != nil {
		t.Fatal(err)
	}
	re.mu.Lock()
	delete(re.clients, "lag-r1")
	// Mark replica as lagging so runAntiEntropy picks it up.
	re.replicaStates["lag-r1"].State = ReplicaStateLag
	re.mu.Unlock()

	// Wait for a couple of anti-entropy ticks.
	time.Sleep(100 * time.Millisecond)

	runs := re.GetMetrics().AntiEntropyRuns.Load()
	if runs == 0 {
		t.Error("expected AntiEntropyRuns > 0 after waiting for ticks")
	}
}

// TestReplication_TombstoneGC: tombstone WaitForReplication returns an error
// (write not found) when the op was never stored in pendingWrites.
// This mirrors the tombstone-GC contract: CanReapTombstone must not be called
// until all replicas have acked past the tombstone seqnum.
func TestReplication_TombstoneGC(t *testing.T) {
	t.Parallel()

	re := NewReplicationEngine("primary", fastCfg(QuorumConsistency))
	re.Start()
	defer re.Close()

	// Add a replica with no TCP client.
	if err := re.AddReplica("repl-gc", "127.0.0.1", 19031); err != nil {
		t.Fatal(err)
	}
	re.mu.Lock()
	delete(re.clients, "repl-gc")
	re.mu.Unlock()

	tombstone := &WriteOperation{
		SeqNum:      100,
		Key:         "dead-key",
		IsTombstone: true,
		Timestamp:   time.Now().UnixNano(),
		NodeID:      "primary",
	}
	if err := re.OnLocalWrite(tombstone); err != nil {
		t.Fatalf("OnLocalWrite tombstone: %v", err)
	}

	// Before the replica acks seqNum 100, WaitForReplication should NOT succeed
	// with targetReplicas=2 (primary+replica) within a short window.
	err := re.WaitForReplication(100, 2, 30)
	// The call should time out or fail because the replica hasn't acked.
	// Both "not found" and "timeout" are acceptable non-nil errors here.
	if err == nil {
		// It could succeed if the background worker flushed and updated
		// LastAckSeqNum before we checked. Check replica state explicitly.
		re.mu.RLock()
		ack := re.replicaStates["repl-gc"].LastAckSeqNum
		re.mu.RUnlock()
		if ack < 100 {
			t.Error("WaitForReplication returned nil but replica has not acked seqNum 100")
		}
	}
	// If err != nil that is the expected path (not yet replicated).
}

// TestReplication_VectorClockConcurrent: concurrent UpdateVersion on the same
// VersionVector must not race (run with -race).
func TestReplication_VectorClockConcurrent(t *testing.T) {
	t.Parallel()

	vv := &VersionVector{Clock: make(map[string]uint64)}
	var wg sync.WaitGroup
	var total atomic.Uint64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vv.UpdateVersion("shared-node")
			total.Add(1)
		}()
	}
	wg.Wait()
	if vv.Clock["shared-node"] != 20 {
		t.Errorf("expected clock=20, got %d", vv.Clock["shared-node"])
	}
}

// TestReplication_AddRemoveReplica: AddReplica and RemoveReplica properly manage
// the replicaStates and clients maps.
func TestReplication_AddRemoveReplica(t *testing.T) {
	t.Parallel()

	re := NewReplicationEngine("primary", fastCfg(EventualConsistency))
	defer re.Close()

	if err := re.AddReplica("r1", "127.0.0.1", 19041); err != nil {
		t.Fatalf("AddReplica: %v", err)
	}
	// Duplicate add must fail.
	if err := re.AddReplica("r1", "127.0.0.1", 19041); err == nil {
		t.Error("expected error on duplicate AddReplica, got nil")
	}

	re.mu.RLock()
	_, exists := re.replicaStates["r1"]
	re.mu.RUnlock()
	if !exists {
		t.Fatal("r1 not found in replicaStates after AddReplica")
	}

	if err := re.RemoveReplica("r1"); err != nil {
		t.Fatalf("RemoveReplica: %v", err)
	}
	re.mu.RLock()
	_, exists = re.replicaStates["r1"]
	re.mu.RUnlock()
	if exists {
		t.Error("r1 still in replicaStates after RemoveReplica")
	}
}

// TestReplication_GetReplicaLag: GetReplicaLag returns an entry per registered replica.
func TestReplication_GetReplicaLag(t *testing.T) {
	t.Parallel()

	re := NewReplicationEngine("primary", fastCfg(EventualConsistency))
	defer re.Close()

	if err := re.AddReplica("lag-check", "127.0.0.1", 19051); err != nil {
		t.Fatal(err)
	}

	lag := re.GetReplicaLag()
	info, ok := lag["lag-check"]
	if !ok {
		t.Fatal("GetReplicaLag missing entry for lag-check")
	}
	if info.NodeID != "lag-check" {
		t.Errorf("NodeID mismatch: got %q want %q", info.NodeID, "lag-check")
	}
}
