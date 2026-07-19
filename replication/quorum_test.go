package replication

// quorum_test.go — synchronization-based tests for per-batch ack accounting.
//
// All tests drive replicateBatch directly (white-box, same package) through
// gateTransport, a ReplicaTransport whose Send blocks until the test releases
// it.  Progress is proven with channels, not sleeps: a batch provably cannot
// complete before the acks the test has released.

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// gateTransport is a ReplicaTransport whose Send blocks until released.
type gateTransport struct {
	called  chan struct{} // receives one signal per Send invocation
	release chan struct{} // close to let Send return err
	err     error         // result Send returns once released (set before use)
	closed  chan struct{}
	once    sync.Once
}

func newGateTransport() *gateTransport {
	return &gateTransport{
		called:  make(chan struct{}, 16),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (g *gateTransport) Send(ops []*WriteOperation) error {
	g.called <- struct{}{}
	select {
	case <-g.release:
		return g.err
	case <-g.closed:
		return errors.New("transport closed")
	}
}

func (g *gateTransport) Close() { g.once.Do(func() { close(g.closed) }) }

// newGatedEngine builds an engine (not Started — tests call replicateBatch
// directly) with n replicas, each backed by a gateTransport.
func newGatedEngine(t *testing.T, level ConsistencyLevel, rf int, timeout time.Duration, n int) (*ReplicationEngine, []*gateTransport) {
	t.Helper()

	cfg := fastCfg(level)
	cfg.ReplicationFactor = rf
	cfg.ReplicationTimeout = timeout

	re := NewReplicationEngine("primary", cfg)
	gates := make([]*gateTransport, n)
	for i := range gates {
		nodeID := fmt.Sprintf("r%d", i+1)
		if err := re.AddReplica(nodeID, "127.0.0.1", 21000+i); err != nil {
			t.Fatalf("AddReplica(%s): %v", nodeID, err)
		}
		gates[i] = newGateTransport()
		if err := re.SetReplicaTransport(nodeID, gates[i]); err != nil {
			t.Fatalf("SetReplicaTransport(%s): %v", nodeID, err)
		}
	}
	t.Cleanup(func() { re.Close() }) // releases any still-hung gates
	return re, gates
}

// TestReplication_QuorumAfterMajorityNotBefore proves the per-batch ack
// accounting: with RF=3 (majority = 2 copies = local + 1 remote ack) and two
// replicas, replicateBatch must NOT return while zero remote acks have been
// delivered, and must return nil as soon as one replica acks.
//
// Synchronization-based proof, no sleeps: after both transports have entered
// Send (observed via g.called) and neither has been released, zero results
// exist, so a correct implementation is provably blocked — done cannot be
// readable.  The old racy implementation (draining a channel still being
// filled) returned immediately with zero acks and is caught by the
// non-blocking done check.
func TestReplication_QuorumAfterMajorityNotBefore(t *testing.T) {
	t.Parallel()

	re, gates := newGatedEngine(t, QuorumConsistency, 3, 5*time.Second, 2)
	ops := []*WriteOperation{newOp(1, "k1", []byte("v1")), newOp(2, "k2", []byte("v2"))}

	done := make(chan error, 1)
	go func() { done <- re.replicateBatch(ops) }()

	// Both replica sends are now in flight and blocked.
	<-gates[0].called
	<-gates[1].called

	// Zero acks delivered → quorum cannot have been declared.
	select {
	case err := <-done:
		t.Fatalf("replicateBatch returned (err=%v) before any replica acked", err)
	default:
	}

	// Release exactly one replica (the majority, counting the local write).
	close(gates[0].release)

	if err := <-done; err != nil {
		t.Fatalf("replicateBatch after majority ack: %v", err)
	}

	// The acking replica's ack watermark must cover the whole batch.
	re.mu.RLock()
	ack := re.replicaStates["r1"].LastAckSeqNum
	re.mu.RUnlock()
	if ack != 2 {
		t.Errorf("r1 LastAckSeqNum = %d, want 2", ack)
	}
}

// TestReplication_QuorumTimeoutWithHungReplicas: RF=5 → majority is 3 copies
// = local + 2 remote acks.  Of 3 replicas, 1 acks and 2 hang, so the quorum
// can never be met and the batch must fail with ErrReplicationTimeout at the
// deadline (not ErrQuorumNotReached — no replica failed outright).
func TestReplication_QuorumTimeoutWithHungReplicas(t *testing.T) {
	t.Parallel()

	re, gates := newGatedEngine(t, QuorumConsistency, 5, 150*time.Millisecond, 3)
	close(gates[0].release) // r1 acks immediately; r2, r3 hang forever

	err := re.replicateBatch([]*WriteOperation{newOp(1, "k", []byte("v"))})
	if !errors.Is(err, ErrReplicationTimeout) {
		t.Fatalf("expected ErrReplicationTimeout, got %v", err)
	}
	if errors.Is(err, ErrQuorumNotReached) {
		t.Errorf("timeout must be distinguishable from quorum failure, got %v", err)
	}
	if re.GetMetrics().FailedReplications.Load() == 0 {
		t.Error("expected FailedReplications > 0 after timeout")
	}
}

// TestReplication_SyncErrorsOnHungReplica: StrongConsistency requires ALL
// replicas to ack; one hung replica must surface ErrReplicationTimeout even
// though the other acked.
func TestReplication_SyncErrorsOnHungReplica(t *testing.T) {
	t.Parallel()

	re, gates := newGatedEngine(t, StrongConsistency, 3, 150*time.Millisecond, 2)
	close(gates[0].release) // r1 acks; r2 hangs

	err := re.replicateBatch([]*WriteOperation{newOp(1, "k", []byte("v"))})
	if !errors.Is(err, ErrReplicationTimeout) {
		t.Fatalf("expected ErrReplicationTimeout, got %v", err)
	}
}

// TestReplication_SyncFailsFastOnReplicaError: in StrongConsistency mode a
// replica that fails outright makes "all acks" unreachable, so the batch must
// fail fast with ErrQuorumNotReached — well before the (long) deadline.
func TestReplication_SyncFailsFastOnReplicaError(t *testing.T) {
	t.Parallel()

	re, gates := newGatedEngine(t, StrongConsistency, 3, time.Hour, 2)
	close(gates[0].release) // r1 acks
	gates[1].err = errors.New("disk on fire")
	close(gates[1].release) // r2 fails

	err := re.replicateBatch([]*WriteOperation{newOp(1, "k", []byte("v"))})
	if !errors.Is(err, ErrQuorumNotReached) {
		t.Fatalf("expected ErrQuorumNotReached (fail fast), got %v", err)
	}
}

// TestReplication_AsyncNeverBlocks: EventualConsistency must return
// immediately even with every replica transport hung.  The proof is
// synchronization-based: the gates are never released, so if replicateBatch
// waited on any replica it could never return, and the test would time out.
func TestReplication_AsyncNeverBlocks(t *testing.T) {
	t.Parallel()

	re, gates := newGatedEngine(t, EventualConsistency, 3, time.Hour, 2)

	if err := re.replicateBatch([]*WriteOperation{newOp(1, "k", []byte("v"))}); err != nil {
		t.Fatalf("async replicateBatch: %v", err)
	}

	// The sends were still dispatched in the background.
	<-gates[0].called
	<-gates[1].called
}

// TestReplication_NoTransportErrors: a replica that is registered but has no
// transport client must produce an error (never a silent fake ack) in
// Sync/Quorum modes; the error chain exposes both ErrQuorumNotReached and
// ErrNoTransport.  A single-node engine (no replicas at all) keeps working.
func TestReplication_NoTransportErrors(t *testing.T) {
	t.Parallel()

	for _, level := range []ConsistencyLevel{QuorumConsistency, StrongConsistency} {
		re, _ := newGatedEngine(t, level, 3, time.Hour, 1)
		if err := re.SetReplicaTransport("r1", nil); err != nil { // drop the client
			t.Fatal(err)
		}

		err := re.replicateBatch([]*WriteOperation{newOp(1, "k", []byte("v"))})
		if !errors.Is(err, ErrQuorumNotReached) {
			t.Errorf("level %d: expected ErrQuorumNotReached, got %v", level, err)
		}
		if !errors.Is(err, ErrNoTransport) {
			t.Errorf("level %d: expected ErrNoTransport in chain, got %v", level, err)
		}
	}

	// Single-node default: no replicas registered → trivially satisfied.
	re := NewReplicationEngine("primary", fastCfg(QuorumConsistency))
	t.Cleanup(func() { re.Close() })
	if err := re.replicateBatch([]*WriteOperation{newOp(1, "k", []byte("v"))}); err != nil {
		t.Errorf("single-node replicateBatch: %v", err)
	}
}

// TestReplication_BackpressureOnLag: in Sync/Quorum mode a replica whose
// accumulated lag exceeds BackpressureLagBytes marks the batch result with
// ErrReplicaLagging and increments the BackpressureEvents metric, even though
// the batch itself replicates successfully.
func TestReplication_BackpressureOnLag(t *testing.T) {
	t.Parallel()

	re, gates := newGatedEngine(t, QuorumConsistency, 3, 5*time.Second, 1)
	re.config.BackpressureLagBytes = 10
	close(gates[0].release) // replica acks immediately

	re.mu.RLock()
	re.replicaStates["r1"].LagBytes.Store(1000) // over the threshold
	re.mu.RUnlock()

	err := re.replicateBatch([]*WriteOperation{newOp(1, "k", []byte("v"))})
	if !errors.Is(err, ErrReplicaLagging) {
		t.Fatalf("expected ErrReplicaLagging, got %v", err)
	}
	if got := re.GetMetrics().BackpressureEvents.Load(); got != 1 {
		t.Errorf("BackpressureEvents = %d, want 1", got)
	}
}

// TestReplication_WaitForReplicationTracksAcks: WaitForReplication blocks
// until the replica's ack watermark reaches the sequence number and then
// returns nil (2 total copies = local + 1 replica).
func TestReplication_WaitForReplicationTracksAcks(t *testing.T) {
	t.Parallel()

	re, gates := newGatedEngine(t, QuorumConsistency, 3, 5*time.Second, 1)

	op := newOp(9, "k9", []byte("v9"))
	if err := re.OnLocalWrite(op); err != nil { // registers the op as pending
		t.Fatalf("OnLocalWrite: %v", err)
	}

	go re.replicateBatch([]*WriteOperation{op})
	<-gates[0].called // replica send in flight, not yet acked

	waitDone := make(chan error, 1)
	go func() { waitDone <- re.WaitForReplication(9, 2, 2000) }()

	// No ack yet → WaitForReplication cannot have succeeded.
	select {
	case err := <-waitDone:
		t.Fatalf("WaitForReplication returned (err=%v) before the replica acked", err)
	default:
	}

	close(gates[0].release) // deliver the ack
	if err := <-waitDone; err != nil {
		t.Fatalf("WaitForReplication after ack: %v", err)
	}
}
