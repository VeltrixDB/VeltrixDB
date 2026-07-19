package replication

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Sentinel errors returned by the replication paths.  Callers can distinguish
// failure modes with errors.Is:
//
//	ErrReplicationTimeout — the ack deadline expired before enough replicas acked.
//	ErrQuorumNotReached   — enough replicas failed that the required ack count
//	                        can no longer be met (fails fast, before the deadline).
//	ErrNoTransport        — a replica is registered but has no transport client;
//	                        in Sync/Quorum modes this is a hard error, never a fake ack.
//	ErrReplicaLagging     — backpressure: a replica's lag exceeds
//	                        ReplicationConfig.BackpressureLagBytes in Sync/Quorum mode.
var (
	ErrReplicationTimeout = errors.New("replication: timeout waiting for replica acks")
	ErrQuorumNotReached   = errors.New("replication: quorum not reached")
	ErrNoTransport        = errors.New("replication: no transport client registered for replica")
	ErrReplicaLagging     = errors.New("replication: replica lag exceeds backpressure threshold")
)

// quorumError carries ack-count detail while matching both ErrQuorumNotReached
// (via Is) and the first underlying replica error (via Unwrap).
type quorumError struct {
	acked    int
	required int
	total    int
	cause    error
}

func (e *quorumError) Error() string {
	return fmt.Sprintf("replication: quorum not reached: %d/%d acks (need %d): %v",
		e.acked, e.total, e.required, e.cause)
}

func (e *quorumError) Unwrap() error { return e.cause }

func (e *quorumError) Is(target error) bool { return target == ErrQuorumNotReached }

// ConsistencyLevel defines consistency requirements
type ConsistencyLevel int

const (
	EventualConsistency ConsistencyLevel = iota
	StrongConsistency
	QuorumConsistency
)

// ReplicationMode defines how replication is performed
type ReplicationMode int

const (
	AsyncReplication ReplicationMode = iota
	SyncReplication
	HybridReplication
)

// ReplicaState represents the state of a replica
type ReplicaState int

const (
	ReplicaStateSync    ReplicaState = iota
	ReplicaStateSync_Pending
	ReplicaStateLag
	ReplicaStateFailed
)

// ReplicationMetrics tracks replication statistics
type ReplicationMetrics struct {
	ReplicatedWrites    atomic.Uint64
	FailedReplications  atomic.Uint64
	ReplicaLagBytes     atomic.Uint64
	ReplicaLagNs        atomic.Int64
	ConflictResolutions atomic.Uint64
	VectorClockUpdates  atomic.Uint64
	AntiEntropyRuns     atomic.Uint64
	// BackpressureEvents counts Sync/Quorum batches in which at least one
	// replica's lag exceeded ReplicationConfig.BackpressureLagBytes.
	BackpressureEvents atomic.Uint64
}

// VersionVector tracks causal ordering (MVCC)
type VersionVector struct {
	mu    sync.RWMutex
	Clock map[string]uint64 // node_id → logical_clock
}

// WriteOperation represents a write that needs replication
type WriteOperation struct {
	SeqNum        uint64
	Key           string
	Value         []byte
	Timestamp     int64
	Version       uint64
	VersionVector *VersionVector
	TTL           int32
	NodeID        string // Origin node
	IsTombstone   bool   // Delete marker
}

// ReplicationConfig contains replication settings
type ReplicationConfig struct {
	ConsistencyLevel    ConsistencyLevel
	ReplicationMode     ReplicationMode
	ReplicationFactor   int
	BatchSize           int
	FlushIntervalMs     int32
	MaxLagBytesToSync   uint64
	VectorClockEnabled  bool
	AntiEntropyInterval time.Duration
	ReadRepairEnabled   bool

	// ReplicationTimeout bounds how long a Sync/Quorum batch waits for replica
	// acks before returning ErrReplicationTimeout.  Zero means the default
	// (10s).  Async (EventualConsistency) never waits.
	ReplicationTimeout time.Duration

	// BackpressureLagBytes is a per-replica lag threshold (bytes).  In
	// Sync/Quorum mode, a batch against a replica whose accumulated lag
	// exceeds this threshold marks the replica ReplicaStateLag, increments
	// ReplicationMetrics.BackpressureEvents, and surfaces ErrReplicaLagging
	// on the batch result even if the batch itself replicated.  Zero disables
	// the check (legacy behavior).
	BackpressureLagBytes uint64

	// TLS enables TLS 1.3 for the inter-node replication transport (both the
	// clients created by AddReplica and the server started by
	// StartReplicationServer).  Nil or TLSEnabled=false keeps the legacy
	// plaintext TCP transport.
	TLS *TransportTLSConfig
}

// DefaultReplicationConfig returns sensible defaults
func DefaultReplicationConfig() *ReplicationConfig {
	return &ReplicationConfig{
		ConsistencyLevel:    QuorumConsistency,
		ReplicationMode:     AsyncReplication,
		ReplicationFactor:   3,
		BatchSize:           100,
		FlushIntervalMs:     10,
		MaxLagBytesToSync:   1024 * 1024, // 1MB
		VectorClockEnabled:  true,
		AntiEntropyInterval: 30 * time.Second,
		ReadRepairEnabled:   true,
		ReplicationTimeout:  10 * time.Second,
	}
}

// ReplicationEngine handles data replication across nodes
type ReplicationEngine struct {
	mu                sync.RWMutex
	config            *ReplicationConfig
	localNodeID       string
	writeQueue        chan *WriteOperation
	replicaStates     map[string]*ReplicaInfo
	metrics           *ReplicationMetrics
	versionVectors    map[string]*VersionVector // per replica
	done              chan struct{}
	pendingWrites     map[uint64]*WriteOperation
	lastReplicationTs int64

	// clients holds one replication transport per replica nodeID.
	// Populated by AddReplica (real TCP/TLS client) or SetReplicaTransport
	// (custom/test transport).
	clients map[string]ReplicaTransport
}

// ReplicaTransport abstracts the per-replica transport so tests and future
// integrations can inject their own implementation.  *ReplicationClient (the
// TCP/TLS transport in transport.go) satisfies this interface.
type ReplicaTransport interface {
	// Send synchronously replicates a batch and returns nil only once the
	// replica acknowledged it.
	Send(ops []*WriteOperation) error
	Close()
}

// ReplicaInfo tracks information about a replica.
//
// Locking: State and LastAckSeqNum are guarded by ReplicationEngine.mu (reads
// under RLock, writes under Lock).  The atomic fields are safe to touch
// without the engine lock.
type ReplicaInfo struct {
	NodeID         string
	Address        string
	Port           int
	State          ReplicaState
	LastAckSeqNum  uint64
	LagBytes       atomic.Uint64
	LagNs          atomic.Int64
	FailureCount   atomic.Uint64
	SyncedAt       int64
}

// NewReplicationEngine creates a new replication engine
func NewReplicationEngine(nodeID string, config *ReplicationConfig) *ReplicationEngine {
	return &ReplicationEngine{
		localNodeID:    nodeID,
		config:         config,
		writeQueue:     make(chan *WriteOperation, 10000),
		replicaStates:  make(map[string]*ReplicaInfo),
		metrics:        &ReplicationMetrics{},
		versionVectors: make(map[string]*VersionVector),
		done:           make(chan struct{}),
		pendingWrites:  make(map[uint64]*WriteOperation),
		clients:        make(map[string]ReplicaTransport),
	}
}

// Start begins the replication engine
func (re *ReplicationEngine) Start() {
	go re.backgroundReplicationWorker()
	go re.backgroundAntiEntropyWorker()
	go re.backgroundLagMonitor()
}

// OnLocalWrite handles a write on the primary node.  It never blocks: the op
// is queued for the background replication worker.  Callers that need Sync or
// Quorum semantics on the write path should follow up with WaitForReplication.
//
// The op is registered in pendingWrites before it is queued so that
// WaitForReplication called immediately after OnLocalWrite observes it.
func (re *ReplicationEngine) OnLocalWrite(op *WriteOperation) error {
	re.mu.Lock()
	re.pendingWrites[op.SeqNum] = op
	re.mu.Unlock()

	select {
	case re.writeQueue <- op:
		return nil
	case <-re.done:
		re.dropPending(op.SeqNum)
		return fmt.Errorf("replication engine stopped")
	default:
		re.dropPending(op.SeqNum)
		return fmt.Errorf("write queue full")
	}
}

func (re *ReplicationEngine) dropPending(seqNum uint64) {
	re.mu.Lock()
	delete(re.pendingWrites, seqNum)
	re.mu.Unlock()
}

// AddReplica registers a replica node and establishes a TCP replication client.
// replPort is the port the replica's ReplicationServer listens on; if 0 it
// defaults to port+1 (storage port + 1 convention).
func (re *ReplicationEngine) AddReplica(nodeID, address string, port int) error {
	return re.AddReplicaWithReplPort(nodeID, address, port, 0)
}

// AddReplicaWithReplPort is like AddReplica but lets the caller specify the
// dedicated replication port explicitly (non-zero).
func (re *ReplicationEngine) AddReplicaWithReplPort(nodeID, address string, port, replPort int) error {
	re.mu.Lock()
	defer re.mu.Unlock()

	if _, exists := re.replicaStates[nodeID]; exists {
		return fmt.Errorf("replica %s already exists", nodeID)
	}

	re.replicaStates[nodeID] = &ReplicaInfo{
		NodeID:        nodeID,
		Address:       address,
		Port:          port,
		State:         ReplicaStateSync,
		LastAckSeqNum: 0,
		SyncedAt:      time.Now().UnixNano(),
	}

	re.versionVectors[nodeID] = &VersionVector{
		Clock: make(map[string]uint64),
	}

	// Create a TCP replication client.  Convention: replication port = storage port + 1.
	if replPort == 0 {
		replPort = port + 1
	}
	addr := fmt.Sprintf("%s:%d", address, replPort)
	if re.config != nil && re.config.TLS.enabled() {
		client, err := NewReplicationClientTLS(addr, re.config.TLS)
		if err != nil {
			delete(re.replicaStates, nodeID)
			delete(re.versionVectors, nodeID)
			return fmt.Errorf("replica %s: %w", nodeID, err)
		}
		re.clients[nodeID] = client
	} else {
		re.clients[nodeID] = NewReplicationClient(addr)
	}

	return nil
}

// SetReplicaTransport replaces (or installs) the transport used to reach an
// already-registered replica.  The previous transport, if any, is closed.
// Intended for tests and custom integrations.
func (re *ReplicationEngine) SetReplicaTransport(nodeID string, t ReplicaTransport) error {
	re.mu.Lock()
	defer re.mu.Unlock()

	if _, exists := re.replicaStates[nodeID]; !exists {
		return fmt.Errorf("replica %s not registered", nodeID)
	}
	if old, ok := re.clients[nodeID]; ok && old != nil {
		old.Close()
	}
	if t == nil {
		delete(re.clients, nodeID)
	} else {
		re.clients[nodeID] = t
	}
	return nil
}

// RemoveReplica unregisters a replica node and closes its TCP client.
func (re *ReplicationEngine) RemoveReplica(nodeID string) error {
	re.mu.Lock()
	defer re.mu.Unlock()

	if c, ok := re.clients[nodeID]; ok {
		c.Close()
		delete(re.clients, nodeID)
	}
	delete(re.replicaStates, nodeID)
	delete(re.versionVectors, nodeID)
	return nil
}

// WaitForReplication waits until seqNum has been acked by enough replicas
// that targetReplicas total copies exist (the local write counts as one).
// Returns nil on success, an ErrReplicationTimeout-wrapping error on deadline
// expiry, or "write not found" if the op was never (or is no longer tracked
// as) pending and the target has not been met.
//
// Ack progress is judged solely on LastAckSeqNum, which is monotonic — a
// replica that acked seqNum and later transitioned to LAG/FAILED still counts,
// because the data is durably on it.
func (re *ReplicationEngine) WaitForReplication(seqNum uint64, targetReplicas int, maxWaitMs int) error {
	deadline := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)

	for {
		re.mu.RLock()
		syncedCount := 0
		for _, replica := range re.replicaStates {
			if replica.LastAckSeqNum >= seqNum {
				syncedCount++
			}
		}
		_, pending := re.pendingWrites[seqNum]
		re.mu.RUnlock()

		// Primary + syncedCount replicas.
		if syncedCount+1 >= targetReplicas {
			return nil // Replication complete
		}

		if !pending {
			return fmt.Errorf("write not found")
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %d/%d copies after %dms",
				ErrReplicationTimeout, syncedCount+1, targetReplicas, maxWaitMs)
		}

		time.Sleep(1 * time.Millisecond)
	}
}

// GetReplicaLag returns the replication lag for all replicas
func (re *ReplicationEngine) GetReplicaLag() map[string]ReplicaLagInfo {
	re.mu.RLock()
	defer re.mu.RUnlock()
	
	lag := make(map[string]ReplicaLagInfo)
	for nodeID, replica := range re.replicaStates {
		lag[nodeID] = ReplicaLagInfo{
			NodeID:        nodeID,
			State:         replica.State.String(),
			LastAckSeqNum: replica.LastAckSeqNum,
			LagBytes:      replica.LagBytes.Load(),
			LagNs:         replica.LagNs.Load(),
		}
	}
	return lag
}

// ReplicaLagInfo contains replica lag information
type ReplicaLagInfo struct {
	NodeID        string
	State         string
	LastAckSeqNum uint64
	LagBytes      uint64
	LagNs         int64
}

// String representation of ReplicaState
func (rs ReplicaState) String() string {
	switch rs {
	case ReplicaStateSync:
		return "SYNC"
	case ReplicaStateSync_Pending:
		return "SYNC_PENDING"
	case ReplicaStateLag:
		return "LAG"
	case ReplicaStateFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Background workers

// backgroundReplicationWorker processes write replication
func (re *ReplicationEngine) backgroundReplicationWorker() {
	ticker := time.NewTicker(time.Duration(re.config.FlushIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	
	batch := make([]*WriteOperation, 0, re.config.BatchSize)
	
	for {
		select {
		case <-re.done:
			return
		case op := <-re.writeQueue:
			batch = append(batch, op)
			
			// Store pending write
			re.mu.Lock()
			re.pendingWrites[op.SeqNum] = op
			re.mu.Unlock()
			
			if len(batch) >= re.config.BatchSize {
				re.replicateBatch(batch)
				batch = make([]*WriteOperation, 0, re.config.BatchSize)
			}
		
		case <-ticker.C:
			if len(batch) > 0 {
				re.replicateBatch(batch)
				batch = make([]*WriteOperation, 0, re.config.BatchSize)
			}
		}
	}
}

// defaultReplicationTimeout bounds Sync/Quorum ack waits when
// ReplicationConfig.ReplicationTimeout is zero.
const defaultReplicationTimeout = 10 * time.Second

// replicateBatch sends a batch of writes to all non-failed replicas and waits
// according to the configured consistency level, using per-batch ack
// accounting (each replica goroutine reports exactly one result on a private
// channel — the decision never inspects a channel that is still being filled):
//
//	StrongConsistency   — every replica must ack; any failure or an expired
//	                      deadline is an error.
//	QuorumConsistency   — a majority of ReplicationFactor copies must ack,
//	                      counting the local write as one copy.  Fails fast
//	                      (ErrQuorumNotReached) as soon as enough replicas
//	                      have failed that the majority is unreachable;
//	                      otherwise errors with ErrReplicationTimeout at the
//	                      deadline.
//	EventualConsistency — fire and forget; never blocks.
//
// With no replicas registered (single-node default) every level is trivially
// satisfied by the local write and the call returns nil immediately.
func (re *ReplicationEngine) replicateBatch(ops []*WriteOperation) error {
	if len(ops) == 0 {
		return nil
	}

	re.mu.RLock()
	replicas := make([]*ReplicaInfo, 0, len(re.replicaStates))
	for _, replica := range re.replicaStates {
		if replica.State != ReplicaStateFailed {
			replicas = append(replicas, replica)
		}
	}
	re.mu.RUnlock()

	re.metrics.ReplicatedWrites.Add(uint64(len(ops)))

	// Single-node mode: the local write is the only copy.
	if len(replicas) == 0 {
		re.clearPending(ops)
		return nil
	}

	level := re.config.ConsistencyLevel

	// Backpressure hook: in Sync/Quorum mode, flag replicas whose accumulated
	// lag exceeds the configured threshold before sending the batch.  The
	// batch is still attempted, but the result surfaces ErrReplicaLagging so
	// the write path can slow down or shed load.
	var lagging bool
	if re.config.BackpressureLagBytes > 0 && level != EventualConsistency {
		re.mu.Lock()
		for _, r := range replicas {
			if r.LagBytes.Load() > re.config.BackpressureLagBytes {
				r.State = ReplicaStateLag
				lagging = true
			}
		}
		re.mu.Unlock()
		if lagging {
			re.metrics.BackpressureEvents.Add(1)
		}
	}

	// One buffered slot per replica: sender goroutines never block, and every
	// send attempt reports exactly one result.
	results := make(chan error, len(replicas))
	for _, replica := range replicas {
		go func(r *ReplicaInfo) {
			results <- re.sendToReplica(r, ops)
		}(replica)
	}

	// requiredAcks is the number of REMOTE acks to wait for.
	var requiredAcks int
	switch level {
	case StrongConsistency:
		requiredAcks = len(replicas)
	case QuorumConsistency:
		// Majority of ReplicationFactor copies, counting the local write.
		requiredAcks = (re.config.ReplicationFactor / 2) + 1 - 1
		if requiredAcks <= 0 {
			// RF <= 1: the local write already is the majority.
			if lagging {
				return fmt.Errorf("%w (threshold %d bytes)", ErrReplicaLagging, re.config.BackpressureLagBytes)
			}
			return nil
		}
		if requiredAcks > len(replicas) {
			// Not enough replicas registered to ever reach quorum.
			re.metrics.FailedReplications.Add(1)
			return &quorumError{
				acked:    0,
				required: requiredAcks,
				total:    len(replicas),
				cause:    fmt.Errorf("only %d replicas registered", len(replicas)),
			}
		}
	default: // EventualConsistency: fire and forget.
		return nil
	}

	timeout := re.config.ReplicationTimeout
	if timeout <= 0 {
		timeout = defaultReplicationTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	acked, failed := 0, 0
	var firstErr error
	for acked < requiredAcks {
		// Fail fast once enough replicas failed that requiredAcks is unreachable.
		if len(replicas)-failed < requiredAcks {
			re.metrics.FailedReplications.Add(1)
			return &quorumError{acked: acked, required: requiredAcks, total: len(replicas), cause: firstErr}
		}
		select {
		case err := <-results:
			if err != nil {
				failed++
				if firstErr == nil {
					firstErr = err
				}
			} else {
				acked++
			}
		case <-timer.C:
			re.metrics.FailedReplications.Add(1)
			return fmt.Errorf("%w: %d/%d replica acks (need %d) after %v",
				ErrReplicationTimeout, acked, len(replicas), requiredAcks, timeout)
		case <-re.done:
			return fmt.Errorf("replication engine stopped")
		}
	}

	// Fully replicated batches are no longer pending (anti-entropy keeps
	// retrying partially-acked batches from pendingWrites).
	if acked == len(replicas) {
		re.clearPending(ops)
	}
	if lagging {
		return fmt.Errorf("%w (threshold %d bytes)", ErrReplicaLagging, re.config.BackpressureLagBytes)
	}
	return nil
}

// clearPending removes fully-replicated ops from the pendingWrites map.
func (re *ReplicationEngine) clearPending(ops []*WriteOperation) {
	re.mu.Lock()
	for _, op := range ops {
		delete(re.pendingWrites, op.SeqNum)
	}
	re.mu.Unlock()
}

// sendToReplica performs the replication RPC and records the outcome on the
// replica's ack/lag state (under re.mu, per the ReplicaInfo locking contract).
func (re *ReplicationEngine) sendToReplica(r *ReplicaInfo, ops []*WriteOperation) error {
	var batchBytes uint64
	var maxSeq uint64
	for _, op := range ops {
		batchBytes += uint64(len(op.Key) + len(op.Value))
		if op.SeqNum > maxSeq {
			maxSeq = op.SeqNum
		}
	}

	start := time.Now()
	err := re.sendReplicationRPC(r, ops)

	re.mu.Lock()
	if err == nil {
		if maxSeq > r.LastAckSeqNum {
			r.LastAckSeqNum = maxSeq
		}
		r.State = ReplicaStateSync
	} else {
		r.State = ReplicaStateFailed
	}
	re.mu.Unlock()

	if err == nil {
		r.LagBytes.Store(0)
		r.LagNs.Store(time.Since(start).Nanoseconds())
		return nil
	}
	r.LagBytes.Add(batchBytes) // un-acked bytes accumulate as lag
	r.FailureCount.Add(1)
	return err
}

// sendReplicationRPC sends a batch of operations to a replica over its
// registered transport.  A replica with no registered transport yields
// ErrNoTransport — never a fake ack — so Sync/Quorum consistency can never be
// "satisfied" by a silently dropped batch.  Single-node deployments register
// no replicas at all and never reach this path.
func (re *ReplicationEngine) sendReplicationRPC(replica *ReplicaInfo, ops []*WriteOperation) error {
	re.mu.RLock()
	client, hasClient := re.clients[replica.NodeID]
	re.mu.RUnlock()

	if !hasClient || client == nil {
		return fmt.Errorf("%w: %s", ErrNoTransport, replica.NodeID)
	}

	if err := client.Send(ops); err != nil {
		return fmt.Errorf("send to %s: %w", replica.NodeID, err)
	}
	return nil
}

// backgroundAntiEntropyWorker periodically syncs replicas
func (re *ReplicationEngine) backgroundAntiEntropyWorker() {
	ticker := time.NewTicker(re.config.AntiEntropyInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-re.done:
			return
		case <-ticker.C:
			re.runAntiEntropy()
			re.metrics.AntiEntropyRuns.Add(1)
		}
	}
}

// runAntiEntropy reconciles replica state with primary
func (re *ReplicationEngine) runAntiEntropy() {
	re.mu.Lock()
	replicas := make([]*ReplicaInfo, 0, len(re.replicaStates))
	for _, replica := range re.replicaStates {
		if replica.State == ReplicaStateLag || replica.State == ReplicaStateSync_Pending {
			replicas = append(replicas, replica)
		}
	}
	re.mu.Unlock()
	
	for _, replica := range replicas {
		// Find operations that haven't been synced to this replica
		re.mu.RLock()
		var pendingOps []*WriteOperation
		for seqNum, op := range re.pendingWrites {
			if seqNum > replica.LastAckSeqNum {
				pendingOps = append(pendingOps, op)
			}
		}
		re.mu.RUnlock()
		
		if len(pendingOps) > 0 {
			// sendToReplica records ack progress so a caught-up replica
			// transitions back to ReplicaStateSync.
			re.sendToReplica(replica, pendingOps)
		}
	}
}

// backgroundLagMonitor tracks replica lag
func (re *ReplicationEngine) backgroundLagMonitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-re.done:
			return
		case <-ticker.C:
			re.mu.Lock()
			totalLagBytes := uint64(0)
			for _, replica := range re.replicaStates {
				totalLagBytes += replica.LagBytes.Load()
			}
			re.metrics.ReplicaLagBytes.Store(totalLagBytes)
			re.mu.Unlock()
		}
	}
}

// StartReplicationServer starts a ReplicationServer on listenAddr that applies
// received operations via applyFn.  The server is stopped when Close() is called.
// Call this once on every node that will act as a replica.
//
// When ReplicationConfig.TLS is enabled the server accepts TLS 1.3 connections
// only; otherwise it listens on plaintext TCP (default).
func (re *ReplicationEngine) StartReplicationServer(listenAddr string, applyFn ApplyFn) error {
	var srv *ReplicationServer
	var err error
	if re.config != nil && re.config.TLS.enabled() {
		srv, err = NewReplicationServerTLS(listenAddr, applyFn, re.config.TLS)
	} else {
		srv, err = NewReplicationServer(listenAddr, applyFn)
	}
	if err != nil {
		return err
	}
	go srv.ListenAndServe()
	// Stop the server when the engine is closed.
	go func() {
		<-re.done
		srv.Stop()
	}()
	return nil
}

// Close shuts down the replication engine and all TCP clients.
func (re *ReplicationEngine) Close() error {
	close(re.done)
	re.mu.Lock()
	for _, c := range re.clients {
		c.Close()
	}
	re.mu.Unlock()
	return nil
}

// GetMetrics returns the replication metrics for external observation.
func (re *ReplicationEngine) GetMetrics() *ReplicationMetrics { return re.metrics }

// UpdateVersionVector updates causal clock for a node
func (vv *VersionVector) UpdateVersion(nodeID string) {
	vv.mu.Lock()
	defer vv.mu.Unlock()
	vv.Clock[nodeID]++
}

// HappenedBefore checks if vv1 causally precedes vv2
func (vv *VersionVector) HappenedBefore(other *VersionVector) bool {
	vv.mu.RLock()
	other.mu.RLock()
	defer vv.mu.RUnlock()
	defer other.mu.RUnlock()
	
	atLeastOneLess := false
	for node := range vv.Clock {
		if vv.Clock[node] > other.Clock[node] {
			return false
		}
		if vv.Clock[node] < other.Clock[node] {
			atLeastOneLess = true
		}
	}
	return atLeastOneLess
}

// Concurrent detects concurrent writes (causally unrelated)
func (vv *VersionVector) Concurrent(other *VersionVector) bool {
	vv.mu.RLock()
	other.mu.RLock()
	defer vv.mu.RUnlock()
	defer other.mu.RUnlock()
	
	// Check if neither happens-before the other
	vvLess := false
	otherLess := false
	
	for node := range vv.Clock {
		if vv.Clock[node] < other.Clock[node] {
			vvLess = true
		} else if vv.Clock[node] > other.Clock[node] {
			otherLess = true
		}
	}
	
	return vvLess && otherLess
}
