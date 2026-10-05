package replication

import (
	"errors"
	"fmt"
	"log"
	"sort"
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

// ReplicaState represents the state of a replica.
//
// Lifecycle:
//
//	SYNC ──send fails──► FAILED ──probe ok (recovery worker, backoff)──► SYNC_PENDING
//	  ▲                    ▲                                                │
//	  │                    └──────────── catch-up send fails ───────────────┤
//	  └──────────── catch-up done (no retained op left un-acked) ───────────┘
//
// LAG is set by backpressure only and returns to SYNC on the next successful
// send.  FAILED and SYNC_PENDING replicas receive no live batches: the ops
// they miss stay in the retention buffer and are replayed by recovery.
type ReplicaState int

const (
	ReplicaStateSync ReplicaState = iota
	ReplicaStateSync_Pending
	ReplicaStateLag
	ReplicaStateFailed
)

// ReplicationMetrics tracks replication statistics
type ReplicationMetrics struct {
	ReplicatedWrites   atomic.Uint64
	FailedReplications atomic.Uint64
	ReplicaLagBytes    atomic.Uint64
	// ReplicaLagNs is the maximum, across replicas, of the age of the oldest
	// write that replica has not acknowledged (0 when every replica is caught
	// up).  Refreshed by the lag monitor every second.
	ReplicaLagNs    atomic.Int64
	AntiEntropyRuns atomic.Uint64
	// WriteQueueOverflows counts ops that found the write queue full and were
	// left to anti-entropy instead of the live send path.
	WriteQueueOverflows atomic.Uint64
	// BackpressureEvents counts Sync/Quorum batches in which at least one
	// replica's lag exceeded ReplicationConfig.BackpressureLagBytes.
	BackpressureEvents atomic.Uint64

	// ReplicasFailed / ReplicasSyncPending are gauges (replica counts in each
	// state), refreshed by the lag monitor.
	ReplicasFailed      atomic.Int64
	ReplicasSyncPending atomic.Int64
	// RecoveryProbes counts reconnect attempts against FAILED replicas;
	// ReplicaRecoveries counts replicas that went FAILED → SYNC_PENDING → SYNC.
	RecoveryProbes    atomic.Uint64
	ReplicaRecoveries atomic.Uint64
	// CatchUpGaps counts recoveries that could not replay everything the
	// replica missed (retained ops were evicted and no catch-up source is
	// configured).  Always 0 in cmd/server, which sets a catch-up source.
	CatchUpGaps atomic.Uint64
}

// VersionVector is a per-node logical clock map.
//
// Reserved: the engine never sets WriteOperation.VersionVector, never
// compares vectors and runs no conflict resolution — replicas apply ops in
// arrival order.  The type and the wire field are kept so the gob wire format
// and the exported API stay stable.
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
	VersionVector *VersionVector // reserved: never set (see VersionVector)
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

	// RecoveryProbeInterval is the first reconnect delay for a FAILED
	// replica; it doubles after each failed probe up to RecoveryMaxBackoff.
	// Zero means defaultRecoveryProbeInterval (1s).
	RecoveryProbeInterval time.Duration
	// RecoveryMaxBackoff caps the reconnect delay.  Zero means
	// defaultRecoveryMaxBackoff (30s).
	RecoveryMaxBackoff time.Duration
	// MaxRetainedOps bounds the retention buffer (ops not yet acknowledged
	// by every registered replica).  When it overflows the oldest ops are
	// dropped and every replica that had not acked them is flagged for a
	// catch-up from the CatchUpFunc source on recovery.  Zero means
	// defaultMaxRetainedOps (100000).
	MaxRetainedOps int
}

const (
	defaultRecoveryProbeInterval = 1 * time.Second
	defaultRecoveryMaxBackoff    = 30 * time.Second
	defaultMaxRetainedOps        = 100000

	// catchUpSkew is subtracted from the resync start time when reading the
	// catch-up source, and watermarkMargin from every reported watermark.
	// op.Timestamp is taken after the local write, so the storage entry's
	// write timestamp can be slightly older; replays are idempotent, so being
	// generous costs only re-sent keys.
	catchUpSkew     = 10 * time.Second
	watermarkMargin = 10 * time.Second
)

// CatchUpFunc streams the primary's current state for every key written at or
// after sinceUnixNano (tombstones included) to emit, in pages.  emit returns
// an error when the replica fails to apply a page; the function must then
// stop and return it.  cmd/server implements it with
// StorageEngine.ChangesSince.
type CatchUpFunc func(sinceUnixNano int64, emit func(ops []*WriteOperation) error) error

// WatermarkObserver receives, once per lag-monitor tick, each replica's
// acknowledgement watermark in Unix microseconds: the replica has applied
// every write this node originated with an earlier timestamp.  cmd/server
// forwards it to StorageEngine.SetReplicaWatermark (tombstone GC).
type WatermarkObserver func(replicaID string, watermarkUs int64)

// Pinger is optionally implemented by a ReplicaTransport: Ping performs one
// round trip with the replica without applying anything.  The recovery worker
// uses it to probe FAILED replicas; transports without it are probed by the
// catch-up send itself.
type Pinger interface {
	Ping() error
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
	mu          sync.RWMutex
	config      *ReplicationConfig
	localNodeID string
	writeQueue  chan *WriteOperation
	// aeKick asks the anti-entropy worker for an immediate run (buffered 1,
	// sends never block). OnLocalWrite kicks it when the write queue is full.
	aeKick            chan struct{}
	replicaStates     map[string]*ReplicaInfo
	metrics           *ReplicationMetrics
	done              chan struct{}
	pendingWrites     map[uint64]*pendingOp
	lastReplicationTs int64

	// clients holds one replication transport per replica nodeID.
	// Populated by AddReplica (real TCP/TLS client) or SetReplicaTransport
	// (custom/test transport).
	clients map[string]ReplicaTransport

	catchUp   CatchUpFunc       // optional; guarded by mu
	watermark WatermarkObserver // optional; guarded by mu
}

// pendingOp is one retained write and the set of replicas that acked it.  It
// leaves pendingWrites once every registered replica has acked it (or when
// the retention buffer overflows).
type pendingOp struct {
	op    *WriteOperation
	acked map[string]struct{}
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
	NodeID        string
	Address       string
	Port          int
	State         ReplicaState
	LastAckSeqNum uint64
	LagBytes      atomic.Uint64
	// LagNs is the age of the oldest write this replica has not acked (0 when
	// caught up), refreshed by the lag monitor every second.
	LagNs        atomic.Int64
	FailureCount atomic.Uint64
	SyncedAt     int64

	// Recovery bookkeeping, guarded by ReplicationEngine.mu.
	lastAckAt   int64         // UnixNano of the last successful ack
	nextProbeAt int64         // UnixNano before which no probe is attempted
	backoff     time.Duration // current reconnect delay
	recovering  bool          // a recovery attempt is in progress
	// needsResync: ops this replica never acked were dropped from the
	// retention buffer; recovery must read the catch-up source from
	// resyncFromNs (the oldest dropped op's timestamp).
	needsResync  bool
	resyncFromNs int64
}

// NewReplicationEngine creates a new replication engine
func NewReplicationEngine(nodeID string, config *ReplicationConfig) *ReplicationEngine {
	return &ReplicationEngine{
		localNodeID:   nodeID,
		config:        config,
		writeQueue:    make(chan *WriteOperation, 10000),
		aeKick:        make(chan struct{}, 1),
		replicaStates: make(map[string]*ReplicaInfo),
		metrics:       &ReplicationMetrics{},
		done:          make(chan struct{}),
		pendingWrites: make(map[uint64]*pendingOp),
		clients:       make(map[string]ReplicaTransport),
	}
}

// Start begins the replication engine
func (re *ReplicationEngine) Start() {
	go re.backgroundReplicationWorker()
	go re.backgroundAntiEntropyWorker()
	go re.backgroundLagMonitor()
	go re.backgroundRecoveryWorker()
}

// SetCatchUpSource installs the source recovery reads when a replica missed
// ops that are no longer retained.  Nil disables it.
func (re *ReplicationEngine) SetCatchUpSource(fn CatchUpFunc) {
	re.mu.Lock()
	re.catchUp = fn
	re.mu.Unlock()
}

// SetWatermarkObserver installs the per-replica watermark callback (see
// WatermarkObserver).  Nil disables it.
func (re *ReplicationEngine) SetWatermarkObserver(fn WatermarkObserver) {
	re.mu.Lock()
	re.watermark = fn
	re.mu.Unlock()
}

func (re *ReplicationEngine) probeInterval() time.Duration {
	if re.config != nil && re.config.RecoveryProbeInterval > 0 {
		return re.config.RecoveryProbeInterval
	}
	return defaultRecoveryProbeInterval
}

func (re *ReplicationEngine) maxBackoff() time.Duration {
	if re.config != nil && re.config.RecoveryMaxBackoff > 0 {
		return re.config.RecoveryMaxBackoff
	}
	return defaultRecoveryMaxBackoff
}

func (re *ReplicationEngine) maxRetained() int {
	if re.config != nil && re.config.MaxRetainedOps > 0 {
		return re.config.MaxRetainedOps
	}
	return defaultMaxRetainedOps
}

func (re *ReplicationEngine) batchSize() int {
	if re.config != nil && re.config.BatchSize > 0 {
		return re.config.BatchSize
	}
	return 100
}

// OnLocalWrite handles a write on the primary node.  It never blocks: the op
// is queued for the background replication worker.  Callers that need Sync or
// Quorum semantics on the write path should follow up with WaitForReplication.
//
// The op is registered in pendingWrites before it is queued so that
// WaitForReplication called immediately after OnLocalWrite observes it.
func (re *ReplicationEngine) OnLocalWrite(op *WriteOperation) error {
	re.mu.Lock()
	re.pendingWrites[op.SeqNum] = &pendingOp{op: op, acked: make(map[string]struct{})}
	if len(re.pendingWrites) > re.maxRetained() {
		re.evictOldestLocked()
	}
	re.mu.Unlock()

	select {
	case re.writeQueue <- op:
		return nil
	case <-re.done:
		re.dropPending(op.SeqNum)
		return fmt.Errorf("replication engine stopped")
	default:
		// The write is already durable locally, so dropping the op here would
		// leave every replica permanently missing it. Keep it retained, mark
		// healthy replicas LAG and kick anti-entropy, which re-sends every
		// retained op a LAG replica has not acked (and returns it to SYNC once
		// caught up). WaitForReplication still sees the op, so Quorum/Strong
		// writes wait for the ack as usual.
		re.mu.Lock()
		for _, r := range re.replicaStates {
			if r.State == ReplicaStateSync {
				r.State = ReplicaStateLag
			}
		}
		re.mu.Unlock()
		re.metrics.WriteQueueOverflows.Add(1)
		select {
		case re.aeKick <- struct{}{}:
		default:
		}
		return nil
	}
}

func (re *ReplicationEngine) dropPending(seqNum uint64) {
	re.mu.Lock()
	delete(re.pendingWrites, seqNum)
	re.mu.Unlock()
}

// evictOldestLocked drops the oldest ~10% of retained ops (by SeqNum) once the
// retention buffer exceeds MaxRetainedOps.  Every replica that had not acked
// a dropped op is flagged needsResync so recovery reads the catch-up source.
// Caller holds re.mu (write).
func (re *ReplicationEngine) evictOldestLocked() {
	n := len(re.pendingWrites) / 10
	if n < 1 {
		n = 1
	}
	seqs := make([]uint64, 0, len(re.pendingWrites))
	for s := range re.pendingWrites {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, s := range seqs[:n] {
		p := re.pendingWrites[s]
		delete(re.pendingWrites, s)
		for id, r := range re.replicaStates {
			if _, ok := p.acked[id]; !ok {
				re.flagResyncLocked(r, p.op.Timestamp)
			}
		}
	}
}

// flagResyncLocked records that r missed a write (timestamp tsNs) that is no
// longer retained.  Caller holds re.mu (write).
func (re *ReplicationEngine) flagResyncLocked(r *ReplicaInfo, tsNs int64) {
	if !r.needsResync || tsNs < r.resyncFromNs {
		r.resyncFromNs = tsNs
	}
	r.needsResync = true
}

// fullyAckedLocked reports whether every registered replica acked p.
func (re *ReplicationEngine) fullyAckedLocked(p *pendingOp) bool {
	for id := range re.replicaStates {
		if _, ok := p.acked[id]; !ok {
			return false
		}
	}
	return true
}

// recordAcksLocked marks ops as acked by r, drops fully-acked ops from the
// retention buffer and advances r's ack progress.  Caller holds re.mu (write).
func (re *ReplicationEngine) recordAcksLocked(r *ReplicaInfo, ops []*WriteOperation, now int64) {
	for _, op := range ops {
		if op.SeqNum > r.LastAckSeqNum {
			r.LastAckSeqNum = op.SeqNum
		}
		p, ok := re.pendingWrites[op.SeqNum]
		if !ok || p.op != op {
			continue // catch-up op (never retained) or already dropped
		}
		p.acked[r.NodeID] = struct{}{}
		if re.fullyAckedLocked(p) {
			delete(re.pendingWrites, op.SeqNum)
		}
	}
	r.lastAckAt = now
}

// unackedLocked returns the retained ops r has not acked, in SeqNum order.
func (re *ReplicationEngine) unackedLocked(nodeID string) []*WriteOperation {
	var out []*WriteOperation
	for _, p := range re.pendingWrites {
		if _, ok := p.acked[nodeID]; !ok {
			out = append(out, p.op)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SeqNum < out[j].SeqNum })
	return out
}

// markFailedLocked moves r to FAILED (from any other state) and schedules the
// first recovery probe.  Caller holds re.mu (write).
func (re *ReplicationEngine) markFailedLocked(r *ReplicaInfo, now int64) {
	if r.State == ReplicaStateFailed {
		return
	}
	r.State = ReplicaStateFailed
	if r.backoff <= 0 {
		r.backoff = re.probeInterval()
	}
	r.nextProbeAt = now + int64(r.backoff)
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
		lastAckAt:     time.Now().UnixNano(),
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
	// Ops that were only waiting for this replica are now fully acked.
	for seq, p := range re.pendingWrites {
		if re.fullyAckedLocked(p) {
			delete(re.pendingWrites, seq)
		}
	}
	return nil
}

// WaitForReplication waits until seqNum has been acked by enough replicas
// that targetReplicas total copies exist (the local write counts as one).
// Returns nil on success, an ErrReplicationTimeout-wrapping error on deadline
// expiry, or "write not found" if the op was never (or is no longer tracked
// as) pending and the target has not been met.
//
// While the op is retained, progress is the set of replicas that acked this
// exact op (a replica that acked a later op does not count: concurrent writers
// can enqueue seq N+1 before seq N).  A replica that acked it and later
// transitioned to LAG/FAILED still counts, because the data is durably on it.
// Once the op has left the retention buffer (every replica acked it, or it
// was evicted) progress falls back to LastAckSeqNum.
func (re *ReplicationEngine) WaitForReplication(seqNum uint64, targetReplicas int, maxWaitMs int) error {
	deadline := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)

	for {
		re.mu.RLock()
		syncedCount := 0
		p, pending := re.pendingWrites[seqNum]
		if pending {
			syncedCount = len(p.acked)
		} else {
			for _, replica := range re.replicaStates {
				if replica.LastAckSeqNum >= seqNum {
					syncedCount++
				}
			}
		}
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
			LastAckAt:     replica.lastAckAt,
			NextProbeAt:   nextProbe(replica),
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
	LagNs         int64 // age of the oldest write the replica has not acked
	LastAckAt     int64 // UnixNano of the last successful ack (AddReplica time before the first)
	NextProbeAt   int64 // UnixNano of the next reconnect probe; 0 unless FAILED
}

func nextProbe(r *ReplicaInfo) int64 {
	if r.State != ReplicaStateFailed {
		return 0
	}
	return r.nextProbeAt
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

			// OnLocalWrite registered the op; re-register only if it was
			// injected straight into the queue.
			re.mu.Lock()
			if _, ok := re.pendingWrites[op.SeqNum]; !ok {
				re.pendingWrites[op.SeqNum] = &pendingOp{op: op, acked: make(map[string]struct{})}
			}
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

// replicateBatch sends a batch of writes to all live replicas (SYNC or LAG —
// FAILED and SYNC_PENDING replicas are caught up by the recovery worker) and
// waits according to the configured consistency level, using per-batch ack
// accounting (each replica goroutine reports exactly one result on a private
// channel — the decision never inspects a channel that is still being filled):
//
//	StrongConsistency   — every live replica must ack; any failure or an
//	                      expired deadline is an error.
//	QuorumConsistency   — a majority of ReplicationFactor copies must ack,
//	                      counting the local write as one copy.  Fails fast
//	                      (ErrQuorumNotReached) as soon as enough replicas
//	                      have failed that the majority is unreachable;
//	                      otherwise errors with ErrReplicationTimeout at the
//	                      deadline.
//	EventualConsistency — fire and forget; never blocks.
//
// Per-op acks are recorded by sendToReplica; an op leaves the retention
// buffer once every registered replica (live or not) has acked it.
//
// With no replicas registered (single-node default) every level is trivially
// satisfied by the local write and the call returns nil immediately.
func (re *ReplicationEngine) replicateBatch(ops []*WriteOperation) error {
	if len(ops) == 0 {
		return nil
	}

	re.mu.RLock()
	registered := len(re.replicaStates)
	replicas := make([]*ReplicaInfo, 0, len(re.replicaStates))
	for _, replica := range re.replicaStates {
		if replica.State == ReplicaStateSync || replica.State == ReplicaStateLag {
			replicas = append(replicas, replica)
		}
	}
	re.mu.RUnlock()

	re.metrics.ReplicatedWrites.Add(uint64(len(ops)))

	// Single-node mode: the local write is the only copy.
	if registered == 0 {
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
			// Not enough live replicas to ever reach quorum.
			re.metrics.FailedReplications.Add(1)
			return &quorumError{
				acked:    0,
				required: requiredAcks,
				total:    len(replicas),
				cause:    fmt.Errorf("only %d live replicas (%d registered)", len(replicas), registered),
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

	if lagging {
		return fmt.Errorf("%w (threshold %d bytes)", ErrReplicaLagging, re.config.BackpressureLagBytes)
	}
	return nil
}

// clearPending removes ops from the retention buffer (single-node mode).
func (re *ReplicationEngine) clearPending(ops []*WriteOperation) {
	re.mu.Lock()
	for _, op := range ops {
		delete(re.pendingWrites, op.SeqNum)
	}
	re.mu.Unlock()
}

func batchStats(ops []*WriteOperation) (bytes uint64, minTs int64) {
	for i, op := range ops {
		bytes += uint64(len(op.Key) + len(op.Value))
		if i == 0 || op.Timestamp < minTs {
			minTs = op.Timestamp
		}
	}
	return bytes, minTs
}

// sendToReplica performs a live-path replication RPC and records the outcome
// on the replica's ack/lag state (under re.mu, per the ReplicaInfo locking
// contract).  Success returns a LAG replica to SYNC but never revives a
// FAILED / SYNC_PENDING one — only the recovery worker does that, after
// replaying what the replica missed.  Failure moves the replica to FAILED.
func (re *ReplicationEngine) sendToReplica(r *ReplicaInfo, ops []*WriteOperation) error {
	batchBytes, _ := batchStats(ops)
	err := re.sendReplicationRPC(r, ops)
	now := time.Now().UnixNano()

	re.mu.Lock()
	if err == nil {
		re.recordAcksLocked(r, ops, now)
		if r.State == ReplicaStateLag {
			r.State = ReplicaStateSync
		}
	} else {
		if r.State != ReplicaStateFailed {
			log.Printf("[repl] replica %s FAILED: %v", r.NodeID, err)
		}
		re.markFailedLocked(r, now)
		// Ops of this batch that are no longer retained cannot be replayed:
		// recovery must read the catch-up source for them.
		for _, op := range ops {
			if p, ok := re.pendingWrites[op.SeqNum]; !ok || p.op != op {
				re.flagResyncLocked(r, op.Timestamp)
			}
		}
	}
	re.mu.Unlock()

	if err == nil {
		r.LagBytes.Store(0)
		return nil
	}
	r.LagBytes.Add(batchBytes) // un-acked bytes accumulate as lag
	r.FailureCount.Add(1)
	return err
}

// deliver sends ops to r for the recovery / anti-entropy paths: acks are
// recorded, but the replica's state is left to the caller.
func (re *ReplicationEngine) deliver(r *ReplicaInfo, ops []*WriteOperation) error {
	if len(ops) == 0 {
		return nil
	}
	if err := re.sendReplicationRPC(r, ops); err != nil {
		r.FailureCount.Add(1)
		return err
	}
	re.mu.Lock()
	re.recordAcksLocked(r, ops, time.Now().UnixNano())
	re.mu.Unlock()
	r.LagBytes.Store(0)
	return nil
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
		case <-re.aeKick:
			re.runAntiEntropy()
			re.metrics.AntiEntropyRuns.Add(1)
		}
	}
}

// runAntiEntropy re-sends to LAG replicas every retained op they have not
// acked (FAILED / SYNC_PENDING replicas belong to the recovery worker).
func (re *ReplicationEngine) runAntiEntropy() {
	re.mu.Lock()
	replicas := make([]*ReplicaInfo, 0, len(re.replicaStates))
	for _, replica := range re.replicaStates {
		if replica.State == ReplicaStateLag {
			replicas = append(replicas, replica)
		}
	}
	re.mu.Unlock()

	for _, replica := range replicas {
		re.mu.RLock()
		pendingOps := re.unackedLocked(replica.NodeID)
		re.mu.RUnlock()

		if len(pendingOps) > 0 {
			// sendToReplica records ack progress so a caught-up replica
			// transitions back to ReplicaStateSync.
			re.sendToReplica(replica, pendingOps)
		}
	}
}

// ── Recovery of FAILED replicas ──────────────────────────────────────────────

// backgroundRecoveryWorker re-probes FAILED replicas.  Each replica has its
// own reconnect backoff (RecoveryProbeInterval doubling to RecoveryMaxBackoff).
func (re *ReplicationEngine) backgroundRecoveryWorker() {
	tick := re.probeInterval() / 4
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	if tick > time.Second {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-re.done:
			return
		case <-ticker.C:
			re.recoverFailedReplicas(time.Now())
		}
	}
}

// recoverFailedReplicas runs one recovery attempt (in parallel) for every
// FAILED replica whose backoff has elapsed, and waits for them.
func (re *ReplicationEngine) recoverFailedReplicas(now time.Time) {
	nowNs := now.UnixNano()
	re.mu.Lock()
	var due []*ReplicaInfo
	for _, r := range re.replicaStates {
		if r.State == ReplicaStateFailed && !r.recovering && nowNs >= r.nextProbeAt {
			r.recovering = true
			due = append(due, r)
		}
	}
	re.mu.Unlock()

	var wg sync.WaitGroup
	for _, r := range due {
		wg.Add(1)
		go func(r *ReplicaInfo) {
			defer wg.Done()
			re.recoverReplica(r)
			re.mu.Lock()
			r.recovering = false
			re.mu.Unlock()
		}(r)
	}
	wg.Wait()
}

// recoveryFailed returns r to FAILED and doubles its reconnect backoff.
func (re *ReplicationEngine) recoveryFailed(r *ReplicaInfo, err error) {
	now := time.Now().UnixNano()
	re.mu.Lock()
	r.State = ReplicaStateFailed
	if r.backoff <= 0 {
		r.backoff = re.probeInterval()
	} else {
		r.backoff *= 2
	}
	if max := re.maxBackoff(); r.backoff > max {
		r.backoff = max
	}
	r.nextProbeAt = now + int64(r.backoff)
	backoff := r.backoff
	re.mu.Unlock()
	log.Printf("[repl] replica %s recovery failed (next probe in %s): %v", r.NodeID, backoff, err)
}

// recoverReplica probes r and, if it answers, moves it to SYNC_PENDING,
// replays what it missed — the catch-up source first when retained ops were
// dropped, then every retained op it has not acked, in SeqNum order — and
// moves it to SYNC once nothing retained is left un-acked.
func (re *ReplicationEngine) recoverReplica(r *ReplicaInfo) {
	re.metrics.RecoveryProbes.Add(1)

	re.mu.RLock()
	_, registered := re.replicaStates[r.NodeID]
	client := re.clients[r.NodeID]
	src := re.catchUp
	re.mu.RUnlock()
	if !registered {
		return
	}
	if client == nil {
		re.recoveryFailed(r, fmt.Errorf("%w: %s", ErrNoTransport, r.NodeID))
		return
	}
	if p, ok := client.(Pinger); ok {
		if err := p.Ping(); err != nil {
			re.recoveryFailed(r, err)
			return
		}
	}

	re.mu.Lock()
	if re.replicaStates[r.NodeID] != r || r.State != ReplicaStateFailed {
		re.mu.Unlock()
		return
	}
	r.State = ReplicaStateSync_Pending
	needResync, since := r.needsResync, r.resyncFromNs
	re.mu.Unlock()
	log.Printf("[repl] replica %s reachable — SYNC_PENDING (resync=%v)", r.NodeID, needResync)

	if needResync {
		if src != nil {
			// Clear the flag first: ops dropped while the catch-up runs set
			// it again, with their own (later) timestamp.
			re.mu.Lock()
			r.needsResync = false
			re.mu.Unlock()
			err := src(since-int64(catchUpSkew), func(ops []*WriteOperation) error {
				return re.deliver(r, ops)
			})
			if err != nil {
				re.mu.Lock()
				re.flagResyncLocked(r, since)
				re.mu.Unlock()
				re.recoveryFailed(r, fmt.Errorf("catch-up: %w", err))
				return
			}
		} else {
			re.metrics.CatchUpGaps.Add(1)
			log.Printf("[repl] replica %s missed writes that are no longer retained and no catch-up source is set — it may be missing data", r.NodeID)
			re.mu.Lock()
			r.needsResync = false
			re.mu.Unlock()
		}
	}

	bs := re.batchSize()
	for {
		select {
		case <-re.done:
			return
		default:
		}
		re.mu.Lock()
		ops := re.unackedLocked(r.NodeID)
		if len(ops) == 0 {
			if r.needsResync {
				// Ops were dropped during catch-up; go round again.
				r.State = ReplicaStateFailed
				r.nextProbeAt = 0
				re.mu.Unlock()
				return
			}
			// Nothing retained is un-acked.  Any op registered after this
			// point is batched with a replica snapshot taken after the flip,
			// so live replication covers it.
			r.State = ReplicaStateSync
			r.backoff = 0
			r.SyncedAt = time.Now().UnixNano()
			re.mu.Unlock()
			re.metrics.ReplicaRecoveries.Add(1)
			log.Printf("[repl] replica %s recovered — SYNC", r.NodeID)
			return
		}
		re.mu.Unlock()
		for i := 0; i < len(ops); i += bs {
			end := i + bs
			if end > len(ops) {
				end = len(ops)
			}
			if err := re.deliver(r, ops[i:end]); err != nil {
				re.recoveryFailed(r, err)
				return
			}
		}
	}
}

// ── Lag / watermark monitor ──────────────────────────────────────────────────

// backgroundLagMonitor refreshes replica lag, state gauges and tombstone
// watermarks every second.
func (re *ReplicationEngine) backgroundLagMonitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-re.done:
			return
		case <-ticker.C:
			re.updateLag(time.Now())
		}
	}
}

// updateLag computes, per replica, the age of the oldest write it has not
// acked (LagNs) and its watermark — every write with an earlier timestamp is
// on the replica — and publishes the aggregates and the watermarks.
func (re *ReplicationEngine) updateLag(now time.Time) {
	nowNs := now.UnixNano()
	type wm struct {
		id string
		us int64
	}
	var marks []wm

	re.mu.RLock()
	oldest := make(map[string]int64, len(re.replicaStates))
	for _, p := range re.pendingWrites {
		for id := range re.replicaStates {
			if _, ok := p.acked[id]; ok {
				continue
			}
			if cur, ok := oldest[id]; !ok || p.op.Timestamp < cur {
				oldest[id] = p.op.Timestamp
			}
		}
	}
	var totalLagBytes uint64
	var maxLag int64
	var failed, syncPending int64
	for id, r := range re.replicaStates {
		totalLagBytes += r.LagBytes.Load()
		switch r.State {
		case ReplicaStateFailed:
			failed++
		case ReplicaStateSync_Pending:
			syncPending++
		}
		behindSince := nowNs
		if ts, ok := oldest[id]; ok && ts < behindSince {
			behindSince = ts
		}
		if r.needsResync && r.resyncFromNs < behindSince {
			behindSince = r.resyncFromNs
		}
		lag := nowNs - behindSince
		if lag < 0 {
			lag = 0
		}
		r.LagNs.Store(lag)
		if lag > maxLag {
			maxLag = lag
		}
		marks = append(marks, wm{id: id, us: (behindSince - int64(watermarkMargin)) / 1000})
	}
	obs := re.watermark
	re.mu.RUnlock()

	re.metrics.ReplicaLagBytes.Store(totalLagBytes)
	re.metrics.ReplicaLagNs.Store(maxLag)
	re.metrics.ReplicasFailed.Store(failed)
	re.metrics.ReplicasSyncPending.Store(syncPending)
	if obs != nil {
		for _, m := range marks {
			obs(m.id, m.us)
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
