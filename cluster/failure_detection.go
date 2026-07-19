package cluster

import (
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// FailureDetector monitors node health and detects failures
type FailureDetector struct {
	mu                    sync.RWMutex
	localNodeID           string           // never marked failed — set by SetLocalNode
	nodeHeartbeats        map[string]int64 // node_id → last_heartbeat_ns
	suspectedNodes        map[string]bool
	failedNodes           map[string]bool
	config                *FailureDetectorConfig
	partitionMap          *PartitionMap
	metrics               *FailureDetectionMetrics
	done                  chan struct{}
	closeOnce             sync.Once
	recoveryQueue         chan string // Failed nodes to recover
	lastPartitionMapCheck int64
}

// FailureDetectorConfig contains failure detector configuration
type FailureDetectorConfig struct {
	HeartbeatInterval    time.Duration // How often to check for failures
	SuspectThreshold     time.Duration // Time before marking as suspect
	FailureThreshold     time.Duration // Time before marking as failed
	MaxSuspectTime       time.Duration // Max time a node can be suspected
	RecoveryInterval     time.Duration // How often to attempt recovery
	MaxRecoveryRetries   int
	PingTimeout          time.Duration // Dial + exchange timeout for pingNode; 0 → defaultPingTimeout
}

// defaultPingTimeout is used when FailureDetectorConfig.PingTimeout is zero
// (e.g. configs built before the field existed).
const defaultPingTimeout = 2 * time.Second

// DefaultFailureDetectorConfig returns sensible defaults
func DefaultFailureDetectorConfig() *FailureDetectorConfig {
	return &FailureDetectorConfig{
		HeartbeatInterval:  1 * time.Second,
		SuspectThreshold:   3 * time.Second,
		FailureThreshold:   10 * time.Second,
		MaxSuspectTime:     15 * time.Second,
		RecoveryInterval:   5 * time.Second,
		MaxRecoveryRetries: 3,
		PingTimeout:        defaultPingTimeout,
	}
}

// FailureDetectionMetrics tracks failure detection statistics
type FailureDetectionMetrics struct {
	NodesDetected     atomic.Uint64 // Nodes detected as failed
	NodesRecovered    atomic.Uint64 // Nodes that recovered
	FalsePositives    atomic.Uint64 // False failure detections
	RecoveryAttempts  atomic.Uint64
	RecoverySuccess   atomic.Uint64
	RecoveryFailures  atomic.Uint64
}

// NewFailureDetector creates a new failure detector
func NewFailureDetector(pm *PartitionMap, config *FailureDetectorConfig) *FailureDetector {
	return &FailureDetector{
		nodeHeartbeats: make(map[string]int64),
		suspectedNodes: make(map[string]bool),
		failedNodes:    make(map[string]bool),
		config:         config,
		partitionMap:   pm,
		metrics:        &FailureDetectionMetrics{},
		done:           make(chan struct{}),
		recoveryQueue:  make(chan string, 100),
	}
}

// SetLocalNode registers the ID of the node running this detector.
// The local node is exempt from failure detection — it cannot heartbeat itself
// via the gossip protocol, so its heartbeat timestamp starts at 0, which would
// otherwise trigger an immediate false-positive FAILED detection on every check.
// Call this once after NewFailureDetector and before Start.
func (fd *FailureDetector) SetLocalNode(nodeID string) {
	fd.mu.Lock()
	fd.localNodeID = nodeID
	// Initialise timestamp so the first health check does not see a stale zero.
	fd.nodeHeartbeats[nodeID] = time.Now().UnixNano()
	fd.mu.Unlock()
}

// Start begins the failure detector
func (fd *FailureDetector) Start() {
	go fd.backgroundHeartbeatChecker()
	go fd.backgroundRecoveryWorker()
}

// RecordHeartbeat records a heartbeat from a node
func (fd *FailureDetector) RecordHeartbeat(nodeID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	
	fd.nodeHeartbeats[nodeID] = time.Now().UnixNano()
	
	// Clear suspect state if was suspected
	if fd.suspectedNodes[nodeID] {
		delete(fd.suspectedNodes, nodeID)
		fd.partitionMap.UpdateNodeState(nodeID, NodeStateActive)
		fd.metrics.FalsePositives.Add(1)
	}
	
	// Clear failed state if was failed (recovery)
	if fd.failedNodes[nodeID] {
		delete(fd.failedNodes, nodeID)
		fd.metrics.NodesRecovered.Add(1)
		fd.partitionMap.UpdateNodeState(nodeID, NodeStateRecovering)
	}
}

// backgroundHeartbeatChecker periodically checks node health
func (fd *FailureDetector) backgroundHeartbeatChecker() {
	ticker := time.NewTicker(fd.config.HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-fd.done:
			return
		case <-ticker.C:
			fd.checkNodeHealth()
		}
	}
}

// checkNodeHealth checks health of all nodes
func (fd *FailureDetector) checkNodeHealth() {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	
	now := time.Now().UnixNano()
	suspectThresholdNs := fd.config.SuspectThreshold.Nanoseconds()
	failureThresholdNs := fd.config.FailureThreshold.Nanoseconds()
	_ = fd.config.MaxSuspectTime.Nanoseconds() // reserved for future eviction policy
	
	// Get all nodes from partition map (skip the local node — it heartbeats itself
	// via SetLocalNode refresh, not via gossip, so skipping avoids false positives).
	for nodeID, node := range fd.partitionMap.Nodes {
		if nodeID == fd.localNodeID {
			continue
		}
		lastHeartbeat := fd.nodeHeartbeats[nodeID]
		if lastHeartbeat == 0 {
			lastHeartbeat = node.LastHeartbeat
		}
		
		timeSinceHeartbeat := now - lastHeartbeat
		currentState := node.State.Load().(NodeState)
		
		// No heartbeat for a while
		if timeSinceHeartbeat > failureThresholdNs {
			if currentState != NodeStateFailed {
				// Mark as failed
				fd.partitionMap.UpdateNodeState(nodeID, NodeStateFailed)
				fd.failedNodes[nodeID] = true
				delete(fd.suspectedNodes, nodeID)
				fd.metrics.NodesDetected.Add(1)
				
				// Queue for recovery attempts
				select {
				case fd.recoveryQueue <- nodeID:
				default:
				}
			}
		} else if timeSinceHeartbeat > suspectThresholdNs {
			if currentState != NodeStateSuspect && currentState != NodeStateFailed {
				// Mark as suspect
				fd.partitionMap.UpdateNodeState(nodeID, NodeStateSuspect)
				fd.suspectedNodes[nodeID] = true
			}
		} else if currentState == NodeStateSuspect && timeSinceHeartbeat < suspectThresholdNs {
			// Recover from suspect state
			fd.partitionMap.UpdateNodeState(nodeID, NodeStateActive)
			delete(fd.suspectedNodes, nodeID)
			fd.metrics.FalsePositives.Add(1)
		}
	}
}

// backgroundRecoveryWorker attempts to recover failed nodes
func (fd *FailureDetector) backgroundRecoveryWorker() {
	ticker := time.NewTicker(fd.config.RecoveryInterval)
	defer ticker.Stop()
	
	recoveryRetries := make(map[string]int)
	
	for {
		select {
		case <-fd.done:
			return
		
		case nodeID := <-fd.recoveryQueue:
			fd.metrics.RecoveryAttempts.Add(1)
			
			retries := recoveryRetries[nodeID]
			if retries >= fd.config.MaxRecoveryRetries {
				// Max retries exceeded
				fd.metrics.RecoveryFailures.Add(1)
				delete(recoveryRetries, nodeID)
				continue
			}
			
			if fd.attemptNodeRecovery(nodeID) {
				fd.metrics.RecoverySuccess.Add(1)
				delete(recoveryRetries, nodeID)
			} else {
				recoveryRetries[nodeID]++
				// Re-queue for retry
				select {
				case fd.recoveryQueue <- nodeID:
				default:
				}
			}
		
		case <-ticker.C:
			// Periodically check for recovery update
		}
	}
}

// attemptNodeRecovery tries to recover a failed node
func (fd *FailureDetector) attemptNodeRecovery(nodeID string) bool {
	// Node lookup must be guarded by the partition map's own lock — fd.mu does
	// not protect pm.Nodes.
	fd.partitionMap.mu.RLock()
	node, exists := fd.partitionMap.Nodes[nodeID]
	fd.partitionMap.mu.RUnlock()

	if !exists {
		return false
	}

	// Try to ping the node
	if fd.pingNode(node.Address, node.Port) {
		// Node is back online, mark as recovering
		fd.partitionMap.UpdateNodeState(nodeID, NodeStateRecovering)

		// Trigger partition rebalance
		fd.triggerRebalance()

		return true
	}

	return false
}

// pingNode performs a real TCP health check against address:port.
//
// It dials with the configured PingTimeout, sends "PING\n" (the VeltrixDB
// text-protocol liveness probe — the server replies "PONG\n"), and requires
// at least one response byte before the deadline. A refused/timed-out dial or
// a peer that accepts but never answers both count as unhealthy: an accepting
// socket alone does not prove the application layer is responsive.
func (fd *FailureDetector) pingNode(address string, port int) bool {
	timeout := fd.config.PingTimeout
	if timeout <= 0 {
		timeout = defaultPingTimeout
	}

	addr := net.JoinHostPort(address, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}
	if _, err := conn.Write([]byte("PING\n")); err != nil {
		return false
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	return err == nil && n > 0
}

// triggerRebalance triggers partition rebalancing
func (fd *FailureDetector) triggerRebalance() {
	pm := fd.partitionMap

	// Get active node count (under the partition map's lock, which owns pm.Nodes).
	pm.mu.RLock()
	activeCount := 0
	for _, node := range pm.Nodes {
		if node.State.Load().(NodeState) == NodeStateActive {
			activeCount++
		}
	}
	pm.mu.RUnlock()

	if activeCount > 0 {
		// Rebalance across active nodes using the map's configured partition
		// count. (A previous version passed the PartitionMigrations metric — a
		// running counter of migrations — which rebalanced to a nonsense
		// partition count, e.g. 0 on a fresh cluster.)
		if err := pm.Rebalance(pm.PartitionCount()); err != nil {
			log.Printf("[fd] rebalance after recovery failed: %v", err)
		}
	}
}

// Close stops the failure detector. Idempotent.
func (fd *FailureDetector) Close() error {
	fd.closeOnce.Do(func() { close(fd.done) })
	return nil
}

// GetFailureStats returns failure detection statistics
func (fd *FailureDetector) GetFailureStats() FailureStats {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	
	return FailureStats{
		NodesDetected:    fd.metrics.NodesDetected.Load(),
		NodesRecovered:   fd.metrics.NodesRecovered.Load(),
		FalsePositives:   fd.metrics.FalsePositives.Load(),
		RecoveryAttempts: fd.metrics.RecoveryAttempts.Load(),
		RecoverySuccess:  fd.metrics.RecoverySuccess.Load(),
		RecoveryFailures: fd.metrics.RecoveryFailures.Load(),
		SuspectedCount:   uint64(len(fd.suspectedNodes)),
		FailedCount:      uint64(len(fd.failedNodes)),
	}
}

// FailureStats contains failure detection statistics
type FailureStats struct {
	NodesDetected    uint64
	NodesRecovered   uint64
	FalsePositives   uint64
	RecoveryAttempts uint64
	RecoverySuccess  uint64
	RecoveryFailures uint64
	SuspectedCount   uint64
	FailedCount      uint64
}

// GetMetrics returns the failure detection metrics for external observation.
func (fd *FailureDetector) GetMetrics() *FailureDetectionMetrics { return fd.metrics }
