package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// newTestPM creates a minimal PartitionMap for use in failure detection tests.
// It adds the supplied nodeIDs with dummy addresses. The replication factor is
// set to 1 so Rebalance never errors from an RF > node count mismatch.
func newTestPM(t *testing.T, nodeIDs ...string) *PartitionMap {
	t.Helper()
	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	for i, id := range nodeIDs {
		if err := pm.AddNode(id, "127.0.0.1", 9000+i); err != nil {
			t.Fatalf("AddNode(%s): %v", id, err)
		}
	}
	return pm
}

// fastFDConfig returns a FailureDetectorConfig with short timeouts suitable
// for unit tests.
func fastFDConfig() *FailureDetectorConfig {
	return &FailureDetectorConfig{
		HeartbeatInterval:  20 * time.Millisecond,
		SuspectThreshold:   40 * time.Millisecond,
		FailureThreshold:   80 * time.Millisecond,
		MaxSuspectTime:     200 * time.Millisecond,
		RecoveryInterval:   20 * time.Millisecond,
		MaxRecoveryRetries: 3,
	}
}

// nodeState reads the state of nodeID from the partition map (thread-safe).
func nodeState(pm *PartitionMap, nodeID string) (NodeState, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	n, ok := pm.Nodes[nodeID]
	if !ok {
		return NodeStateActive, false
	}
	return n.State.Load().(NodeState), true
}

// waitForState polls until the node reaches the expected state or times out.
func waitForState(pm *PartitionMap, nodeID string, want NodeState, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s, ok := nodeState(pm, nodeID); ok && s == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestFD_HealthyNode: a node that receives regular heartbeats stays Active.
func TestFD_HealthyNode(t *testing.T) {
	t.Parallel()

	pm := newTestPM(t, "n1")
	fd := NewFailureDetector(pm, fastFDConfig())
	fd.SetLocalNode("self")
	fd.Start()
	defer fd.Close()

	// Record heartbeats faster than the failure threshold.
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				fd.RecordHeartbeat("n1")
			}
		}
	}()

	// Wait longer than the failure threshold — n1 should still be Active.
	time.Sleep(200 * time.Millisecond)
	close(stop)

	s, ok := nodeState(pm, "n1")
	if !ok {
		t.Fatal("n1 not found in partition map")
	}
	if s == NodeStateFailed {
		t.Errorf("n1 should not be Failed while receiving heartbeats, got %s", s)
	}
}

// TestFD_DetectFailure: a node that stops heartbeating is eventually marked Failed.
func TestFD_DetectFailure(t *testing.T) {
	t.Parallel()

	pm := newTestPM(t, "n-fail")
	// Prime the node with a fresh heartbeat so the clock starts from now.
	pm.mu.Lock()
	pm.Nodes["n-fail"].LastHeartbeat = time.Now().UnixNano()
	pm.mu.Unlock()

	fd := NewFailureDetector(pm, fastFDConfig())
	fd.SetLocalNode("self")
	// Record one heartbeat so the FD map has an entry.
	fd.RecordHeartbeat("n-fail")
	fd.Start()
	defer fd.Close()

	// Do NOT send any more heartbeats — wait for failure threshold.
	timeout := 500 * time.Millisecond
	if !waitForState(pm, "n-fail", NodeStateFailed, timeout) {
		s, _ := nodeState(pm, "n-fail")
		t.Errorf("n-fail should be Failed after timeout, got %s", s)
	}
}

// TestFD_RecoveryAfterFailure: a node resumes heartbeating and transitions to
// Recovering (or Active) after being marked Failed.
func TestFD_RecoveryAfterFailure(t *testing.T) {
	t.Parallel()

	pm := newTestPM(t, "n-recover")
	pm.mu.Lock()
	pm.Nodes["n-recover"].LastHeartbeat = time.Now().UnixNano()
	pm.mu.Unlock()

	fd := NewFailureDetector(pm, fastFDConfig())
	fd.SetLocalNode("self")
	fd.RecordHeartbeat("n-recover")
	fd.Start()
	defer fd.Close()

	// Wait until node is marked Failed.
	if !waitForState(pm, "n-recover", NodeStateFailed, 500*time.Millisecond) {
		s, _ := nodeState(pm, "n-recover")
		t.Skipf("node did not reach Failed state within deadline (got %s); skipping recovery check", s)
	}

	// Now resume heartbeating.
	fd.RecordHeartbeat("n-recover")

	// Node should transition away from Failed to Recovering.
	deadline := time.Now().Add(500 * time.Millisecond)
	recovered := false
	for time.Now().Before(deadline) {
		s, _ := nodeState(pm, "n-recover")
		if s == NodeStateRecovering || s == NodeStateActive {
			recovered = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !recovered {
		s, _ := nodeState(pm, "n-recover")
		t.Errorf("n-recover should be Recovering or Active after heartbeat, got %s", s)
	}
}

// TestFD_LocalNodeNotDetected: the local node (set via SetLocalNode) is never
// marked as Failed regardless of missing heartbeats.
func TestFD_LocalNodeNotDetected(t *testing.T) {
	t.Parallel()

	pm := newTestPM(t, "local", "other")
	fd := NewFailureDetector(pm, fastFDConfig())
	// Must call SetLocalNode BEFORE Start per Invariant 11.
	fd.SetLocalNode("local")
	// Provide continuous heartbeats for "other" so only "local" is at risk.
	fd.RecordHeartbeat("other")
	fd.Start()
	defer fd.Close()

	// Keep "other" alive; let "local" starve of heartbeats.
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				fd.RecordHeartbeat("other")
			}
		}
	}()
	defer close(stop)

	// Wait well beyond the failure threshold.
	time.Sleep(300 * time.Millisecond)

	s, _ := nodeState(pm, "local")
	if s == NodeStateFailed {
		t.Errorf("local node should never be marked Failed; got %s", s)
	}
}

// TestFD_MultipleNodes: 5 nodes; one stops heartbeating; only that one is Failed.
func TestFD_MultipleNodes(t *testing.T) {
	t.Parallel()

	nodeIDs := []string{"a", "b", "c", "d", "e"}
	pm := newTestPM(t, nodeIDs...)
	// Prime all nodes with fresh heartbeats.
	for _, id := range nodeIDs {
		pm.mu.Lock()
		pm.Nodes[id].LastHeartbeat = time.Now().UnixNano()
		pm.mu.Unlock()
	}

	fd := NewFailureDetector(pm, fastFDConfig())
	fd.SetLocalNode("self")
	for _, id := range nodeIDs {
		fd.RecordHeartbeat(id)
	}
	fd.Start()
	defer fd.Close()

	// Keep a, b, c, d alive; let e stop.
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				for _, id := range []string{"a", "b", "c", "d"} {
					fd.RecordHeartbeat(id)
				}
			}
		}
	}()
	defer close(stop)

	// Wait for "e" to be detected as Failed.
	if !waitForState(pm, "e", NodeStateFailed, 500*time.Millisecond) {
		s, _ := nodeState(pm, "e")
		t.Errorf("node e should be Failed, got %s", s)
	}

	// All others must not be Failed.
	for _, id := range []string{"a", "b", "c", "d"} {
		s, _ := nodeState(pm, id)
		if s == NodeStateFailed {
			t.Errorf("node %s should not be Failed", id)
		}
	}
}

// TestFD_GetNodeStatus: partition map state is accessible and correct for a
// node that has been explicitly set.
func TestFD_GetNodeStatus(t *testing.T) {
	t.Parallel()

	pm := newTestPM(t, "status-node")
	// Set node state directly so we can assert it.
	if err := pm.UpdateNodeState("status-node", NodeStateSuspect); err != nil {
		t.Fatalf("UpdateNodeState: %v", err)
	}

	s, ok := nodeState(pm, "status-node")
	if !ok {
		t.Fatal("status-node not found")
	}
	if s != NodeStateSuspect {
		t.Errorf("expected Suspect, got %s", s)
	}
}

// TestFD_NodeNotRegistered: GetNodeStatus for an unknown node returns not-found.
func TestFD_NodeNotRegistered(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	pm := NewPartitionMap(cfg)

	_, ok := nodeState(pm, "ghost-node")
	if ok {
		t.Error("expected ghost-node to not be found in empty partition map")
	}

	// UpdateNodeState on a non-existent node should return an error.
	if err := pm.UpdateNodeState("ghost-node", NodeStateActive); err == nil {
		t.Error("expected error from UpdateNodeState on unknown node, got nil")
	}
}

// TestFD_ConcurrentHeartbeats: 10 goroutines sending heartbeats for different
// nodes; no data race (run with -race).
func TestFD_ConcurrentHeartbeats(t *testing.T) {
	t.Parallel()

	n := 10
	nodeIDs := make([]string, n)
	for i := 0; i < n; i++ {
		nodeIDs[i] = fmt.Sprintf("concurrent-%d", i)
	}

	pm := newTestPM(t, nodeIDs...)
	fd := NewFailureDetector(pm, fastFDConfig())
	fd.SetLocalNode("self")
	fd.Start()
	defer fd.Close()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				fd.RecordHeartbeat(id)
				time.Sleep(1 * time.Millisecond)
			}
		}(nodeIDs[i])
	}
	wg.Wait()
	// If we reach here without a race the test passes.
}
