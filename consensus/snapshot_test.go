package consensus

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ── Snapshot-capable mock state machine ───────────────────────────────────────

// mockSnapSM is mockSM plus Snapshot/Restore (SnapshotStateMachine).
type mockSnapSM struct {
	mockSM
}

func (m *mockSnapSM) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m.applied); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *mockSnapSM) Restore(data []byte) error {
	var applied [][]byte
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&applied); err != nil {
		return err
	}
	m.mu.Lock()
	m.applied = applied
	m.mu.Unlock()
	return nil
}

// contains reports whether cmd was applied.
func (m *mockSnapSM) contains(cmd string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.applied {
		if string(c) == cmd {
			return true
		}
	}
	return false
}

// ── Cluster helper with options + partitionable transports ───────────────────

type snapCluster struct {
	nodes []*RaftNode
	sms   []*mockSnapSM
	ids   []string
	hub   *mockTransport
}

// newSnapCluster builds n nodes with mockSnapSM state machines and per-node
// transport views (so hub.cutNode gives bidirectional partitions).
func newSnapCluster(t *testing.T, n int, opts Options) *snapCluster {
	t.Helper()
	c := &snapCluster{hub: newMockTransport()}
	prefix := t.Name() + "-"
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("%snode-%d", prefix, i))
	}
	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, c.ids[j])
			}
		}
		sm := &mockSnapSM{}
		node, err := NewRaftNodeWithOptions(c.ids[i], peers, t.TempDir(), sm, c.hub.forNode(c.ids[i]), opts)
		if err != nil {
			t.Fatalf("NewRaftNodeWithOptions(%s): %v", c.ids[i], err)
		}
		c.nodes = append(c.nodes, node)
		c.sms = append(c.sms, sm)
		c.hub.register(c.ids[i], node)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.Stop()
		}
	})
	return c
}

// leaderIdx returns the index of the current leader among nodes, or -1.
func leaderIdx(nodes []*RaftNode) int {
	for i, n := range nodes {
		if n.IsLeader() {
			return i
		}
	}
	return -1
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestSnapshot_TakenTruncatedAndRestart: a single-node cluster with a low
// SnapshotThreshold takes a snapshot, truncates the log prefix, and restarts
// from the snapshot + remaining log with the full state intact.
func TestSnapshot_TakenTruncatedAndRestart(t *testing.T) {
	id := t.Name() + "-node"
	dir := t.TempDir()
	hub := newMockTransport()
	sm := &mockSnapSM{}
	node, err := NewRaftNodeWithOptions(id, nil, dir, sm, hub.forNode(id), Options{SnapshotThreshold: 8})
	if err != nil {
		t.Fatalf("NewRaftNodeWithOptions: %v", err)
	}
	hub.register(id, node)

	if waitForLeader([]*RaftNode{node}, 3*time.Second) < 0 {
		node.Stop()
		t.Fatal("single node did not elect itself leader")
	}

	const numCmds = 20
	for i := 0; i < numCmds; i++ {
		if err := node.Submit([]byte(fmt.Sprintf("snap-cmd-%02d", i))); err != nil {
			node.Stop()
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// Wait for the automatic snapshot + truncation.
	var lastIncluded uint64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		lastIncluded = node.lastIncludedIndex
		node.mu.Unlock()
		if lastIncluded > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lastIncluded == 0 {
		node.Stop()
		t.Fatal("no snapshot taken despite log exceeding threshold")
	}
	if _, err := os.Stat(filepath.Join(dir, "raft_snapshot.gob")); err != nil {
		node.Stop()
		t.Fatalf("snapshot file missing: %v", err)
	}

	// Log prefix must have been truncated: first retained index follows the
	// snapshot, and the retained log is shorter than everything submitted.
	node.mu.Lock()
	firstIdx := node.logFirstIndex()
	logLen := len(node.ps.Log)
	node.mu.Unlock()
	if firstIdx != lastIncluded+1 {
		t.Errorf("log first index: want %d (snapshot+1), got %d", lastIncluded+1, firstIdx)
	}
	if logLen >= numCmds {
		t.Errorf("log not truncated: %d entries retained", logLen)
	}

	node.Stop()

	// Restart from the same dataDir with a FRESH state machine: the snapshot
	// must be restored and the log tail replayed.
	sm2 := &mockSnapSM{}
	hub2 := newMockTransport()
	node2, err := NewRaftNodeWithOptions(id, nil, dir, sm2, hub2.forNode(id), Options{SnapshotThreshold: 8})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	hub2.register(id, node2)
	defer node2.Stop()

	node2.mu.Lock()
	gotIncluded := node2.lastIncludedIndex
	node2.mu.Unlock()
	if gotIncluded < lastIncluded {
		t.Errorf("lastIncludedIndex after restart: want >= %d, got %d", lastIncluded, gotIncluded)
	}

	// The snapshot restores the applied prefix immediately; the tail replays
	// once the node re-elects itself and commits.
	if waitForLeader([]*RaftNode{node2}, 3*time.Second) < 0 {
		t.Fatal("restarted node did not become leader")
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && sm2.count() < numCmds {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sm2.count(); got != numCmds {
		t.Fatalf("state machine after restart: want %d commands, got %d", numCmds, got)
	}
	for i := 0; i < numCmds; i++ {
		want := fmt.Sprintf("snap-cmd-%02d", i)
		if got := string(sm2.get(i)); got != want {
			t.Errorf("applied[%d]: want %q, got %q", i, want, got)
		}
	}
}

// TestSnapshot_InstallSnapshotCatchUp: in a 3-node cluster, a follower is
// partitioned while the leader commits enough entries to compact its log past
// the follower's tail.  After the partition heals, the follower can only
// catch up via InstallSnapshot — verify it receives the snapshot and the full
// state.
func TestSnapshot_InstallSnapshotCatchUp(t *testing.T) {
	c := newSnapCluster(t, 3, Options{SnapshotThreshold: 6})

	if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no stable leader")
	}

	const preCut = 3
	const total = 15
	for i := 0; i < preCut; i++ {
		if err := submitWithRetry(c.nodes, []byte(fmt.Sprintf("catchup-%02d", i)), 8*time.Second); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// Partition a follower (bidirectional).
	li := leaderIdx(c.nodes)
	if li < 0 {
		t.Fatal("no leader after initial submits")
	}
	fi := (li + 1) % 3
	follower := c.nodes[fi]
	c.hub.cutNode(c.ids[fi])
	follower.mu.Lock()
	followerTail := follower.lastLogIndex()
	follower.mu.Unlock()

	survivors := []*RaftNode{}
	for i, n := range c.nodes {
		if i != fi {
			survivors = append(survivors, n)
		}
	}

	for i := preCut; i < total; i++ {
		if err := submitWithRetry(survivors, []byte(fmt.Sprintf("catchup-%02d", i)), 8*time.Second); err != nil {
			t.Fatalf("submit %d while partitioned: %v", i, err)
		}
	}

	// Wait until the current leader has compacted past the follower's tail,
	// so plain AppendEntries can no longer catch it up.
	deadline := time.Now().Add(10 * time.Second)
	compacted := false
	for time.Now().Before(deadline) && !compacted {
		if si := leaderIdx(survivors); si >= 0 {
			survivors[si].mu.Lock()
			compacted = survivors[si].lastIncludedIndex > followerTail
			survivors[si].mu.Unlock()
		}
		if !compacted {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !compacted {
		t.Fatal("leader never compacted past the partitioned follower's tail")
	}

	// Heal and wait for the follower to catch up.  Its own log never reached
	// the snapshot threshold, and the entries it needs were compacted away —
	// only InstallSnapshot can advance it.
	c.hub.healNode(c.ids[fi])

	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && c.sms[fi].count() < total {
		time.Sleep(20 * time.Millisecond)
	}
	if got := c.sms[fi].count(); got != total {
		t.Fatalf("follower state machine: want %d commands, got %d", total, got)
	}
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("catchup-%02d", i)
		if !c.sms[fi].contains(want) {
			t.Errorf("follower missing command %q", want)
		}
	}

	follower.mu.Lock()
	fIncluded := follower.lastIncludedIndex
	follower.mu.Unlock()
	if fIncluded <= followerTail {
		t.Errorf("follower lastIncludedIndex=%d not past its pre-partition tail %d — "+
			"catch-up did not go through InstallSnapshot", fIncluded, followerTail)
	}
}
