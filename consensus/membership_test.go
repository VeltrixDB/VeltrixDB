package consensus

import (
	"fmt"
	"testing"
	"time"
)

// changeMembershipWithRetry retries a config change until it succeeds or the
// timeout expires (leadership can move mid-call, like Submit).
func changeMembershipWithRetry(nodes []*RaftNode, change func(*RaftNode) error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		idx := waitForStableLeader(nodes, 500*time.Millisecond)
		if idx < 0 {
			continue
		}
		lastErr = change(nodes[idx])
		if lastErr == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("membership change not applied within %v (last error: %v)", timeout, lastErr)
}

// configServers returns the current configuration of node under lock.
func configServers(n *RaftNode) []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.config.Servers...)
}

// TestMembership_AddServer: a 4th node added through the log becomes a full
// quorum participant — with one original follower partitioned, commits
// require the new node's acknowledgement (3 of 4).
func TestMembership_AddServer(t *testing.T) {
	c := newSnapCluster(t, 3, Options{})
	if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no stable leader")
	}
	if err := submitWithRetry(c.nodes, []byte("pre-add"), 8*time.Second); err != nil {
		t.Fatalf("pre-add submit: %v", err)
	}

	// Start the 4th node (bootstrap peers = the existing cluster).
	newID := t.Name() + "-node-3"
	sm4 := &mockSnapSM{}
	node4, err := NewRaftNodeWithOptions(newID, c.ids, t.TempDir(), sm4, c.hub.forNode(newID), Options{})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	c.hub.register(newID, node4)
	t.Cleanup(node4.Stop)

	if err := changeMembershipWithRetry(c.nodes, func(l *RaftNode) error {
		return l.AddServer(newID)
	}, 20*time.Second); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	all := append(append([]*RaftNode{}, c.nodes...), node4)
	li := waitForStableLeader(all, 10*time.Second)
	if li < 0 {
		t.Fatal("no stable leader after AddServer")
	}
	if got := configServers(all[li]); len(got) != 4 {
		t.Fatalf("leader config: want 4 servers, got %v", got)
	}

	// Quorum participation: partition one ORIGINAL follower.  The remaining
	// leader + 1 original + new node = exactly 3 of 4 — every commit now
	// requires the new node's ack.
	cutIdx := -1
	for i := range c.nodes {
		if c.nodes[i] != all[li] {
			cutIdx = i
			break
		}
	}
	if cutIdx < 0 {
		t.Fatal("no original follower to cut (new node became leader and so did everyone?)")
	}
	c.hub.cutNode(c.ids[cutIdx])

	alive := []*RaftNode{node4}
	for i, n := range c.nodes {
		if i != cutIdx {
			alive = append(alive, n)
		}
	}
	if err := submitWithRetry(alive, []byte("quorum-with-new-node"), 15*time.Second); err != nil {
		t.Fatalf("submit requiring new node's ack: %v", err)
	}

	// The new node must have applied the committed entry.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !sm4.contains("quorum-with-new-node") {
		time.Sleep(20 * time.Millisecond)
	}
	if !sm4.contains("quorum-with-new-node") {
		t.Fatal("new node never applied the entry it must have acknowledged")
	}
	c.hub.healNode(c.ids[cutIdx])
}

// TestMembership_RemoveServer: a follower removed through the log stops
// receiving entries, and the shrunken cluster (quorum 2 of 2) keeps
// committing.
func TestMembership_RemoveServer(t *testing.T) {
	c := newSnapCluster(t, 3, Options{})
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no stable leader")
	}
	ri := (li + 1) % 3 // a follower
	removedID := c.ids[ri]

	if err := changeMembershipWithRetry(c.nodes, func(l *RaftNode) error {
		return l.RemoveServer(removedID)
	}, 20*time.Second); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	remaining := []*RaftNode{}
	for i, n := range c.nodes {
		if i != ri {
			remaining = append(remaining, n)
		}
	}
	nli := waitForStableLeader(remaining, 10*time.Second)
	if nli < 0 {
		t.Fatal("no stable leader after RemoveServer")
	}
	cfg := configServers(remaining[nli])
	if len(cfg) != 2 {
		t.Fatalf("config after removal: want 2 servers, got %v", cfg)
	}
	for _, s := range cfg {
		if s == removedID {
			t.Fatalf("removed server %s still in config %v", removedID, cfg)
		}
	}

	// The 2-server cluster must still commit (quorum 2 of 2).
	if err := submitWithRetry(remaining, []byte("post-remove"), 10*time.Second); err != nil {
		t.Fatalf("submit after removal: %v", err)
	}

	// The removed node must NOT receive the new entry.
	time.Sleep(300 * time.Millisecond)
	if c.sms[ri].contains("post-remove") {
		t.Error("removed server received an entry committed after its removal")
	}
}

// TestMembership_LeaderSelfRemoval: a leader that removes itself keeps
// leading until the removal commits, then steps down; the remaining servers
// elect a new leader.
func TestMembership_LeaderSelfRemoval(t *testing.T) {
	c := newSnapCluster(t, 3, Options{})
	if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no stable leader")
	}

	// Retry loop that always removes the CURRENT leader from itself, but
	// stops if a previous (timed-out) attempt actually committed — otherwise
	// a retry would remove a second leader.
	var oldLeader *RaftNode
	deadline := time.Now().Add(20 * time.Second)
	removed := false
	for time.Now().Before(deadline) && !removed {
		idx := waitForStableLeader(c.nodes, 1*time.Second)
		if idx < 0 {
			continue
		}
		l := c.nodes[idx]
		if oldLeader != nil && len(configServers(l)) == 2 {
			removed = true // an earlier attempt committed after timing out
			break
		}
		oldLeader = l
		if err := l.RemoveServer(l.id); err == nil {
			removed = true
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !removed {
		t.Fatal("leader self-removal never committed")
	}

	// The old leader must step down once its removal committed.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && oldLeader.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if oldLeader.IsLeader() {
		t.Fatal("leader still leading after committing its own removal")
	}

	// The two remaining servers elect a new leader.
	remaining := []*RaftNode{}
	for _, n := range c.nodes {
		if n != oldLeader {
			remaining = append(remaining, n)
		}
	}
	nli := waitForStableLeader(remaining, 10*time.Second)
	if nli < 0 {
		t.Fatal("survivors did not elect a new leader after self-removal")
	}
	cfg := configServers(remaining[nli])
	if len(cfg) != 2 {
		t.Fatalf("config after self-removal: want 2 servers, got %v", cfg)
	}
	for _, s := range cfg {
		if s == oldLeader.id {
			t.Fatalf("removed leader %s still in config %v", oldLeader.id, cfg)
		}
	}
}

// TestMembership_RejectConcurrentChange: a second configuration change is
// rejected while the first is still uncommitted.
func TestMembership_RejectConcurrentChange(t *testing.T) {
	c := newSnapCluster(t, 3, Options{})
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no stable leader")
	}
	leader := c.nodes[li]

	// Cut both followers so nothing can commit, then start a change.
	for i := range c.nodes {
		if i != li {
			c.hub.cutNode(c.ids[i])
		}
	}
	// Leadership can only be LOST via a higher term, which cannot reach the
	// leader while both followers are cut — but re-check to avoid a race
	// between observing stability and cutting.
	if !leader.IsLeader() {
		t.Skip("leadership moved between observation and partition — rare timing, skipping")
	}
	leader.submitTimeout = 500 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- leader.AddServer(t.Name() + "-extra") }()

	// Give the first change time to be appended, then try a second one.
	time.Sleep(100 * time.Millisecond)
	if err := leader.RemoveServer(c.ids[(li+1)%3]); err != ErrConfigChangeInProgress {
		t.Errorf("concurrent change: want ErrConfigChangeInProgress, got %v", err)
	}

	// The first change cannot commit (no quorum) and must time out.
	select {
	case err := <-done:
		if err == nil {
			t.Error("AddServer committed without a quorum")
		}
	case <-time.After(5 * time.Second):
		t.Error("AddServer did not return after submit timeout")
	}
	for i := range c.nodes {
		if i != li {
			c.hub.healNode(c.ids[i])
		}
	}
}
