package consensus

import (
	"testing"
	"time"
)

// TestReadIndex_LeaderServes verifies a leader passes the fence and the
// returned index covers all committed entries.
func TestReadIndex_LeaderServes(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	li := waitForLeader(nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no leader elected")
	}
	leader := nodes[li]

	if err := leader.Submit([]byte("w1")); err != nil {
		t.Fatalf("submit: %v", err)
	}

	idx, err := leader.ReadIndex(2 * time.Second)
	if err != nil {
		t.Fatalf("ReadIndex on leader: %v", err)
	}
	leader.mu.Lock()
	commit, applied := leader.commitIndex, leader.lastApplied
	leader.mu.Unlock()
	if idx < 1 || idx > commit {
		t.Fatalf("readIdx=%d out of range (commit=%d)", idx, commit)
	}
	if applied < idx {
		t.Fatalf("ReadIndex returned before apply: applied=%d < idx=%d", applied, idx)
	}
}

// TestReadIndex_FollowerRejected verifies followers refuse the fence with
// ErrNotLeader so the caller redirects instead of serving a stale read.
func TestReadIndex_FollowerRejected(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	li := waitForLeader(nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no leader elected")
	}
	leader := nodes[li]

	for _, n := range nodes {
		if n == leader {
			continue
		}
		if _, err := n.ReadIndex(500 * time.Millisecond); err != ErrNotLeader {
			t.Fatalf("follower ReadIndex err = %v, want ErrNotLeader", err)
		}
	}
}
