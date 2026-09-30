package consensus

import (
	"fmt"
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

// TestWaitLeaderApplied_FailoverVisibility: after the leader that
// acknowledged a write dies, the new leader must not serve a local read until
// that write is applied. WaitLeaderApplied is the barrier; without it a GET
// right after election could miss an acknowledged write
// (TestRaftClusterFailover saw "data lost across failover" this way).
func TestWaitLeaderApplied_FailoverVisibility(t *testing.T) {
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			c := newSnapCluster(t, 3, Options{})
			li := waitForStableLeader(c.nodes, 5*time.Second)
			if li < 0 {
				t.Fatal("no leader elected")
			}
			payload := fmt.Sprintf("acked-%d", round)
			if err := c.nodes[li].Submit([]byte(payload)); err != nil {
				t.Fatalf("submit: %v", err)
			}
			c.hub.cutNode(c.ids[li]) // the old leader is gone right after the ACK

			newIdx := -1
			deadline := time.Now().Add(10 * time.Second)
			for newIdx < 0 && time.Now().Before(deadline) {
				for i, n := range c.nodes {
					if i != li && n.IsLeader() {
						newIdx = i
					}
				}
				time.Sleep(time.Millisecond)
			}
			if newIdx < 0 {
				t.Fatal("no new leader after partitioning the old one")
			}
			if err := c.nodes[newIdx].WaitLeaderApplied(2 * time.Second); err != nil {
				t.Fatalf("WaitLeaderApplied: %v", err)
			}
			if !c.sms[newIdx].contains(payload) {
				t.Fatal("acknowledged write not applied on the new leader after WaitLeaderApplied")
			}
			// Followers never block.
			for i, n := range c.nodes {
				if i != li && i != newIdx {
					if err := n.WaitLeaderApplied(time.Millisecond); err != nil {
						t.Fatalf("follower blocked: %v", err)
					}
				}
			}
		})
	}
}
