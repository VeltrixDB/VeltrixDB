package consensus

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestSubmit_CommitNotifyPrompt: Submit returns promptly once the entry is
// applied — the applier signals the per-index waiter channel directly instead
// of Submit polling.
func TestSubmit_CommitNotifyPrompt(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	idx := waitForStableLeader(nodes, 5*time.Second)
	if idx < 0 {
		t.Fatal("no stable leader")
	}

	const numOps = 10
	var total time.Duration
	completed := 0
	for i := 0; i < numOps; i++ {
		leader := nodes[idx]
		start := time.Now()
		err := leader.Submit([]byte(fmt.Sprintf("prompt-%02d", i)))
		if err != nil {
			// Leadership moved — re-resolve and retry this op.
			idx = waitForStableLeader(nodes, 3*time.Second)
			if idx < 0 {
				t.Fatal("lost leader mid-test")
			}
			i--
			continue
		}
		total += time.Since(start)
		completed++
	}
	if completed == 0 {
		t.Fatal("no submits completed")
	}
	avg := total / time.Duration(completed)
	// With channel-based notification a commit round-trip over the in-process
	// mock transport is a few ms (dominated by the state-file fsync).  200ms
	// would indicate waiting on timers/polling rather than the notify path.
	if avg > 200*time.Millisecond {
		t.Errorf("Submit avg latency %v — commit notification not prompt", avg)
	}
	t.Logf("avg Submit latency over %d ops: %v", completed, avg)
}

// TestSubmit_Timeout: when the quorum is unreachable, Submit fails after the
// configured timeout (not sooner, not never).
func TestSubmit_Timeout(t *testing.T) {
	c := newSnapCluster(t, 3, Options{})
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no stable leader")
	}
	leader := c.nodes[li]

	// Cut both followers: the leader stays leader (no higher term can reach
	// it) but can never reach quorum.
	for i := range c.nodes {
		if i != li {
			c.hub.cutNode(c.ids[i])
		}
	}
	if !leader.IsLeader() {
		t.Skip("leadership moved between observation and partition — rare timing, skipping")
	}

	const timeout = 400 * time.Millisecond
	leader.submitTimeout = timeout

	start := time.Now()
	err := leader.Submit([]byte("never-commits"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Submit succeeded without a quorum")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("want timeout error, got: %v", err)
	}
	if elapsed < timeout-50*time.Millisecond {
		t.Errorf("Submit returned after %v, before the %v timeout", elapsed, timeout)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Submit took %v — timeout semantics broken", elapsed)
	}

	for i := range c.nodes {
		if i != li {
			c.hub.healNode(c.ids[i])
		}
	}
}
