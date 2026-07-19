package consensus

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// all returns an ordered copy of every command applied to this mock state
// machine.  Used by the group-commit tests to compare per-node apply order.
func (m *mockSM) all() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.applied))
	for i, c := range m.applied {
		out[i] = string(c)
	}
	return out
}

// smOf returns the *mockSM backing node n (same-package access).
func smOf(n *RaftNode) *mockSM { return n.sm.(*mockSM) }

// submitToCluster submits cmd, retrying on the current leader until success or
// the deadline.  Under a stable leader the first attempt succeeds.
func submitToCluster(nodes []*RaftNode, cmd []byte, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx := waitForLeader(nodes, 500*time.Millisecond)
		if idx < 0 {
			continue
		}
		if err := nodes[idx].Submit(cmd); err == nil {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("submitToCluster: no success within %v", timeout)
}

// TestGroupCommit_BatchingReducesFsyncs fires many concurrent Submits at a
// 3-node cluster and asserts:
//   - every command commits and applies exactly once,
//   - all three nodes apply them in an identical order,
//   - the leader performed FAR fewer writeStateFile fsyncs than there were
//     Submits (i.e. group commit coalesced them).
func TestGroupCommit_BatchingReducesFsyncs(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	lead := waitForStableLeader(nodes, 5*time.Second)
	if lead < 0 {
		t.Fatal("no stable leader")
	}
	leader := nodes[lead]

	const (
		numCmds    = 200
		numWorkers = 32
	)

	fsyncBefore := leader.writeStateFileCalls.Load()

	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make([]error, numWorkers)
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				n := next.Add(1) - 1
				if n >= numCmds {
					return
				}
				cmd := []byte(fmt.Sprintf("gc-cmd-%04d", n))
				if err := submitToCluster(nodes, cmd, 10*time.Second); err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for w, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", w, err)
		}
	}

	fsyncAfter := leader.writeStateFileCalls.Load()
	leaderFsyncs := fsyncAfter - fsyncBefore
	t.Logf("group commit: %d Submits → %d leader writeStateFile fsyncs (%.1f Submits/fsync)",
		numCmds, leaderFsyncs, float64(numCmds)/float64(max64(leaderFsyncs, 1)))

	// Batching MUST have happened: with 32 concurrent submitters coalesced per
	// flush we expect roughly numCmds/batch fsyncs — well under half of numCmds.
	if leaderFsyncs == 0 {
		t.Fatalf("leader recorded 0 fsyncs for %d Submits — impossible", numCmds)
	}
	if leaderFsyncs > numCmds/2 {
		t.Errorf("group commit did not coalesce: %d fsyncs for %d Submits (want <= %d)",
			leaderFsyncs, numCmds, numCmds/2)
	}

	// Wait until all three nodes have applied all commands.
	want := make(map[string]bool, numCmds)
	for i := 0; i < numCmds; i++ {
		want[fmt.Sprintf("gc-cmd-%04d", i)] = true
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		done := true
		for _, n := range nodes {
			if smOf(n).count() < numCmds {
				done = false
				break
			}
		}
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Exactly-once + identical order across nodes.
	ref := smOf(nodes[0]).all()
	if len(ref) != numCmds {
		t.Fatalf("node0 applied %d commands, want %d", len(ref), numCmds)
	}
	seen := make(map[string]bool, numCmds)
	for _, c := range ref {
		if !want[c] {
			t.Errorf("node0 applied unexpected command %q", c)
		}
		if seen[c] {
			t.Errorf("command %q applied more than once", c)
		}
		seen[c] = true
	}
	if len(seen) != numCmds {
		t.Errorf("node0 applied %d distinct commands, want %d (lost writes)", len(seen), numCmds)
	}
	for i := 1; i < len(nodes); i++ {
		got := smOf(nodes[i]).all()
		if len(got) != len(ref) {
			t.Fatalf("node%d applied %d commands, want %d", i, len(got), len(ref))
		}
		for j := range ref {
			if got[j] != ref[j] {
				t.Fatalf("apply-order divergence at index %d: node0=%q node%d=%q",
					j, ref[j], i, got[j])
			}
		}
	}
}

// TestGroupCommit_LeaderStableUnderLoad drives sustained concurrent writes for a
// couple of seconds and asserts the leader and its term do NOT change — directly
// guarding the fsync-storm regression that used to cause re-elections.
func TestGroupCommit_LeaderStableUnderLoad(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	lead := waitForStableLeader(nodes, 5*time.Second)
	if lead < 0 {
		t.Fatal("no stable leader")
	}
	leader := nodes[lead]
	startTerm := leader.Term()

	const (
		numWorkers = 32
		duration   = 2 * time.Second
	)

	stop := make(chan struct{})
	var ok, failed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				cmd := []byte(fmt.Sprintf("stab-%02d-%06d", w, i))
				i++
				// Submit directly to the pinned leader: a leadership change is
				// exactly what this test is designed to catch.
				if err := leader.Submit(cmd); err != nil {
					failed.Add(1)
				} else {
					ok.Add(1)
				}
			}
		}(w)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// The originally-elected leader must still be leading in the same term.
	if !leader.IsLeader() {
		t.Fatalf("leader %s lost leadership under write load (stability regression)", leader.id)
	}
	if got := leader.Term(); got != startTerm {
		t.Fatalf("leader term changed from %d to %d under write load — re-election occurred",
			startTerm, got)
	}
	// No other node should have bumped the term either.
	for _, n := range nodes {
		if n.Term() != startTerm {
			t.Fatalf("node %s term %d != stable term %d — cluster destabilised",
				n.id, n.Term(), startTerm)
		}
	}
	t.Logf("stability: %d writes committed, %d failed, term steady at %d over %v",
		ok.Load(), failed.Load(), startTerm, duration)
	if ok.Load() == 0 {
		t.Fatal("no writes committed under load — Submit path broken")
	}
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
