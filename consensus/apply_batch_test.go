package consensus

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// batchMockSM is a StateMachine that ALSO implements BatchStateMachine.  It
// records ordered applies (via the embedded mockSM) plus batch-apply statistics
// so tests can assert the applier coalesced consecutive committed writes into a
// single ApplyBatch call.
type batchMockSM struct {
	mockSM
	batchCalls atomic.Int64 // ApplyBatch invocations of any size
	maxBatch   atomic.Int64 // largest batch observed
}

func (m *batchMockSM) ApplyBatch(cmds [][]byte) []error {
	m.batchCalls.Add(1)
	for {
		cur := m.maxBatch.Load()
		if int64(len(cmds)) <= cur {
			break
		}
		if m.maxBatch.CompareAndSwap(cur, int64(len(cmds))) {
			break
		}
	}
	res := make([]error, len(cmds))
	for i, c := range cmds {
		// Reuse mockSM.Apply so the ordered-append + exactly-once bookkeeping is
		// identical to the single-apply path.
		res[i] = m.Apply(c)
	}
	return res
}

func bsmOf(n *RaftNode) *batchMockSM { return n.sm.(*batchMockSM) }

// newBatchCluster builds an n-node cluster whose state machines implement
// BatchStateMachine, over a single shared mockTransport (mirrors newTestCluster).
// It also returns the transport and IDs so a test can grow the cluster.
func newBatchCluster(t *testing.T, n int) ([]*RaftNode, *mockTransport, []string) {
	t.Helper()
	transport := newMockTransport()
	prefix := t.Name() + "-"
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("%snode-%d", prefix, i)
	}
	nodes := make([]*RaftNode, n)
	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, ids[j])
			}
		}
		node, err := NewRaftNode(ids[i], peers, t.TempDir(), &batchMockSM{}, transport)
		if err != nil {
			t.Fatalf("NewRaftNode(%s): %v", ids[i], err)
		}
		nodes[i] = node
		transport.register(ids[i], node)
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.Stop()
		}
	})
	return nodes, transport, ids
}

// assertIdenticalOrder waits until every node applied exactly the wanted set and
// asserts all nodes agree on the apply order (exactly-once + no divergence).
func assertIdenticalOrder(t *testing.T, nodes []*RaftNode, want map[string]bool, get func(*RaftNode) []string) {
	t.Helper()
	numCmds := len(want)
	deadline := time.Now().Add(5 * time.Second)
	for {
		done := true
		for _, n := range nodes {
			if len(get(n)) < numCmds {
				done = false
				break
			}
		}
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	ref := get(nodes[0])
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
		t.Errorf("node0 applied %d distinct commands, want %d (lost/dup writes)", len(seen), numCmds)
	}
	for i := 1; i < len(nodes); i++ {
		got := get(nodes[i])
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

// TestApplyBatch_CoalescesUnderLoad fires many concurrent Submits at a 3-node
// cluster whose FSM implements BatchStateMachine and asserts:
//   - the applier actually coalesced consecutive writes (batchApplyCalls > 0
//     and maxApplyBatch > 1 on both the raft node and the state machine),
//   - every command applied exactly once, in an identical order on all nodes.
func TestApplyBatch_CoalescesUnderLoad(t *testing.T) {
	nodes, _, _ := newBatchCluster(t, 3)
	lead := waitForStableLeader(nodes, 5*time.Second)
	if lead < 0 {
		t.Fatal("no stable leader")
	}
	leader := nodes[lead]

	const (
		numCmds    = 400
		numWorkers = 48
	)

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
				cmd := []byte(fmt.Sprintf("ab-cmd-%04d", n))
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

	// Batch coalescing MUST have engaged on the applier and the FSM.
	if leader.batchApplyCalls.Load() == 0 {
		t.Errorf("leader made 0 multi-entry ApplyBatch calls — batching not engaged")
	}
	if leader.maxApplyBatch.Load() <= 1 {
		t.Errorf("leader max ApplyBatch run = %d, want > 1", leader.maxApplyBatch.Load())
	}
	sm := bsmOf(leader)
	if sm.maxBatch.Load() <= 1 {
		t.Errorf("state machine max batch = %d, want > 1", sm.maxBatch.Load())
	}
	t.Logf("batch apply: raft multi-entry batchCalls=%d maxRun=%d ; sm batchCalls=%d maxBatch=%d",
		leader.batchApplyCalls.Load(), leader.maxApplyBatch.Load(),
		sm.batchCalls.Load(), sm.maxBatch.Load())

	want := make(map[string]bool, numCmds)
	for i := 0; i < numCmds; i++ {
		want[fmt.Sprintf("ab-cmd-%04d", i)] = true
	}
	assertIdenticalOrder(t, nodes, want, func(n *RaftNode) []string { return bsmOf(n).all() })
}

// TestApplyBatch_FallbackSingleApply verifies a state machine that does NOT
// implement BatchStateMachine (plain mockSM) still applies correctly under load
// via the single-entry Apply path, with the batched apply never engaged.
func TestApplyBatch_FallbackSingleApply(t *testing.T) {
	nodes, _ := newTestCluster(t, 3) // mockSM has no ApplyBatch
	lead := waitForStableLeader(nodes, 5*time.Second)
	if lead < 0 {
		t.Fatal("no stable leader")
	}
	leader := nodes[lead]

	const numCmds = 150
	for i := 0; i < numCmds; i++ {
		if err := submitToCluster(nodes, []byte(fmt.Sprintf("fb-cmd-%04d", i)), 10*time.Second); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// The batched path must never have been taken for a non-batch SM.
	if got := leader.batchApplyCalls.Load(); got != 0 {
		t.Errorf("batchApplyCalls = %d for a non-batch SM, want 0", got)
	}
	if got := leader.maxApplyBatch.Load(); got != 0 {
		t.Errorf("maxApplyBatch = %d for a non-batch SM, want 0", got)
	}

	want := make(map[string]bool, numCmds)
	for i := 0; i < numCmds; i++ {
		want[fmt.Sprintf("fb-cmd-%04d", i)] = true
	}
	assertIdenticalOrder(t, nodes, want, func(n *RaftNode) []string { return smOf(n).all() })
}

// TestApplyBatch_MixedWithConfigChange interleaves a membership change with a
// stream of batched writes and asserts everything still applies exactly once,
// in identical order, on every node — including the 4th node added mid-load.
// Configuration entries are non-batchable, so the applier must split write runs
// around them; this exercises that split path under concurrent load.
func TestApplyBatch_MixedWithConfigChange(t *testing.T) {
	nodes, transport, ids := newBatchCluster(t, 3)
	if waitForStableLeader(nodes, 5*time.Second) < 0 {
		t.Fatal("no stable leader")
	}

	const numCmds = 300
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make([]error, 24)
	for w := 0; w < 24; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				n := next.Add(1) - 1
				if n >= numCmds {
					return
				}
				cmd := []byte(fmt.Sprintf("mix-%04d", n))
				if err := submitToCluster(nodes, cmd, 15*time.Second); err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}

	// While writes are in flight, add a 4th node through the log.
	newID := t.Name() + "-node-3"
	node4, err := NewRaftNode(newID, ids, t.TempDir(), &batchMockSM{}, transport)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	transport.register(newID, node4)
	t.Cleanup(node4.Stop)

	time.Sleep(30 * time.Millisecond) // let some writes commit before the change
	if err := changeMembershipWithRetry(nodes, func(l *RaftNode) error {
		return l.AddServer(newID)
	}, 20*time.Second); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	wg.Wait()
	for w, e := range errs {
		if e != nil {
			t.Fatalf("worker %d: %v", w, e)
		}
	}

	all := append(append([]*RaftNode{}, nodes...), node4)
	li := waitForStableLeader(all, 10*time.Second)
	if li < 0 {
		t.Fatal("no stable leader after AddServer")
	}
	if got := configServers(all[li]); len(got) != 4 {
		t.Fatalf("leader config: want 4 servers, got %v", got)
	}

	want := make(map[string]bool, numCmds)
	for i := 0; i < numCmds; i++ {
		want[fmt.Sprintf("mix-%04d", i)] = true
	}
	assertIdenticalOrder(t, all, want, func(n *RaftNode) []string { return bsmOf(n).all() })
}
