package consensus

// chaos_test.go — Jepsen-style partition chaos for the Raft layer.
//
// These tests cycle random network partitions through a 5-node cluster while
// clients keep submitting, then heal and assert the two safety properties a
// Raft implementation must never violate:
//
//   1. No split-brain COMMIT: at most one value is ever committed at a given
//      log index. We assert this by requiring every node's applied log to be a
//      prefix of one another (identical order) — divergence at any index means
//      two leaders committed conflicting entries.
//   2. No acknowledged-write loss: every command whose Submit returned nil
//      (committed by quorum) is present on every node after healing.
//
// The partition model is bidirectional (via hub.forNode), so a minority that is
// cut off can neither reach nor be reached — exactly the conditions under which
// a naive implementation elects a second leader and diverges. The persistence
// hardening in HandleRequestVote/HandleAppendEntries/startElection is exercised
// continuously here: elections and appends happen throughout the partitions.

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// chaosCluster builds n nodes wired through per-node transport views so
// hub.cutNode(id) yields a true bidirectional partition.
type chaosCluster struct {
	nodes []*RaftNode
	ids   []string
	hub   *mockTransport
}

func newChaosCluster(t *testing.T, n int) *chaosCluster {
	t.Helper()
	c := &chaosCluster{hub: newMockTransport()}
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
		node, err := NewRaftNode(c.ids[i], peers, t.TempDir(), &mockSM{}, c.hub.forNode(c.ids[i]))
		if err != nil {
			t.Fatalf("NewRaftNode(%s): %v", c.ids[i], err)
		}
		c.nodes = append(c.nodes, node)
		c.hub.register(c.ids[i], node)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.Stop()
		}
	})
	return c
}

// TestChaos_RaftPartitionsNoSplitBrainNoLoss cycles random minority partitions
// while a writer keeps submitting, then heals and asserts log convergence and
// zero loss of committed writes.
func TestChaos_RaftPartitionsNoSplitBrainNoLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in -short mode")
	}
	const (
		numNodes  = 5
		numRounds = 8
		seed      = 0xC0FFEE
	)
	rng := rand.New(rand.NewSource(seed))
	c := newChaosCluster(t, numNodes)

	if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no initial stable leader")
	}

	// committed holds every command Submit acknowledged (quorum-committed).
	var mu sync.Mutex
	committed := map[string]bool{}
	seq := 0

	// submitBarrage tries to submit `count` commands to whatever majority leader
	// exists; records the ones that commit. Some may fail during a partition —
	// that is fine (the client would retry); we only assert on the ones that
	// succeeded (were acknowledged).
	submitBarrage := func(count int) {
		for i := 0; i < count; i++ {
			seq++
			cmd := fmt.Sprintf("cmd-%06d", seq)
			// Give the majority a chance to have/elect a leader.
			if err := submitWithRetry(c.nodes, []byte(cmd), 3*time.Second); err == nil {
				mu.Lock()
				committed[cmd] = true
				mu.Unlock()
			}
		}
	}

	// Warm-up writes with no partition.
	submitBarrage(5)

	for round := 0; round < numRounds; round++ {
		// Cut a random minority (1 or 2 of 5) so a majority always survives and
		// can still make progress — the interesting case for split-brain.
		cutCount := 1 + rng.Intn(2) // 1 or 2
		perm := rng.Perm(numNodes)
		cutIdx := perm[:cutCount]
		for _, idx := range cutIdx {
			c.hub.cutNode(c.ids[idx])
		}

		// Writes during the partition: the surviving majority must keep serving.
		submitBarrage(4)

		// Heal.
		for _, idx := range cutIdx {
			c.hub.healNode(c.ids[idx])
		}

		// Let the healed nodes catch up before the next round.
		submitBarrage(2)
	}

	// Final barrier: a stable leader must exist and one last write must commit,
	// then every node must converge to the identical applied log.
	if waitForStableLeader(c.nodes, 10*time.Second) < 0 {
		t.Fatal("no stable leader after chaos")
	}
	if err := submitWithRetry(c.nodes, []byte("barrier"), 5*time.Second); err != nil {
		t.Fatalf("barrier write failed to commit after heal: %v", err)
	}
	mu.Lock()
	committed["barrier"] = true
	want := make(map[string]bool, len(committed))
	for k := range committed {
		want[k] = true
	}
	mu.Unlock()

	// Wait for full replication convergence: every node applies exactly the same
	// number of entries, sustained across a check window.
	if !waitForConvergence(c.nodes, 15*time.Second) {
		for i, n := range c.nodes {
			t.Logf("node %d applied=%d", i, smOf(n).count())
		}
		t.Fatal("nodes did not converge to identical applied counts")
	}

	// Property 1 — NO SPLIT-BRAIN: every node's applied log is byte-for-byte
	// identical (same commands, same order). A divergence at any index would mean
	// two leaders committed conflicting entries at that index. (We do not assert
	// exactly-once or exactly-`want`: under a partition a client Submit can time
	// out yet still commit, and a retry of the same bytes can then apply twice —
	// both are legitimate Raft behaviours that an app-level request-id would
	// dedup. Neither is a safety violation; divergence would be.)
	ref := smOf(c.nodes[0]).all()
	for i := 1; i < len(c.nodes); i++ {
		got := smOf(c.nodes[i]).all()
		if len(got) != len(ref) {
			t.Fatalf("node%d applied %d entries, node0 applied %d — logs did not converge",
				i, len(got), len(ref))
		}
		for j := range ref {
			if got[j] != ref[j] {
				t.Fatalf("SPLIT-BRAIN: apply-order divergence at index %d: node0=%q node%d=%q",
					j, ref[j], i, got[j])
			}
		}
	}

	// Property 2 — NO ACKNOWLEDGED-WRITE LOSS: every command whose Submit
	// returned nil (quorum-committed) is present on the converged log.
	present := map[string]bool{}
	for _, cmd := range ref {
		present[cmd] = true
	}
	for cmd := range want {
		if !present[cmd] {
			t.Errorf("committed command %q lost after partitions", cmd)
		}
	}
	t.Logf("chaos survived: %d acknowledged writes, %d log entries replicated byte-identically across %d nodes over %d partition rounds",
		len(want), len(ref), numNodes, numRounds)
}

// waitForConvergence returns true once all nodes report the same applied count
// for two consecutive samples (a stable, converged cluster).
func waitForConvergence(nodes []*RaftNode, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	var last int = -1
	stable := 0
	for time.Now().Before(deadline) {
		n0 := smOf(nodes[0]).count()
		allEqual := true
		for _, n := range nodes[1:] {
			if smOf(n).count() != n0 {
				allEqual = false
				break
			}
		}
		if allEqual && n0 == last {
			stable++
			if stable >= 2 {
				return true
			}
		} else {
			stable = 0
		}
		last = n0
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
