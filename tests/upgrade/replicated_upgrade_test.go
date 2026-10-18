package upgrade_test

// replicated_upgrade_test.go — --mode=replicated (--consistency=quorum):
// 3 × v1.16, every node writing its own keys, upgraded one node at a time
// while the other two keep writing.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func replicatedCluster(t *testing.T, dir string, ids ...string) []*node {
	t.Helper()
	nodes := make([]*node, len(ids))
	for i, id := range ids {
		nodes[i] = newNode(t, dir, id, nil)
	}
	var peers []string
	for _, n := range nodes {
		peers = append(peers, fmt.Sprintf("%s@127.0.0.1:%d", n.id, n.port))
	}
	for _, n := range nodes {
		n.args = []string{"-mode", "replicated", "-node-id", n.id, "-peers", strings.Join(peers, ","), "-consistency", "quorum"}
	}
	return nodes
}

// waitReplSync waits until every running node reports every replica SYNC.
func waitReplSync(t *testing.T, nodes []*node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	for time.Now().Before(deadline) {
		last = last[:0]
		for _, n := range nodes {
			tp, err := topology(n.addr())
			if err != nil {
				last = append(last, n.id+": "+err.Error())
				continue
			}
			if len(tp.Replication) != len(nodes)-1 {
				last = append(last, fmt.Sprintf("%s: %d replicas", n.id, len(tp.Replication)))
			}
			for _, r := range tp.Replication {
				if r.State != "SYNC" {
					last = append(last, fmt.Sprintf("%s→%s %s", n.id, r.NodeID, r.State))
				}
			}
		}
		if len(last) == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("replicas not SYNC after %s: %v", timeout, last)
}

// syncRepl has every node write a marker and waits until all nodes hold all
// markers: each node's earlier writes have reached every peer.
func syncRepl(t *testing.T, nodes []*node, tag string) {
	t.Helper()
	val := fmt.Sprintf("%d", time.Now().UnixNano())
	for _, n := range nodes {
		if resp, err := one(n.addr(), "PUT sync/"+tag+"/"+n.id+" "+val); err != nil || resp != "OK" {
			t.Fatalf("marker on %s: %q %v", n.id, resp, err)
		}
	}
	for _, n := range nodes {
		for _, src := range nodes {
			if err := waitKey(n.addr(), "sync/"+tag+"/"+src.id, val, 60*time.Second); err != nil {
				t.Fatalf("%s's writes did not reach %s: %v", src.id, n.id, err)
			}
		}
	}
}

func TestUpgrade_ReplicatedRolling(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := replicatedCluster(t, dir, "r1", "r2", "r3")
	for _, n := range nodes {
		n.start(verOld)
	}
	waitReplSync(t, nodes, 30*time.Second)
	m := newModel()
	for _, n := range nodes {
		a, u := runOps(t, directWriter(n), m, 8, baseOps("pre-"+n.id, 1500, 240))
		t.Logf("v1.16 load on %s: %d acknowledged, %d unknown", n.id, a, u)
	}
	syncRepl(t, nodes, "loaded")
	verifyCluster(t, nodes, m, "v1.16 baseline")
	if t.Failed() {
		t.Fatal("baseline on v1.16 already inconsistent — not an upgrade problem")
	}

	// One background writer per node, on that node's own keys. The writer of
	// the node being upgraded is stopped first (drain), the others keep going
	// while it is down, so it must be caught up by the old/new peers.
	bgs := map[string]*background{}
	for _, n := range nodes {
		bgs[n.id] = startBackground(directWriter(n), m, "bg-"+n.id+"-0")
	}
	for round, target := range nodes {
		time.Sleep(2 * time.Second)
		bgs[target.id].stop(t)
		syncRepl(t, nodes, fmt.Sprintf("drain-%s", target.id))
		t.Logf("upgrade %d/3: %s", round+1, target.id)
		target.swap(verNew)
		waitReplSync(t, nodes, 90*time.Second)
		syncRepl(t, nodes, fmt.Sprintf("after-%s", target.id))
		bgs[target.id] = startBackground(directWriter(target), m, fmt.Sprintf("bg-%s-%d", target.id, round+1))
	}
	time.Sleep(2 * time.Second)
	for _, n := range nodes {
		bgs[n.id].stop(t)
	}
	syncRepl(t, nodes, "upgraded")
	verifyCluster(t, nodes, m, "after rolling upgrade (replicated)")
	logEvidence(t, nodes, "catch-up", "CatchUp", "recover")
	checkNoPanics(t, nodes)
}
