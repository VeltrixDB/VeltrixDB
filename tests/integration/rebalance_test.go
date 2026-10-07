package integration_test

// rebalance_test.go — a node failure must not make the auto-rebalancer
// delete data (CLAUDE.md invariant 62). Before the fix, a 3-node raft
// cluster with the default --auto-rebalance=true lost roughly two thirds of
// its keys on each survivor once the killed leader was marked FAILED: every
// node sent each key to its single ring owner and deleted it locally,
// outside the raft log.

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestRaftAutoRebalance_LeaderKillKeepsEveryKey(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	used := map[int]bool{}
	ids := []string{"rb1", "rb2", "rb3"}
	ports := []int{reserveBlock(t, used), reserveBlock(t, used), reserveBlock(t, used)}
	pf := peersFlag(ids, ports)
	nodes := make([]*testServer, 3)
	for i := range ids {
		// --auto-rebalance is left at its default (true).
		nodes[i] = startNode(t, ports[i], ports[i]+4, raftArgs(t, ids[i], pf)...)
		defer nodes[i].stop()
	}
	addrs := []string{nodes[0].Addr, nodes[1].Addr, nodes[2].Addr}
	leaderID, leaderAddr := waitForRaftLeader(t, addrs, 20*time.Second)
	checkRaftPipeline(t, addrs)

	const n = 2000
	lc := newTextClient(t, leaderAddr)
	for i := 0; i < n; i++ {
		if resp := lc.send(fmt.Sprintf("PUT rbkey:%05d v%05d", i, i)); resp != "OK" {
			t.Fatalf("PUT %d via leader: %q", i, resp)
		}
	}
	lc.close()

	var survivors []string
	for i, a := range addrs {
		if a == leaderAddr {
			nodes[i].stop()
		} else {
			survivors = append(survivors, a)
		}
	}

	// Wait until both survivors' failure detectors mark the old leader
	// FAILED (the event that used to start the destructive migration), then
	// past the rebalancer's 3 s debounce.
	deadline := time.Now().Add(40 * time.Second)
	for {
		failed := 0
		for _, a := range survivors {
			tp, err := queryTopoStates(t, a)
			if err == nil && tp[leaderID] == "FAILED" {
				failed++
			}
		}
		if failed == len(survivors) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("survivors never marked %s FAILED", leaderID)
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(6 * time.Second)

	// Every survivor still serves every key (local reads), and so does the
	// new leader.
	newLeaderID, newLeaderAddr := waitForRaftLeader(t, survivors, 25*time.Second)
	t.Logf("new leader %s", newLeaderID)
	for _, a := range append(survivors, newLeaderAddr) {
		c := newTextClient(t, a)
		missing := 0
		first := ""
		for i := 0; i < n; i++ {
			if got := c.send(fmt.Sprintf("GET rbkey:%05d", i)); got != fmt.Sprintf("v%05d", i) {
				if missing == 0 {
					first = fmt.Sprintf("rbkey:%05d=%q", i, got)
				}
				missing++
			}
		}
		c.close()
		if missing != 0 {
			t.Fatalf("node %s lost %d/%d keys after the leader was killed (first: %s)", a, missing, n, first)
		}
	}
}

// queryTopoStates returns node ID → state from TOPOLOGY on addr.
func queryTopoStates(t *testing.T, addr string) (map[string]string, error) {
	t.Helper()
	c := newTextClient(t, addr)
	defer c.close()
	var tp struct {
		Nodes []struct {
			NodeID string `json:"node_id"`
			State  string `json:"state"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(c.send("TOPOLOGY")), &tp); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, n := range tp.Nodes {
		out[n.NodeID] = n.State
	}
	return out, nil
}
