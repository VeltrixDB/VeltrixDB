package integration_test

// distributed_test.go — end-to-end tests that spin up a REAL 3-node VeltrixDB
// cluster (three server processes on random ports + temp data dirs) and exercise
// the distributed serving path: raft leader election/failover + follower
// redirect, replicated-mode quorum/strong/async consistency, and cluster-client
// consistent-hash routing.  These are driven through the actual server binary,
// not the raft/replication libraries in isolation.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	veltrixclient "github.com/VeltrixDB/veltrixdb/client"
)

// ── cluster harness ──────────────────────────────────────────────────────────

// reserveBlock returns a base client port P for which P..P+5 are all bindable:
// client (P), replication (P+1), raft (P+2), gossip (P+3), metrics (P+4).
// bases already handed out are avoided so per-node port blocks never overlap.
func reserveBlock(t *testing.T, used map[int]bool) int {
	t.Helper()
	for tries := 0; tries < 200; tries++ {
		base := freePort(t)
		clash := false
		for off := 0; off <= 5; off++ {
			if used[base+off] {
				clash = true
				break
			}
		}
		if clash {
			continue
		}
		ok := true
		for off := 1; off <= 5; off++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+off))
			if err != nil {
				ok = false
				break
			}
			l.Close()
		}
		if !ok {
			continue
		}
		for off := 0; off <= 5; off++ {
			used[base+off] = true
		}
		return base
	}
	t.Fatal("reserveBlock: no free 6-port block found")
	return 0
}

// startNode launches one server process on the given client port with a temp
// data dir and the supplied extra flags.  It waits for the TCP port to accept.
func startNode(t *testing.T, clientPort, metricsPort int, extraArgs ...string) *testServer {
	t.Helper()
	dataDir, err := os.MkdirTemp("", "veltrix-cluster-")
	if err != nil {
		t.Fatalf("startNode: mkdirtemp: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", clientPort)
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", metricsPort)

	flags := []string{
		"-addr", addr,
		"-metrics-addr", metricsAddr,
		"-data", dataDir,
		"-wal-flush-window-ms", "1",
		"-vlog-flush-window-ms", "1",
	}
	flags = append(flags, extraArgs...)

	bin := serverBinary()
	if bin == "" {
		bin = builtBinary(t)
	}
	cmd := exec.Command(bin, flags...)
	cmd.Dir = moduleRoot(t)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("startNode: start: %v", err)
	}
	s := &testServer{Addr: addr, MetricsAddr: metricsAddr, DataDir: dataDir, cmd: cmd, t: t}
	if err := s.waitReady(20 * time.Second); err != nil {
		s.stop()
		t.Fatalf("startNode: not ready: %v", err)
	}
	return s
}

// peersFlag builds the shared --peers value for a set of node id/port pairs.
func peersFlag(ids []string, ports []int) string {
	out := ""
	for i := range ids {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%s@127.0.0.1:%d", ids[i], ports[i])
	}
	return out
}

// topo is the subset of the TOPOLOGY / /admin/cluster JSON the tests inspect.
type topo struct {
	NodeID string `json:"node_id"`
	Mode   string `json:"mode"`
	Epoch  uint64 `json:"epoch"`
	Raft   *struct {
		Role     string `json:"role"`
		Term     uint64 `json:"term"`
		LeaderID string `json:"leader_id"`
		IsLeader bool   `json:"is_leader"`
	} `json:"raft"`
	Nodes []struct {
		NodeID string `json:"node_id"`
	} `json:"nodes"`
}

// queryTopo runs the TOPOLOGY command against addr.
func queryTopo(t *testing.T, addr string) (topo, error) {
	t.Helper()
	c := newTextClient(t, addr)
	defer c.close()
	line := c.send("TOPOLOGY")
	var tp topo
	if err := json.Unmarshal([]byte(line), &tp); err != nil {
		return tp, fmt.Errorf("bad topology %q: %w", line, err)
	}
	return tp, nil
}

// waitForLeader polls all node addresses until one reports IsLeader and all
// agree on the same non-empty leader id.  Returns the leader's client address.
func waitForRaftLeader(t *testing.T, addrs []string, timeout time.Duration) (string, string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaderID := ""
		leaderAddr := ""
		agree := 0
		for _, a := range addrs {
			tp, err := queryTopo(t, a)
			if err != nil || tp.Raft == nil {
				continue
			}
			if tp.Raft.IsLeader {
				leaderID = tp.Raft.LeaderID
				leaderAddr = a
			}
		}
		if leaderID != "" {
			for _, a := range addrs {
				tp, err := queryTopo(t, a)
				if err == nil && tp.Raft != nil && tp.Raft.LeaderID == leaderID {
					agree++
				}
			}
			if agree >= (len(addrs)/2)+1 {
				return leaderID, leaderAddr
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("waitForLeader: no stable leader within %s", timeout)
	return "", ""
}

// ── Raft mode ────────────────────────────────────────────────────────────────

// TestRaftClusterFailover proves: a 3-node raft cluster elects a leader; writes
// through the leader replicate & apply; a write to a follower is redirected
// (MOVED); killing the leader triggers re-election; committed data survives and
// new writes succeed on the new leader.
func TestRaftClusterFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	used := map[int]bool{}
	ids := []string{"n1", "n2", "n3"}
	ports := []int{reserveBlock(t, used), reserveBlock(t, used), reserveBlock(t, used)}
	metrics := []int{ports[0] + 4, ports[1] + 4, ports[2] + 4}
	pf := peersFlag(ids, ports)

	nodes := make([]*testServer, 3)
	for i := range ids {
		nodes[i] = startNode(t, ports[i], metrics[i],
			"-mode", "raft", "-node-id", ids[i], "-peers", pf)
		defer nodes[i].stop()
	}
	addrs := []string{nodes[0].Addr, nodes[1].Addr, nodes[2].Addr}

	leaderID, leaderAddr := waitForRaftLeader(t, addrs, 20*time.Second)
	t.Logf("elected leader %s at %s", leaderID, leaderAddr)

	// Write through the leader.
	lc := newTextClient(t, leaderAddr)
	if resp := lc.send("PUT survives yes"); resp != "OK" {
		t.Fatalf("leader PUT: %q", resp)
	}
	lc.close()

	// A follower must redirect writes with MOVED.
	var followerAddr string
	for _, a := range addrs {
		if a != leaderAddr {
			followerAddr = a
			break
		}
	}
	fc := newTextClient(t, followerAddr)
	resp := fc.send("PUT shouldredirect nope")
	fc.close()
	if !contains(resp, "MOVED") {
		t.Fatalf("follower PUT should redirect with MOVED, got %q", resp)
	}
	t.Logf("follower correctly redirected: %q", resp)

	// Kill the leader; the remaining quorum must re-elect.
	var survivors []string
	for i, a := range addrs {
		if a == leaderAddr {
			nodes[i].stop() // kill leader process (+ its data dir)
		} else {
			survivors = append(survivors, a)
		}
	}

	newLeaderID, newLeaderAddr := waitForRaftLeader(t, survivors, 25*time.Second)
	if newLeaderID == leaderID {
		t.Fatalf("expected a new leader after killing %s, still %s", leaderID, newLeaderID)
	}
	t.Logf("re-elected new leader %s at %s", newLeaderID, newLeaderAddr)

	// Committed data survived the failover.
	nc := newTextClient(t, newLeaderAddr)
	defer nc.close()
	if got := nc.send("GET survives"); got != "yes" {
		t.Fatalf("data lost across failover: GET survives = %q, want yes", got)
	}
	// New writes succeed on the new leader.
	if resp := nc.send("PUT afterfailover ok"); resp != "OK" {
		t.Fatalf("new-leader PUT after failover: %q", resp)
	}
	if got := nc.send("GET afterfailover"); got != "ok" {
		t.Fatalf("new-leader GET after failover: %q", got)
	}
}

// ── Replicated mode ──────────────────────────────────────────────────────────

// startReplicatedCluster launches a 3-node replicated cluster with the given
// consistency level and returns the node handles + addresses.
func startReplicatedCluster(t *testing.T, consistency string) ([]*testServer, []string) {
	t.Helper()
	used := map[int]bool{}
	ids := []string{"r1", "r2", "r3"}
	ports := []int{reserveBlock(t, used), reserveBlock(t, used), reserveBlock(t, used)}
	metrics := []int{ports[0] + 4, ports[1] + 4, ports[2] + 4}
	pf := peersFlag(ids, ports)
	nodes := make([]*testServer, 3)
	for i := range ids {
		nodes[i] = startNode(t, ports[i], metrics[i],
			"-mode", "replicated", "-node-id", ids[i], "-peers", pf,
			"-consistency", consistency)
	}
	addrs := []string{nodes[0].Addr, nodes[1].Addr, nodes[2].Addr}
	return nodes, addrs
}

// TestReplicatedAsyncPropagates proves eventual consistency: a write ACKs at the
// primary immediately and the value appears on a replica shortly after.
func TestReplicatedAsyncPropagates(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	nodes, addrs := startReplicatedCluster(t, "eventual")
	for _, n := range nodes {
		defer n.stop()
	}
	// Let AddReplica clients connect.
	time.Sleep(500 * time.Millisecond)

	pc := newTextClient(t, addrs[0])
	if resp := pc.send("PUT asynckey asyncval"); resp != "OK" {
		t.Fatalf("primary PUT: %q", resp)
	}
	pc.close()

	// The replicas should receive it in the background within a few seconds.
	found := false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && !found {
		rc := newTextClient(t, addrs[1])
		got := rc.send("GET asynckey")
		rc.close()
		if got == "asyncval" {
			found = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !found {
		t.Fatal("async write never propagated to replica within 8s")
	}
}

// TestReplicatedStrongErrorsWithReplicasDown proves strong consistency gating:
// with 2 of 3 replicas down, a strong write on the survivor errors instead of
// silently ACKing.
func TestReplicatedStrongErrorsWithReplicasDown(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	nodes, addrs := startReplicatedCluster(t, "strong")
	// Node 0 is the survivor/primary for this test.
	defer nodes[0].stop()
	time.Sleep(500 * time.Millisecond)

	// A strong write with all replicas up should succeed.
	pc := newTextClient(t, addrs[0])
	if resp := pc.send("PUT strongok v1"); resp != "OK" {
		pc.close()
		t.Fatalf("strong PUT with all up should succeed, got %q", resp)
	}
	pc.close()

	// Kill the other two replicas.
	nodes[1].stop()
	nodes[2].stop()
	time.Sleep(500 * time.Millisecond)

	// A strong write now cannot reach all replicas → must error.
	pc2 := newTextClient(t, addrs[0])
	defer pc2.close()
	resp := pc2.send("PUT strongfail v2")
	if resp == "OK" {
		t.Fatalf("strong PUT with 2/3 replicas down must NOT ACK, got OK")
	}
	t.Logf("strong write correctly errored with replicas down: %q", resp)
}

// ── Cluster client routing ───────────────────────────────────────────────────

// TestClusterClientRouting proves the cluster-aware client writes N keys across
// the cluster (routed by consistent hash) and reads them all back correctly.
// Replicated mode is used so each key's owner node serves both its write and its
// read deterministically.
func TestClusterClientRouting(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	nodes, addrs := startReplicatedCluster(t, "quorum")
	for _, n := range nodes {
		defer n.stop()
	}
	time.Sleep(500 * time.Millisecond)

	cli, err := veltrixclient.NewClient(addrs, veltrixclient.DefaultClientConfig())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	// Confirm the client discovered all three nodes via TOPOLOGY.
	nodeAddrs, _ := cli.Topology()
	if len(nodeAddrs) != 3 {
		t.Fatalf("client topology has %d nodes, want 3: %v", len(nodeAddrs), nodeAddrs)
	}

	ctx := context.Background()
	const n = 200
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("route-key-%04d", i)
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := cli.Put(ctx, key, val); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("route-key-%04d", i)
		want := fmt.Sprintf("val-%d", i)
		got, err := cli.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get %s = %q, want %q", key, got, want)
		}
	}
	t.Logf("cluster client round-tripped %d keys across %d nodes via routing", n, len(nodeAddrs))
}

// contains reports whether s contains substr.
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
