package integration_test

// replicated_recovery_test.go — --mode=replicated recovery of a replica that
// was down while its primary restarted (invariant 61).
//
// Before the per-replica recovery state was persisted, the primary forgot on
// restart which writes a down replica had not acknowledged: the replica came
// back, the primary treated it as caught up, and every write and delete made
// before the restart was missing on it forever.

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// replNode is one replicated-mode server process whose data dir survives
// restarts (testServer.stop removes it).
type replNode struct {
	t       *testing.T
	id      string
	port    int
	peers   string
	cons    string
	dataDir string
	cmd     *exec.Cmd
	extra   []string
}

func (n *replNode) addr() string { return fmt.Sprintf("127.0.0.1:%d", n.port) }

func (n *replNode) start() {
	n.t.Helper()
	flags := []string{
		"-addr", n.addr(),
		"-metrics-addr", fmt.Sprintf("127.0.0.1:%d", n.port+4),
		"-data", n.dataDir,
		"-wal-flush-window-ms", "1",
		"-vlog-flush-window-ms", "1",
		"-mode", "replicated", "-node-id", n.id, "-peers", n.peers,
		"-consistency", n.cons,
	}
	flags = append(flags, n.extra...)
	bin := serverBinary()
	if bin == "" {
		bin = builtBinary(n.t)
	}
	cmd := exec.Command(bin, flags...)
	cmd.Dir = moduleRoot(n.t)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr
	if err := cmd.Start(); err != nil {
		n.t.Fatalf("start %s: %v", n.id, err)
	}
	n.cmd = cmd
	s := &testServer{Addr: n.addr(), t: n.t}
	if err := s.waitReady(20 * time.Second); err != nil {
		n.kill()
		n.t.Fatalf("%s not ready: %v", n.id, err)
	}
}

// kill stops the process like a crash (SIGKILL); the data dir is kept.
func (n *replNode) kill() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
	}
	n.cmd = nil
}

// shutdown stops the process cleanly (SIGINT → engine checkpoint).
func (n *replNode) shutdown() {
	n.t.Helper()
	if n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Signal(gracefulShutdownSignal())
	done := make(chan struct{})
	go func() { _ = n.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = n.cmd.Process.Kill()
		<-done
		n.t.Errorf("%s did not exit within 20s of SIGINT", n.id)
	}
	n.cmd = nil
}

// startReplNodes launches an n-node replicated cluster whose nodes can be
// stopped and restarted on the same ports and data dirs.
func startReplNodes(t *testing.T, n int, consistency string, extra ...string) []*replNode {
	t.Helper()
	used := map[int]bool{}
	ids := make([]string, n)
	ports := make([]int, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("r%d", i+1)
		ports[i] = reserveBlock(t, used)
	}
	pf := peersFlag(ids, ports)
	nodes := make([]*replNode, n)
	for i := range nodes {
		dir, err := os.MkdirTemp("", "veltrix-replrec-")
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &replNode{t: t, id: ids[i], port: ports[i], peers: pf, cons: consistency, dataDir: dir, extra: extra}
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.kill()
			os.RemoveAll(nd.dataDir)
		}
	})
	for _, nd := range nodes {
		nd.start()
	}
	time.Sleep(500 * time.Millisecond) // let the replication clients connect
	return nodes
}

func mustSend(t *testing.T, addr, cmd, want string) {
	t.Helper()
	c := newTextClient(t, addr)
	defer c.close()
	if got := c.send(cmd); got != want {
		t.Fatalf("%s on %s: got %q, want %q", cmd, addr, got, want)
	}
}

// waitReplicaConverged polls addr until every key in live has its value and
// no key in deleted is readable, or the deadline passes.  It returns a
// description of what is still wrong ("" when converged).
func waitReplicaConverged(t *testing.T, addr string, live map[string]string, deleted []string, d time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(d)
	var problem string
	for {
		problem = ""
		c := newTextClient(t, addr)
		missing, resurrected := 0, 0
		var exM, exR string
		for k, v := range live {
			if got := c.send("GET " + k); got != v {
				if missing == 0 {
					exM = fmt.Sprintf("%s=%q", k, got)
				}
				missing++
			}
		}
		for _, k := range deleted {
			if got := c.send("GET " + k); !strings.HasPrefix(got, "ERR") && !strings.HasPrefix(got, "NOT_FOUND") && got != "" && got != "(nil)" {
				if resurrected == 0 {
					exR = fmt.Sprintf("%s=%q", k, got)
				}
				resurrected++
			}
		}
		c.close()
		if missing == 0 && resurrected == 0 {
			return ""
		}
		problem = fmt.Sprintf("%d/%d writes missing (e.g. %s), %d/%d deletes not applied (e.g. %s)",
			missing, len(live), exM, resurrected, len(deleted), exR)
		if time.Now().After(deadline) {
			return problem
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// runPrimaryRestartScenario: the replica (last node) is down while the
// primary (node 0) acknowledges writes and deletes and restarts once per
// entry of restarts ("crash" = SIGKILL, "clean" = SIGINT); the replica then
// comes back and must converge.
func runPrimaryRestartScenario(t *testing.T, nNodes int, restarts ...string) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	const n = 200
	nodes := startReplNodes(t, nNodes, "quorum")
	primary, replica := nodes[0], nodes[nNodes-1]

	// Keys that exist everywhere before the replica goes down; they are
	// deleted while it is down.
	for i := 0; i < n; i++ {
		mustSend(t, primary.addr(), fmt.Sprintf("PUT del-%d old-%d", i, i), "OK")
	}
	if p := waitReplicaConverged(t, replica.addr(), map[string]string{"del-0": "old-0", fmt.Sprintf("del-%d", n-1): fmt.Sprintf("old-%d", n-1)}, nil, 15*time.Second); p != "" {
		t.Fatalf("baseline never reached the replica: %s", p)
	}

	replica.kill()

	live := map[string]string{}
	var deleted []string
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("new-%d", i), fmt.Sprintf("val-%d", i)
		// Quorum = 2 of 3 copies: the primary and the middle node.
		mustSend(t, primary.addr(), fmt.Sprintf("PUT %s %s", k, v), "OK")
		live[k] = v
		mustSend(t, primary.addr(), fmt.Sprintf("DEL del-%d", i), "OK")
		deleted = append(deleted, fmt.Sprintf("del-%d", i))
	}

	for r, how := range restarts {
		if how == "clean" {
			primary.shutdown()
		} else {
			primary.kill()
		}
		primary.start()
		// A write after each restart, also missed by the replica.
		k := fmt.Sprintf("after-restart-%d", r)
		mustSend(t, primary.addr(), fmt.Sprintf("PUT %s v%d", k, r), "OK")
		live[k] = fmt.Sprintf("v%d", r)
		time.Sleep(1500 * time.Millisecond) // past at least one persist tick
	}

	replica.start()
	if p := waitReplicaConverged(t, replica.addr(), live, deleted, 60*time.Second); p != "" {
		t.Fatalf("replica did not converge after the primary restarted: %s", p)
	}
	// The returning replica's own catch-up of its peers must not push its
	// stale copies back (it re-sends only writes it originated): every other
	// node still has every write and every delete.
	for _, nd := range nodes[:nNodes-1] {
		if p := waitReplicaConverged(t, nd.addr(), live, deleted, 10*time.Second); p != "" {
			t.Fatalf("%s diverged after the replica came back: %s", nd.id, p)
		}
	}
}

// TestReplicatedRecovery_ReplicaDownAcrossPrimaryCrash: 3 nodes, quorum,
// primary SIGKILLed while the replica is down.
func TestReplicatedRecovery_ReplicaDownAcrossPrimaryCrash(t *testing.T) {
	runPrimaryRestartScenario(t, 3, "crash")
}

// TestReplicatedRecovery_ReplicaDownAcrossPrimaryCleanRestart: same, but the
// primary shuts down cleanly — its WAL checkpoint must keep the tombstones a
// replica has not acknowledged, or the catch-up has no deletes to send.
func TestReplicatedRecovery_ReplicaDownAcrossPrimaryCleanRestart(t *testing.T) {
	runPrimaryRestartScenario(t, 3, "clean")
}

// TestReplicatedRecovery_ReplicaDownAcrossTwoPrimaryRestarts: the replica is
// still down when the primary restarts a second time (one crash, one clean).
func TestReplicatedRecovery_ReplicaDownAcrossTwoPrimaryRestarts(t *testing.T) {
	runPrimaryRestartScenario(t, 3, "crash", "clean")
}
