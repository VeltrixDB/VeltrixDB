package jepsen

// cluster_test.go — real server processes: build the binary once, start a
// 3-node raft or replicated cluster on loopback, and kill -9 / SIGSTOP /
// SIGCONT / restart individual nodes on their persistent data dirs. The port
// layout and flags follow tests/integration (distributed_test.go,
// raft_snapshot_test.go): client P, replication P+1, raft P+2, gossip P+3,
// metrics P+4, transfer P+5.

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	buildOnce sync.Once
	builtBin  string
	buildErr  error
	buildDir  string
)

// serverBinary returns VELTRIX_SERVER_BIN, or builds ./cmd/server once per
// test binary (named veltrixdb-jepsen-server so stray processes are easy to
// find).
func serverBinary(t *testing.T) string {
	t.Helper()
	if b := os.Getenv("VELTRIX_SERVER_BIN"); b != "" {
		return b
	}
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "veltrix-jepsen-bin-")
		if err != nil {
			buildErr = err
			return
		}
		buildDir = dir
		out := filepath.Join(dir, "veltrixdb-jepsen-server")
		c := exec.Command("go", "build", "-o", out, "./cmd/server")
		c.Dir = moduleRoot()
		c.Stdout, c.Stderr = os.Stderr, os.Stderr
		if err := c.Run(); err != nil {
			buildErr = fmt.Errorf("go build ./cmd/server: %w", err)
			return
		}
		builtBin = out
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtBin
}

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

func moduleRoot() string {
	dir, _ := filepath.Abs("../..")
	return dir
}

// Nodes listen on ports drawn from [portLo, portHi): below the ephemeral
// range of every OS (Linux 32768+, macOS 49152+), so a client's outgoing
// connection can never take a port a killed node needs back on restart, and
// disjoint from tests/integration's [20000, 32000) so the suites can run
// side by side.
const portLo, portHi = 12000, 20000

func portFree(port int) bool {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	l.Close()
	return true
}

// reserveBlock returns a base port P with P..P+5 all free and not handed out.
func reserveBlock(t *testing.T, rng *rand.Rand, used map[int]bool) int {
	t.Helper()
	for tries := 0; tries < 1000; tries++ {
		base := portLo + rng.Intn(portHi-portLo-5)
		ok := true
		for off := 0; off <= 5 && ok; off++ {
			ok = !used[base+off] && portFree(base+off)
		}
		if !ok {
			continue
		}
		for off := 0; off <= 5; off++ {
			used[base+off] = true
		}
		return base
	}
	t.Fatal("reserveBlock: no free 6-port block")
	return 0
}

// node is one server process with a persistent data dir.
type node struct {
	id      string
	port    int
	dataDir string
	logPath string
	args    []string
	bin     string
	onExit  func(n *node, err error, expected bool)

	mu       sync.Mutex
	cmd      *exec.Cmd
	exited   chan error
	running  bool
	paused   bool
	stopping bool // an exit we caused (kill) or expect (crash-stage env)
	logf     *os.File
}

func (n *node) addr() string { return fmt.Sprintf("127.0.0.1:%d", n.port) }

// start launches the process with extra env; it does not wait for readiness.
// expectExit marks an exit as expected (a crash-stage hook run).
func (n *node) start(expectExit bool, env ...string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.running {
		return fmt.Errorf("%s already running", n.id)
	}
	f, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	fmt.Fprintf(f, "\n===== jepsen: start %s at %s env=%v =====\n", n.id, time.Now().Format(time.RFC3339Nano), env)
	cmd := exec.Command(n.bin, n.args...)
	cmd.Dir = moduleRoot()
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		return err
	}
	n.cmd, n.logf = cmd, f
	n.running, n.paused, n.stopping = true, false, expectExit
	ch := make(chan error, 1)
	n.exited = ch
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		expected := n.stopping
		if n.cmd == cmd {
			n.running, n.paused = false, false
		}
		f.Close()
		n.mu.Unlock()
		ch <- err
		if n.onExit != nil {
			n.onExit(n, err, expected)
		}
	}()
	return nil
}

// waitReady waits until the client port accepts connections.
func (n *node) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", n.addr(), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		if !n.isRunning() {
			return fmt.Errorf("%s exited while starting", n.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s not ready after %v", n.id, timeout)
}

func (n *node) isRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running
}

func (n *node) isUp() bool { // running and not paused
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running && !n.paused
}

// kill SIGKILLs the process (no shutdown path runs) and waits for it.
func (n *node) kill() {
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return
	}
	n.stopping = true
	cmd, ch := n.cmd, n.exited
	n.mu.Unlock()
	_ = cmd.Process.Signal(syscall.SIGKILL)
	<-ch
}

func (n *node) signal(sig syscall.Signal, paused bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return fmt.Errorf("%s not running", n.id)
	}
	if err := n.cmd.Process.Signal(sig); err != nil {
		return err
	}
	n.paused = paused
	return nil
}

func (n *node) pause() error  { return n.signal(syscall.SIGSTOP, true) }
func (n *node) resume() error { return n.signal(syscall.SIGCONT, false) }

// waitExit waits for an exit we expect (crash-stage hook).
func (n *node) waitExit(timeout time.Duration) (error, bool) {
	n.mu.Lock()
	ch := n.exited
	n.mu.Unlock()
	if ch == nil {
		return nil, false
	}
	select {
	case err := <-ch:
		ch <- err // keep it readable for kill()
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

// logTail returns the last n lines of the node's log.
func (n *node) logTail(lines int) string {
	b, err := os.ReadFile(n.logPath)
	if err != nil {
		return err.Error()
	}
	all := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}

// clusterConfig selects the mode and server flags.
type clusterConfig struct {
	mode           string // raft | replicated
	linReads       bool   // raft: --linearizable-reads
	consistency    string // replicated: --consistency
	snapThreshold  int    // raft: --raft-snapshot-threshold (0 = server default)
	snapChunkKB    int
	pipeline       bool
	extraArgs      []string
	unexpectedExit func(n *node, err error)
}

type cluster struct {
	t     *testing.T
	cfg   clusterConfig
	nodes []*node
	dir   string // data dirs and logs
}

func startCluster(t *testing.T, cfg clusterConfig, workDir string) *cluster {
	t.Helper()
	bin := serverBinary(t)
	used := map[int]bool{}
	ids := []string{"n1", "n2", "n3"}
	var ports []int
	prng := rand.New(rand.NewSource(time.Now().UnixNano())) // ports: not part of the seeded run
	for range ids {
		ports = append(ports, reserveBlock(t, prng, used))
	}
	var peers []string
	for i, id := range ids {
		peers = append(peers, fmt.Sprintf("%s@127.0.0.1:%d", id, ports[i]))
	}
	pf := strings.Join(peers, ",")
	c := &cluster{t: t, cfg: cfg, dir: workDir}
	for i, id := range ids {
		dd := filepath.Join(workDir, id+"-data")
		if err := os.MkdirAll(dd, 0o755); err != nil {
			t.Fatal(err)
		}
		args := []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", ports[i]),
			"-metrics-addr", fmt.Sprintf("127.0.0.1:%d", ports[i]+4),
			"-data", dd,
			"-wal-flush-window-ms", "1",
			"-vlog-flush-window-ms", "1",
			"-cache", "32",
			"-mode", cfg.mode, "-node-id", id, "-peers", pf,
		}
		switch cfg.mode {
		case "raft":
			args = append(args, "-linearizable-reads="+strconv.FormatBool(cfg.linReads),
				"-raft-pipeline="+strconv.FormatBool(cfg.pipeline))
			if cfg.snapThreshold > 0 {
				args = append(args, "-raft-snapshot-threshold", strconv.Itoa(cfg.snapThreshold))
			}
			if cfg.snapChunkKB > 0 {
				args = append(args, "-raft-snapshot-chunk-kb", strconv.Itoa(cfg.snapChunkKB))
			}
		case "replicated":
			args = append(args, "-consistency", cfg.consistency)
		}
		args = append(args, cfg.extraArgs...)
		n := &node{id: id, port: ports[i], dataDir: dd, logPath: filepath.Join(workDir, id+".log"), args: args, bin: bin}
		n.onExit = func(n *node, err error, expected bool) {
			if !expected && cfg.unexpectedExit != nil {
				cfg.unexpectedExit(n, err)
			}
		}
		c.nodes = append(c.nodes, n)
	}
	t.Cleanup(c.stopAll)
	for _, n := range c.nodes {
		if err := n.start(false); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range c.nodes {
		if err := n.waitReady(30 * time.Second); err != nil {
			t.Fatalf("%v\n%s", err, n.logTail(30))
		}
	}
	return c
}

// stopAll SIGCONTs and SIGKILLs every node (test cleanup).
func (c *cluster) stopAll() {
	for _, n := range c.nodes {
		n.mu.Lock()
		if n.running && n.paused {
			_ = n.cmd.Process.Signal(syscall.SIGCONT)
		}
		n.mu.Unlock()
		n.kill()
	}
}

func (c *cluster) addrs() []string {
	var out []string
	for _, n := range c.nodes {
		out = append(out, n.addr())
	}
	return out
}

func (c *cluster) byAddr(a string) *node {
	for _, n := range c.nodes {
		if n.addr() == a {
			return n
		}
	}
	return nil
}

// topology is the part of TOPOLOGY the harness reads.
type topology struct {
	NodeID string `json:"node_id"`
	Raft   *struct {
		Term     uint64 `json:"term"`
		LeaderID string `json:"leader_id"`
		IsLeader bool   `json:"is_leader"`
	} `json:"raft"`
}

// command sends one text command on a fresh connection with a deadline.
func command(addr, line string, timeout time.Duration) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte(line + "\n")); err != nil {
		return "", err
	}
	s, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(s, "\r\n"), nil
}

func queryTopology(addr string) (topology, error) {
	var tp topology
	line, err := command(addr, "TOPOLOGY", 700*time.Millisecond)
	if err != nil {
		return tp, err
	}
	if err := json.Unmarshal([]byte(line), &tp); err != nil {
		return tp, fmt.Errorf("bad TOPOLOGY %q: %w", line, err)
	}
	return tp, nil
}

// leader returns the node that reports itself leader with a majority of the
// reachable nodes agreeing, or nil.
func (c *cluster) leader() *node {
	votes := map[string]int{}
	var self = map[string]*node{}
	for _, n := range c.nodes {
		if !n.isUp() {
			continue
		}
		tp, err := queryTopology(n.addr())
		if err != nil || tp.Raft == nil {
			continue
		}
		if tp.Raft.LeaderID != "" {
			votes[tp.Raft.LeaderID]++
		}
		if tp.Raft.IsLeader {
			self[tp.Raft.LeaderID] = n
		}
	}
	for id, n := range self {
		if votes[id] >= len(c.nodes)/2+1 {
			return n
		}
	}
	return nil
}

func (c *cluster) waitLeader(timeout time.Duration) (*node, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != nil {
			return l, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, errors.New("no leader agreed by a majority")
}
