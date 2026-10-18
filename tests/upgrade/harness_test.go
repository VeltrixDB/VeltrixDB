package upgrade_test

// harness_test.go — process harness for the rolling-upgrade tests: building
// the old (v1.16) and new server binaries, starting / stopping / swapping
// nodes on a fixed data dir and port block, a small text-protocol client, and
// the checks (key-for-key comparison, the acknowledged-write model, TTLs).
//
// Every test here is skipped unless VELTRIX_UPGRADE_TEST=1. See README.md.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	verOld = "old" // VELTRIX_OLD_BIN or the build of VELTRIX_OLD_REF (default v1.16)
	verNew = "new" // VELTRIX_NEW_BIN or the build of this checkout
)

func requireEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("VELTRIX_UPGRADE_TEST") != "1" {
		t.Skip("upgrade tests run only with VELTRIX_UPGRADE_TEST=1 (they build v1.16 and start real clusters; see tests/upgrade/README.md)")
	}
}

// ── binaries ────────────────────────────────────────────────────────────────

var (
	binOnce          sync.Once
	oldBin, newBin   string
	binErr           error
	workRootOnce     sync.Once
	workRootDir      string
	workRootErr      error
	oldRef           = envOr("VELTRIX_OLD_REF", "v1.16")
	keepWorkOnPassed = os.Getenv("VELTRIX_UPGRADE_KEEP") == "1"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

// workRoot is the directory for binaries, data dirs and logs:
// VELTRIX_UPGRADE_WORKDIR, or a new temp dir.
func workRoot(t *testing.T) string {
	t.Helper()
	workRootOnce.Do(func() {
		if d := os.Getenv("VELTRIX_UPGRADE_WORKDIR"); d != "" {
			workRootErr = os.MkdirAll(d, 0o755)
			workRootDir = d
			return
		}
		workRootDir, workRootErr = os.MkdirTemp("", "veltrix-upgrade-")
	})
	if workRootErr != nil {
		t.Fatalf("work dir: %v", workRootErr)
	}
	return workRootDir
}

// binaries returns the old and the new server binary, building them once.
func binaries(t *testing.T) (string, string) {
	t.Helper()
	root := moduleRoot(t)
	work := workRoot(t)
	binOnce.Do(func() {
		binDir := filepath.Join(work, "bin")
		if err := os.MkdirAll(binDir, 0o755); err != nil {
			binErr = err
			return
		}
		oldBin = os.Getenv("VELTRIX_OLD_BIN")
		if oldBin == "" {
			src := filepath.Join(work, "src-"+oldRef)
			_ = os.RemoveAll(src)
			if err := os.MkdirAll(src, 0o755); err != nil {
				binErr = err
				return
			}
			// git archive only reads the repository.
			sh := exec.Command("sh", "-c", fmt.Sprintf("git -C %q archive %q | tar -x -C %q", root, oldRef, src))
			if out, err := sh.CombinedOutput(); err != nil {
				binErr = fmt.Errorf("git archive %s: %v\n%s", oldRef, err, out)
				return
			}
			oldBin = filepath.Join(binDir, "veltrix-"+oldRef)
			if err := goBuild(src, oldBin); err != nil {
				binErr = fmt.Errorf("build %s: %w", oldRef, err)
				return
			}
		}
		newBin = os.Getenv("VELTRIX_NEW_BIN")
		if newBin == "" {
			newBin = filepath.Join(binDir, "veltrix-new")
			if err := goBuild(root, newBin); err != nil {
				binErr = fmt.Errorf("build current checkout: %w", err)
				return
			}
		}
	})
	if binErr != nil {
		t.Fatalf("binaries: %v", binErr)
	}
	return oldBin, newBin
}

func goBuild(dir, out string) error {
	c := exec.Command("go", "build", "-o", out, "./cmd/server")
	c.Dir = dir
	b, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v\n%s", err, b)
	}
	return nil
}

// ── ports ───────────────────────────────────────────────────────────────────

var (
	portMu   sync.Mutex
	usedPort = map[int]bool{}
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// reserveBlock returns a client port P with P..P+5 free (client, replication,
// raft, gossip, metrics, transfer). The block is kept for the node's whole
// life, across restarts and binary swaps.
func reserveBlock(t *testing.T) int {
	t.Helper()
	portMu.Lock()
	defer portMu.Unlock()
	for tries := 0; tries < 500; tries++ {
		base := freePort(t)
		if base+5 > 65535 {
			continue
		}
		ok := true
		for off := 0; off <= 5 && ok; off++ {
			if usedPort[base+off] {
				ok = false
			}
		}
		for off := 1; off <= 5 && ok; off++ {
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
			usedPort[base+off] = true
		}
		return base
	}
	t.Fatal("no free 6-port block")
	return 0
}

// ── nodes ───────────────────────────────────────────────────────────────────

type node struct {
	t       *testing.T
	id      string
	port    int
	dataDir string
	logDir  string
	args    []string // mode flags (everything but addr / metrics / data / flush windows)
	env     []string

	mu      sync.Mutex
	ver     string
	bin     string
	cmd     *exec.Cmd
	exited  chan struct{}
	exitErr error
	inc     int
	logs    []string // log file of every incarnation, oldest first
}

func (n *node) addr() string { return fmt.Sprintf("127.0.0.1:%d", n.port) }

func newNode(t *testing.T, dir, id string, args []string, env ...string) *node {
	t.Helper()
	n := &node{
		t:       t,
		id:      id,
		port:    reserveBlock(t),
		dataDir: filepath.Join(dir, "data-"+id),
		logDir:  filepath.Join(dir, "logs"),
		args:    args,
		env:     env,
	}
	if err := os.MkdirAll(n.logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.kill() })
	return n
}

// start launches ver ("old"/"new") on the node's data dir and waits until the
// client port accepts connections.
func (n *node) start(ver string) {
	n.t.Helper()
	if err := n.tryStart(ver, 30*time.Second); err != nil {
		n.t.Fatalf("%v", err)
	}
}

func (n *node) tryStart(ver string, readyTimeout time.Duration) error {
	oldB, newB := binaries(n.t)
	bin := oldB
	if ver == verNew {
		bin = newB
	}
	n.mu.Lock()
	if n.cmd != nil {
		n.mu.Unlock()
		return fmt.Errorf("node %s already running", n.id)
	}
	n.inc++
	logPath := filepath.Join(n.logDir, fmt.Sprintf("%s.%02d.%s.log", n.id, n.inc, ver))
	n.logs = append(n.logs, logPath)
	n.mu.Unlock()

	if err := os.MkdirAll(n.dataDir, 0o755); err != nil {
		return err
	}
	lf, err := os.Create(logPath)
	if err != nil {
		return err
	}
	flags := []string{
		"-addr", n.addr(),
		"-metrics-addr", fmt.Sprintf("127.0.0.1:%d", n.port+4),
		"-data", n.dataDir,
		"-wal-flush-window-ms", "1",
		"-vlog-flush-window-ms", "1",
	}
	flags = append(flags, n.args...)
	cmd := exec.Command(bin, flags...)
	cmd.Stdout, cmd.Stderr = lf, lf
	cmd.Env = append(os.Environ(), n.env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	fmt.Fprintf(lf, "### harness: %s %s\n", bin, strings.Join(flags, " "))
	if err := cmd.Start(); err != nil {
		lf.Close()
		return fmt.Errorf("start %s: %w", n.id, err)
	}
	exited := make(chan struct{})
	n.mu.Lock()
	n.ver, n.bin, n.cmd, n.exited, n.exitErr = ver, bin, cmd, exited, nil
	n.mu.Unlock()
	go func() {
		err := cmd.Wait()
		lf.Close()
		n.mu.Lock()
		n.exitErr = err
		n.mu.Unlock()
		close(exited)
	}()
	deadline := time.Now().Add(readyTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			n.mu.Lock()
			n.cmd = nil
			e := n.exitErr
			n.mu.Unlock()
			return fmt.Errorf("node %s (%s) exited during startup: %v\n%s", n.id, ver, e, tailFile(logPath, 25))
		default:
		}
		c, err := net.DialTimeout("tcp", n.addr(), 200*time.Millisecond)
		if err == nil {
			c.Close()
			n.t.Logf("node %s up: %s on %s (log %s)", n.id, ver, n.addr(), logPath)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	n.kill()
	return fmt.Errorf("node %s (%s) not ready after %s\n%s", n.id, ver, readyTimeout, tailFile(logPath, 25))
}

// stop sends SIGTERM (clean shutdown: WAL checkpoint) and waits for exit.
func (n *node) stop() {
	n.t.Helper()
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	n.mu.Unlock()
	if cmd == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(60 * time.Second):
		n.t.Errorf("node %s did not exit 60s after SIGTERM — killing it", n.id)
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-exited
	}
	n.mu.Lock()
	n.cmd = nil
	n.mu.Unlock()
	n.t.Logf("node %s stopped (SIGTERM, %s)", n.id, n.ver)
}

// kill is a crash: SIGKILL, no clean shutdown.
func (n *node) kill() {
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	n.mu.Unlock()
	if cmd == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	<-exited
	n.mu.Lock()
	n.cmd = nil
	n.mu.Unlock()
}

func (n *node) running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		return false
	}
	select {
	case <-n.exited:
		return false
	default:
		return true
	}
}

// swap is one rolling-upgrade step: clean stop, start ver on the same --data.
func (n *node) swap(ver string) {
	n.t.Helper()
	n.stop()
	n.start(ver)
}

func (n *node) wipe() {
	n.t.Helper()
	if n.running() {
		n.t.Fatalf("wipe %s while running", n.id)
	}
	if err := os.RemoveAll(n.dataDir); err != nil {
		n.t.Fatal(err)
	}
}

// grepLogs returns the lines of every incarnation's log containing substr.
func (n *node) grepLogs(substr string) []string {
	n.mu.Lock()
	logs := append([]string(nil), n.logs...)
	n.mu.Unlock()
	var out []string
	for _, p := range logs {
		b, _ := os.ReadFile(p)
		for _, l := range strings.Split(string(b), "\n") {
			if strings.Contains(l, substr) {
				out = append(out, filepath.Base(p)+": "+l)
			}
		}
	}
	return out
}

func (n *node) currentLog() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.logs) == 0 {
		return ""
	}
	return n.logs[len(n.logs)-1]
}

func tailFile(path string, lines int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "(no log: " + err.Error() + ")"
	}
	ls := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(ls) > lines {
		ls = ls[len(ls)-lines:]
	}
	return "  | " + strings.Join(ls, "\n  | ")
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// testDir is a per-test directory under the work root; it is removed when
// the test passes unless VELTRIX_UPGRADE_KEEP=1.
func testDir(t *testing.T) string {
	t.Helper()
	d := filepath.Join(workRoot(t), strings.ReplaceAll(t.Name(), "/", "_"))
	_ = os.RemoveAll(d)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() || keepWorkOnPassed {
			t.Logf("data dirs and node logs kept in %s", d)
			return
		}
		_ = os.RemoveAll(d)
	})
	return d
}

// ── text client ─────────────────────────────────────────────────────────────

type conn struct {
	c net.Conn
	r *bufio.Reader
}

func dial(addr string) (*conn, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &conn{c: c, r: bufio.NewReaderSize(c, 64<<10)}, nil
}

func (c *conn) close() { _ = c.c.Close() }

func (c *conn) do(cmd string) (string, error) {
	_ = c.c.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.WriteString(c.c, cmd+"\n"); err != nil {
		return "", err
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// one sends a single command on a fresh connection.
func one(addr, cmd string) (string, error) {
	c, err := dial(addr)
	if err != nil {
		return "", err
	}
	defer c.close()
	return c.do(cmd)
}

// scanAll returns every live key → value of one node (SCANCUR, local read).
func scanAll(addr string) (map[string]string, error) {
	c, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer c.close()
	out := map[string]string{}
	cursor := "-"
	for {
		_ = c.c.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := io.WriteString(c.c, "SCANCUR "+cursor+" 1000\n"); err != nil {
			return nil, err
		}
		for {
			line, err := c.r.ReadString('\n')
			if err != nil {
				return nil, err
			}
			line = strings.TrimRight(line, "\r\n")
			if strings.HasPrefix(line, "CURSOR ") {
				cursor = strings.TrimPrefix(line, "CURSOR ")
				break
			}
			if strings.HasPrefix(line, "ERR ") {
				return nil, errors.New(line)
			}
			sp := strings.IndexByte(line, ' ')
			if sp < 0 {
				out[line] = ""
			} else {
				out[line[:sp]] = line[sp+1:]
			}
		}
		if cursor == "-" {
			return out, nil
		}
	}
}

// ── raft helpers ────────────────────────────────────────────────────────────

type topo struct {
	NodeID string `json:"node_id"`
	Mode   string `json:"mode"`
	Raft   *struct {
		Role     string `json:"role"`
		Term     uint64 `json:"term"`
		LeaderID string `json:"leader_id"`
		IsLeader bool   `json:"is_leader"`
	} `json:"raft"`
	Replication []struct {
		NodeID string `json:"node_id"`
		State  string `json:"state"`
	} `json:"replication"`
}

func topology(addr string) (topo, error) {
	var tp topo
	line, err := one(addr, "TOPOLOGY")
	if err != nil {
		return tp, err
	}
	if err := json.Unmarshal([]byte(line), &tp); err != nil {
		return tp, fmt.Errorf("topology %q: %w", line, err)
	}
	return tp, nil
}

// waitLeader waits until a running node reports itself leader and a majority
// of the running nodes agree.
func waitLeader(t *testing.T, nodes []*node, timeout time.Duration) *node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var lead *node
		for _, n := range nodes {
			if !n.running() {
				continue
			}
			tp, err := topology(n.addr())
			if err == nil && tp.Raft != nil && tp.Raft.IsLeader {
				lead = n
			}
		}
		if lead != nil {
			agree := 0
			for _, n := range nodes {
				if !n.running() {
					continue
				}
				if tp, err := topology(n.addr()); err == nil && tp.Raft != nil && tp.Raft.LeaderID == lead.id {
					agree++
				}
			}
			if agree >= len(nodes)/2+1 {
				return lead
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no raft leader within %s", timeout)
	return nil
}

func byAddr(nodes []*node, addr string) *node {
	for _, n := range nodes {
		if n.addr() == addr {
			return n
		}
	}
	return nil
}

// outcome of one write.
type outcome int

const (
	acked   outcome = iota // the server acknowledged it
	unknown                // sent, but no acknowledgement: may or may not apply
)

// raftClient sends writes to the leader, following MOVED redirects. A
// redirect or a failed dial means the write was not submitted, so it is
// retried; anything else that is not a reply is "unknown".
type raftClient struct {
	nodes []*node
	cur   *node
	c     *conn
}

func (rc *raftClient) close() {
	if rc.c != nil {
		rc.c.close()
		rc.c = nil
	}
}

func (rc *raftClient) next() {
	rc.close()
	for i, n := range rc.nodes {
		if n == rc.cur {
			rc.cur = rc.nodes[(i+1)%len(rc.nodes)]
			return
		}
	}
	rc.cur = rc.nodes[0]
}

// write returns the server's reply and whether it was acknowledged.
// isOK decides what an acknowledgement looks like for this command.
func (rc *raftClient) write(cmd string, isOK func(string) bool, timeout time.Duration) (string, outcome) {
	deadline := time.Now().Add(timeout)
	if rc.cur == nil {
		rc.cur = rc.nodes[0]
	}
	for time.Now().Before(deadline) {
		if rc.c == nil {
			c, err := dial(rc.cur.addr())
			if err != nil {
				rc.next()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			rc.c = c
		}
		resp, err := rc.c.do(cmd)
		if err != nil {
			rc.close()
			return err.Error(), unknown
		}
		if strings.Contains(resp, "MOVED") {
			f := strings.Fields(resp)
			moved := ""
			for i := range f {
				if f[i] == "MOVED" && i+1 < len(f) {
					moved = f[i+1]
				}
			}
			if n := byAddr(rc.nodes, moved); n != nil && n != rc.cur {
				rc.close()
				rc.cur = n
			} else {
				rc.next()
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
		if isOK(resp) {
			return resp, acked
		}
		return resp, unknown
	}
	return "timeout: no leader accepted the write", unknown
}

func isOKReply(s string) bool { return s == "OK" }
func isNumReply(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

// ── model of acknowledged writes ─────────────────────────────────────────────

// model is what the cluster must hold: acknowledged values, acknowledged
// deletes, counter bounds and TTL probes. Keys whose last write had an
// unknown outcome are dropped from the checks.
type model struct {
	mu      sync.Mutex
	vals    map[string]string
	gone    map[string]bool
	ctrAck  map[string]int64
	ctrUnk  map[string]int64
	ttlKeys map[string]bool // "key\x01field" written with a long TTL: HTTL must stay > 0
	expKeys map[string]time.Time
	// immortalOK: the path under test is documented to drop TTLs, so a
	// short-TTL key that outlived its deadline is not reported.
	immortalOK bool
}

func newModel() *model {
	return &model{
		vals: map[string]string{}, gone: map[string]bool{},
		ctrAck: map[string]int64{}, ctrUnk: map[string]int64{},
		ttlKeys: map[string]bool{}, expKeys: map[string]time.Time{},
	}
}

func (m *model) put(k, v string, o outcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.gone, k)
	if o == acked {
		m.vals[k] = v
	} else {
		delete(m.vals, k)
	}
}

func (m *model) del(k string, o outcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.vals, k)
	if o == acked {
		m.gone[k] = true
	} else {
		delete(m.gone, k)
	}
}

func (m *model) incr(k string, d int64, o outcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o == acked {
		m.ctrAck[k] += d
	} else {
		m.ctrUnk[k] += d
	}
}

// check compares one node's keyspace with the model and returns the problems.
func (m *model) check(name string, kv map[string]string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var bad []string
	add := func(f string, a ...interface{}) {
		if len(bad) < 40 {
			bad = append(bad, name+": "+fmt.Sprintf(f, a...))
		} else if len(bad) == 40 {
			bad = append(bad, name+": …")
		}
	}
	missing, wrong := 0, 0
	now := time.Now()
	for k, v := range m.vals {
		if exp, ok := m.expKeys[k]; ok && now.After(exp.Add(-2*time.Second)) {
			continue // expired (or about to): checked below
		}
		got, ok := kv[k]
		if !ok {
			missing++
			add("acknowledged write lost: %q (want %q)", k, v)
		} else if got != v {
			wrong++
			add("acknowledged write has wrong value: %q = %q, want %q", k, got, v)
		}
	}
	resurrected := 0
	for k := range m.gone {
		if v, ok := kv[k]; ok {
			resurrected++
			add("acknowledged delete undone: %q = %q", k, v)
		}
	}
	for k, a := range m.ctrAck {
		u := m.ctrUnk[k]
		got, err := strconv.ParseInt(kv[k], 10, 64)
		if err != nil || got < a || got > a+u {
			add("counter %q = %q, want %d (acked) .. %d (acked+unknown)", k, kv[k], a, a+u)
		}
	}
	for k, exp := range m.expKeys {
		if !m.immortalOK && now.After(exp.Add(2*time.Second)) {
			if v, ok := kv[k]; ok {
				add("key %q should have expired at %s but is present (%q) — its TTL was lost", k, exp.Format(time.RFC3339), v)
			}
		}
	}
	if missing+wrong+resurrected > 0 {
		bad = append(bad, fmt.Sprintf("%s: totals: %d acknowledged writes lost, %d wrong, %d deletes undone (of %d values / %d deletes)",
			name, missing, wrong, resurrected, len(m.vals), len(m.gone)))
	}
	return bad
}

// ttlProblems runs HTTL on every TTL probe key: -1 means the TTL was lost
// (immortal), -2 means the key is gone.
func (m *model) ttlProblems(name, addr string) []string {
	m.mu.Lock()
	keys := make([]string, 0, len(m.ttlKeys))
	for k := range m.ttlKeys {
		keys = append(keys, k)
	}
	m.mu.Unlock()
	sort.Strings(keys)
	c, err := dial(addr)
	if err != nil {
		return []string{name + ": dial: " + err.Error()}
	}
	defer c.close()
	var bad []string
	immortal, gone := 0, 0
	for _, k := range keys {
		i := strings.IndexByte(k, '\x01')
		resp, err := c.do("HTTL " + k[:i] + " " + k[i+1:])
		if err != nil {
			return append(bad, name+": HTTL: "+err.Error())
		}
		switch n, _ := strconv.Atoi(resp); {
		case resp == "-1":
			immortal++
			if immortal <= 5 {
				bad = append(bad, fmt.Sprintf("%s: TTL lost: HTTL %q = -1 (immortal), want > 0", name, strings.Replace(k, "\x01", " ", 1)))
			}
		case resp == "-2" || n <= 0:
			gone++
			if gone <= 5 {
				bad = append(bad, fmt.Sprintf("%s: TTL key gone: HTTL %q = %s", name, strings.Replace(k, "\x01", " ", 1), resp))
			}
		}
	}
	if immortal+gone > 0 {
		bad = append(bad, fmt.Sprintf("%s: %d of %d TTL keys immortal, %d gone", name, immortal, len(keys), gone))
	}
	return bad
}

// ── keyspace comparison ─────────────────────────────────────────────────────

func diffKV(an string, a map[string]string, bn string, b map[string]string, max int) []string {
	var out []string
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	n := 0
	for _, k := range sorted {
		av, aok := a[k]
		bv, bok := b[k]
		if aok == bok && av == bv {
			continue
		}
		n++
		if len(out) < max {
			show := func(v string, ok bool) string {
				if !ok {
					return "<absent>"
				}
				return strconv.Quote(v)
			}
			out = append(out, fmt.Sprintf("%q: %s=%s %s=%s", k, an, show(av, aok), bn, show(bv, bok)))
		}
	}
	if n > 0 {
		out = append(out, fmt.Sprintf("%d keys differ between %s (%d keys) and %s (%d keys)", n, an, len(a), bn, len(b)))
	}
	return out
}

// waitConverged polls until every node returns the same keyspace; it returns
// the keyspaces and, on timeout, the differences.
func waitConverged(nodes []*node, timeout time.Duration) (map[string]map[string]string, []string) {
	deadline := time.Now().Add(timeout)
	for {
		all := map[string]map[string]string{}
		var errs []string
		for _, n := range nodes {
			kv, err := scanAll(n.addr())
			if err != nil {
				errs = append(errs, n.id+": scan: "+err.Error())
				continue
			}
			all[n.id] = kv
		}
		var diffs []string
		if len(errs) == 0 {
			for _, n := range nodes[1:] {
				diffs = append(diffs, diffKV(nodes[0].id, all[nodes[0].id], n.id, all[n.id], 15)...)
			}
			if len(diffs) == 0 {
				return all, nil
			}
		}
		if time.Now().After(deadline) {
			return all, append(errs, diffs...)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// waitKey polls a node's local GET until it returns want.
func waitKey(addr, key, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		got, err := one(addr, "GET "+key)
		if err == nil && got == want {
			return nil
		}
		if err != nil {
			last = err.Error()
		} else {
			last = got
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s: GET %s = %q after %s, want %q", addr, key, last, timeout, want)
}

func reportAll(t *testing.T, problems []string) {
	t.Helper()
	for _, p := range problems {
		t.Error(p)
	}
}

func joinLines(ls []string) string {
	var b bytes.Buffer
	for _, l := range ls {
		b.WriteString("    ")
		b.WriteString(l)
		b.WriteByte('\n')
	}
	return b.String()
}
