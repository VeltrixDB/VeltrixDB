package integration_test

// membership_test.go — online cluster membership on real processes
// (cmd/server/membership*.go, consensus/membership.go):
//
//   - raft: 3 nodes under write load; an EMPTY 4th node joins with --join
//     (catches up through chunked InstallSnapshot, promoted to voter); a 5th
//     is added through the admin API on a follower (forwarded to the leader),
//     concurrently twice; a follower and then the leader are removed.  No
//     acknowledged write is lost, every member serves the same keyspace, the
//     membership survives a restart of every node (with the original --peers),
//     a restarted removed node does not disrupt the cluster (no term bump),
//     and changes without a quorum are refused.
//   - replicated: a node joins and receives every key; a node is drained and
//     removed without losing a key; membership survives restarts; a restarted
//     removed node refuses writes.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── process helper ──────────────────────────────────────────────────────────

type mnode struct {
	t       *testing.T
	id      string
	port    int
	dataDir string
	args    []string // everything except -addr / -metrics-addr / -data
	cmd     *exec.Cmd
	exited  chan error
}

func (n *mnode) addr() string    { return fmt.Sprintf("127.0.0.1:%d", n.port) }
func (n *mnode) metrics() string { return fmt.Sprintf("127.0.0.1:%d", n.port+4) }

func (n *mnode) start() {
	t := n.t
	t.Helper()
	flags := []string{
		"-addr", n.addr(), "-metrics-addr", n.metrics(), "-data", n.dataDir,
		"-wal-flush-window-ms", "1", "-vlog-flush-window-ms", "1",
	}
	flags = append(flags, n.args...)
	bin := serverBinary()
	if bin == "" {
		bin = builtBinary(t)
	}
	cmd := exec.Command(bin, flags...)
	cmd.Dir = moduleRoot(t)
	logf, err := os.OpenFile(filepath.Join(n.dataDir, "..", n.id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", n.id, err)
	}
	n.cmd = cmd
	n.exited = make(chan error, 1)
	go func() { n.exited <- cmd.Wait(); logf.Close() }()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", n.addr(), 200*time.Millisecond); err == nil {
			c.Close()
			return
		}
		select {
		case err := <-n.exited:
			t.Fatalf("%s exited at startup: %v (log %s)", n.id, err, filepath.Join(n.dataDir, "..", n.id+".log"))
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("%s not ready", n.id)
}

func (n *mnode) kill() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		<-n.exited
	}
	n.cmd = nil
}

func (n *mnode) running() bool { return n.cmd != nil }

func newMNodes(t *testing.T, ids []string, used map[int]bool) []*mnode {
	t.Helper()
	root, err := os.MkdirTemp("", "veltrix-member-")
	if err != nil {
		t.Fatal(err)
	}
	var out []*mnode
	for _, id := range ids {
		dir := filepath.Join(root, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		out = append(out, &mnode{t: t, id: id, port: reserveBlock(t, used), dataDir: dir})
	}
	t.Cleanup(func() {
		for _, n := range out {
			n.kill()
		}
		if t.Failed() {
			t.Logf("node logs kept in %s", root)
			return
		}
		os.RemoveAll(root)
	})
	return out
}

func secretFile(t *testing.T) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(f, []byte("membership-test-secret-0123456789abcdef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// ── admin API ───────────────────────────────────────────────────────────────

type membersDoc struct {
	Mode        string `json:"mode"`
	Version     uint64 `json:"version"`
	Committed   bool   `json:"committed"`
	Leader      string `json:"leader"`
	Coordinator string `json:"coordinator"`
	SelfStatus  string `json:"self_status"`
	Members     []struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
		Role string `json:"role"`
	} `json:"members"`
}

func (m membersDoc) roles() map[string]string {
	out := map[string]string{}
	for _, x := range m.Members {
		out[x.ID] = x.Role
	}
	return out
}

func adminDo(method, metricsAddr, path, body string) (int, string, error) {
	var rb io.Reader
	if body != "" {
		rb = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+metricsAddr+path, rb)
	if err != nil {
		return 0, "", err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 3 * time.Minute}).Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b)), nil
}

func getMembers(n *mnode) (membersDoc, error) {
	code, body, err := adminDo(http.MethodGet, n.metrics(), "/admin/cluster/members", "")
	if err != nil {
		return membersDoc{}, err
	}
	if code != 200 {
		return membersDoc{}, fmt.Errorf("HTTP %d: %s", code, body)
	}
	var m membersDoc
	return m, json.Unmarshal([]byte(body), &m)
}

func addMember(n, newNode *mnode) (int, string) {
	code, body, err := adminDo(http.MethodPost, n.metrics(), "/admin/cluster/members",
		fmt.Sprintf(`{"id":%q,"addr":%q,"wait_ms":60000}`, newNode.id, newNode.addr()))
	if err != nil {
		return 0, err.Error()
	}
	return code, body
}

func removeMember(n *mnode, id string, query string) (int, string) {
	code, body, err := adminDo(http.MethodDelete, n.metrics(), "/admin/cluster/members/"+id+query, "")
	if err != nil {
		return 0, err.Error()
	}
	return code, body
}

// waitMembers waits until node n reports exactly want (id → role), committed.
func waitMembers(t *testing.T, n *mnode, want map[string]string, timeout time.Duration) membersDoc {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last membersDoc
	var lastErr error
	for time.Now().Before(deadline) {
		m, err := getMembers(n)
		last, lastErr = m, err
		if err == nil && m.Committed && sameRoles(m.roles(), want) {
			return m
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s: membership never became %v (last %+v, err %v)", n.id, want, last, lastErr)
	return last
}

func sameRoles(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func nodeByID(nodes []*mnode, id string) *mnode {
	for _, n := range nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

func liveAddrs(nodes []*mnode) []string {
	var out []string
	for _, n := range nodes {
		if n.running() {
			out = append(out, n.addr())
		}
	}
	return out
}

// ── load ────────────────────────────────────────────────────────────────────

// loadGen writes unique keys through whichever node accepts them (following
// MOVED), records every acknowledged write, and never fails the test itself.
type loadGen struct {
	acked  sync.Map // key → value
	n      atomic.Int64
	errs   atomic.Int64
	stop   chan struct{}
	wg     sync.WaitGroup
	addrMu sync.Mutex
	addrs  []string
}

func (g *loadGen) setAddrs(a []string) {
	g.addrMu.Lock()
	g.addrs = append([]string(nil), a...)
	g.addrMu.Unlock()
}

func (g *loadGen) pick(i int) string {
	g.addrMu.Lock()
	defer g.addrMu.Unlock()
	if len(g.addrs) == 0 {
		return ""
	}
	return g.addrs[i%len(g.addrs)]
}

func startLoad(addrs []string, workers int) *loadGen {
	g := &loadGen{stop: make(chan struct{})}
	g.setAddrs(addrs)
	for w := 0; w < workers; w++ {
		g.wg.Add(1)
		go func(w int) {
			defer g.wg.Done()
			var conn net.Conn
			var rd *bufio.Reader
			target := g.pick(w)
			rot := w
			closeConn := func() {
				if conn != nil {
					conn.Close()
					conn = nil
				}
			}
			defer closeConn()
			for i := 0; ; i++ {
				select {
				case <-g.stop:
					return
				default:
				}
				if conn == nil {
					c, err := net.DialTimeout("tcp", target, time.Second)
					if err != nil {
						g.errs.Add(1)
						rot++
						target = g.pick(rot)
						time.Sleep(50 * time.Millisecond)
						continue
					}
					conn, rd = c, bufio.NewReader(c)
				}
				key := fmt.Sprintf("load-%d-%06d", w, i)
				val := fmt.Sprintf("v%d", i)
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				if _, err := fmt.Fprintf(conn, "PUT %s %s\n", key, val); err != nil {
					closeConn()
					continue
				}
				line, err := rd.ReadString('\n')
				if err != nil {
					closeConn()
					rot++
					target = g.pick(rot)
					continue
				}
				line = strings.TrimSpace(line)
				switch {
				case line == "OK":
					g.acked.Store(key, val)
					g.n.Add(1)
				case strings.Contains(line, "MOVED "):
					f := strings.Fields(line[strings.Index(line, "MOVED ")+6:])
					closeConn()
					if len(f) > 0 && f[0] != "-" {
						target = f[0]
					} else {
						rot++
						target = g.pick(rot)
						time.Sleep(50 * time.Millisecond)
					}
				default:
					g.errs.Add(1)
					closeConn()
					rot++
					target = g.pick(rot)
					time.Sleep(20 * time.Millisecond)
				}
			}
		}(w)
	}
	return g
}

func (g *loadGen) halt() {
	close(g.stop)
	g.wg.Wait()
}

// checkAcked fails unless every acknowledged write is in ks.
func checkAcked(t *testing.T, who string, g *loadGen, ks map[string]string) {
	t.Helper()
	missing := 0
	var sample []string
	g.acked.Range(func(k, v interface{}) bool {
		if ks[k.(string)] != v.(string) {
			missing++
			if len(sample) < 5 {
				sample = append(sample, k.(string))
			}
		}
		return true
	})
	if missing > 0 {
		t.Fatalf("%s: %d acknowledged writes missing or wrong (of %d), e.g. %v", who, missing, g.n.Load(), sample)
	}
}

// waitConvergedAddrs waits until every address serves the same keyspace.
func waitConvergedAddrs(t *testing.T, addrs []string, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		want, err := keyspace(addrs[0])
		if err == nil {
			ok := true
			for _, a := range addrs[1:] {
				got, err := keyspace(a)
				if err != nil {
					ok, last = false, err.Error()
					break
				}
				if d := diffKeyspaces(want, got); len(d) > 0 {
					ok, last = false, a+" vs "+addrs[0]+": "+strings.Join(d, ", ")
					break
				}
			}
			if ok {
				return want
			}
		} else {
			last = err.Error()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("keyspaces did not converge within %v: %s", timeout, last)
	return nil
}

func raftTerm(t *testing.T, addr string) (uint64, string) {
	tp, err := queryTopo(t, addr)
	if err != nil || tp.Raft == nil {
		return 0, ""
	}
	return tp.Raft.Term, tp.Raft.LeaderID
}

// ── raft ────────────────────────────────────────────────────────────────────

func TestRaftMembership_OnlineGrowShrink(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	secret := secretFile(t)
	used := map[int]bool{}
	nodes := newMNodes(t, []string{"m1", "m2", "m3", "m4", "m5"}, used)
	common := []string{"-cluster-secret-file", secret, "-raft-snapshot-threshold", "256", "-raft-snapshot-chunk-kb", "16"}
	var ports []int
	for _, n := range nodes[:3] {
		ports = append(ports, n.port)
	}
	peers3 := peersFlag([]string{"m1", "m2", "m3"}, ports)
	for _, n := range nodes[:3] {
		n.args = raftArgs(t, n.id, peers3, common...)
		n.start()
	}
	_, leaderAddr := waitForRaftLeader(t, liveAddrs(nodes), 30*time.Second)
	t.Logf("initial leader at %s", leaderAddr)

	g := startLoad(liveAddrs(nodes), 6)
	defer func() {
		select {
		case <-g.stop:
		default:
			g.halt()
		}
	}()
	time.Sleep(3 * time.Second) // enough writes that the leader compacted (threshold 256)

	// ── 4th node: empty, joins by itself under load (--join).
	n4 := nodes[3]
	n4.args = raftArgs(t, n4.id, "", append([]string{"-join", nodes[0].addr()}, common...)...)
	n4.start()
	all4 := map[string]string{"m1": "voter", "m2": "voter", "m3": "voter", "m4": "voter"}
	waitMembers(t, nodes[0], all4, 90*time.Second)
	waitMembers(t, n4, all4, 30*time.Second)
	g.setAddrs(liveAddrs(nodes))
	t.Logf("m4 joined and was promoted; %d writes acknowledged so far", g.n.Load())
	if v := metricValue(t, n4.metrics(), "veltrixdb_raft_snapshot_installs_total"); v < 1 {
		t.Fatalf("m4 caught up without an InstallSnapshot (installs=%v) — the test meant to exercise one", v)
	}

	// ── 5th node: added through the admin API of a FOLLOWER (forwarded to
	// the leader), twice at once: the changes serialise.
	n5 := nodes[4]
	n5.args = raftArgs(t, n5.id, "", append([]string{"-join", "127.0.0.1:1"}, common...)...) // seed unreachable: waits for the admin call
	n5.start()
	leaderID, _ := waitForRaftLeader(t, liveAddrs(nodes), 30*time.Second)
	var follower *mnode
	for _, n := range nodes[:4] {
		if n.id != leaderID {
			follower = n
			break
		}
	}
	type res struct {
		code int
		body string
	}
	results := make(chan res, 2)
	for _, via := range []*mnode{follower, nodeByID(nodes, leaderID)} {
		go func(via *mnode) {
			c, b := addMember(via, n5)
			results <- res{c, b}
		}(via)
	}
	okCount := 0
	for i := 0; i < 2; i++ {
		r := <-results
		t.Logf("concurrent add m5: HTTP %d %s", r.code, firstLine(r.body))
		switch r.code {
		case 200, 202:
			okCount++
		case 409:
		default:
			t.Fatalf("concurrent add m5: HTTP %d %s", r.code, r.body)
		}
	}
	if okCount == 0 {
		t.Fatal("neither concurrent add of m5 succeeded")
	}
	all5 := map[string]string{"m1": "voter", "m2": "voter", "m3": "voter", "m4": "voter", "m5": "voter"}
	waitMembers(t, nodes[0], all5, 90*time.Second)
	g.setAddrs(liveAddrs(nodes))

	// ── negative cases: duplicate id, unknown id.
	if c, b := addMember(follower, n5); c != http.StatusConflict {
		t.Fatalf("duplicate add: HTTP %d %s, want 409", c, b)
	}
	if c, b := removeMember(follower, "nobody", ""); c != http.StatusNotFound {
		t.Fatalf("remove unknown: HTTP %d %s, want 404", c, b)
	}

	// ── remove a follower.
	leaderID, _ = waitForRaftLeader(t, liveAddrs(nodes), 30*time.Second)
	var victim *mnode
	for _, n := range nodes[:3] {
		if n.id != leaderID {
			victim = n
			break
		}
	}
	if c, b := removeMember(nodeByID(nodes, leaderID), victim.id, ""); c != 200 {
		t.Fatalf("remove follower %s: HTTP %d %s", victim.id, c, b)
	}
	want4 := map[string]string{}
	for k, v := range all5 {
		if k != victim.id {
			want4[k] = v
		}
	}
	waitMembers(t, nodeByID(nodes, leaderID), want4, 30*time.Second)
	t.Logf("removed follower %s", victim.id)

	// ── remove the leader (through a follower: forwarded): it steps down.
	leader := nodeByID(nodes, leaderID)
	var via *mnode
	for id := range want4 {
		if id != leaderID {
			via = nodeByID(nodes, id)
			break
		}
	}
	if c, b := removeMember(via, leaderID, ""); c != 200 {
		t.Fatalf("remove leader %s: HTTP %d %s", leaderID, c, b)
	}
	want3 := map[string]string{}
	for k, v := range want4 {
		if k != leaderID {
			want3[k] = v
		}
	}
	var remaining []*mnode
	for id := range want3 {
		remaining = append(remaining, nodeByID(nodes, id))
	}
	newLeader, _ := waitForRaftLeader(t, liveAddrs(remaining), 30*time.Second)
	if newLeader == leaderID {
		t.Fatalf("removed leader %s still leads", leaderID)
	}
	waitMembers(t, nodeByID(nodes, newLeader), want3, 30*time.Second)
	if tp, err := queryTopo(t, leader.addr()); err == nil && tp.Raft != nil && tp.Raft.IsLeader {
		t.Fatalf("removed leader %s still claims leadership", leaderID)
	}
	g.setAddrs(liveAddrs(remaining))
	time.Sleep(2 * time.Second) // writes on the new leader
	g.halt()
	t.Logf("load: %d writes acknowledged, %d errors/retries", g.n.Load(), g.errs.Load())
	if g.n.Load() < 500 {
		t.Fatalf("only %d writes acknowledged — the cluster did not take load", g.n.Load())
	}

	// ── every acknowledged write on every member; key-for-key equal.
	ks := waitConvergedAddrs(t, liveAddrs(remaining), 60*time.Second)
	checkAcked(t, "remaining members", g, ks)
	t.Logf("%d keys on every remaining member", len(ks))

	// ── restart EVERY node of the new membership with its original flags
	// (m1..m3 still name the old --peers): the persisted configuration wins.
	victim.kill()
	leader.kill()
	for _, n := range remaining {
		n.kill()
	}
	for _, n := range remaining {
		n.start()
	}
	nl, _ := waitForRaftLeader(t, liveAddrs(remaining), 30*time.Second)
	for _, n := range remaining {
		waitMembers(t, n, want3, 30*time.Second)
	}
	ks2 := waitConvergedAddrs(t, liveAddrs(remaining), 60*time.Second)
	checkAcked(t, "after restart", g, ks2)
	t.Logf("after restarting every node: leader %s, membership %v, %d keys", nl, want3, len(ks2))

	// ── a removed node restarted on its old data with its old --peers must
	// not disrupt the cluster: no term bump, same leader.
	termBefore, leaderBefore := raftTerm(t, nodeByID(nodes, nl).addr())
	victim.start()
	time.Sleep(6 * time.Second) // many election timeouts of the removed node
	for _, n := range remaining {
		term, lid := raftTerm(t, n.addr())
		if term != termBefore || lid != leaderBefore {
			t.Fatalf("%s: term/leader changed after a removed node restarted: %d/%s → %d/%s",
				n.id, termBefore, leaderBefore, term, lid)
		}
	}
	if m, err := getMembers(victim); err == nil && m.SelfStatus == "voter" && m.Leader != "" {
		t.Logf("restarted removed node still believes it is a voter (never saw its removal): %+v", m)
	}

	// ── no quorum: with two of three members down, a change is refused.
	var up *mnode
	for _, n := range remaining {
		if n.id == nl {
			up = n
		} else {
			n.kill()
		}
	}
	time.Sleep(3 * time.Second)
	t0 := time.Now()
	c, b := addMember(up, &mnode{id: "m9", port: 1})
	if c != http.StatusServiceUnavailable && c != http.StatusMisdirectedRequest {
		t.Fatalf("add without quorum: HTTP %d %s, want 503", c, b)
	}
	t.Logf("add without quorum: HTTP %d after %v: %s", c, time.Since(t0).Round(time.Millisecond), firstLine(b))
}

// metricValue returns the sum of a Prometheus counter/gauge's samples.
func metricValue(t *testing.T, metricsAddr, name string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatalf("scrape %s: %v", metricsAddr, err)
	}
	defer resp.Body.Close()
	sum := 0.0
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name) || strings.HasPrefix(line, "#") {
			continue
		}
		rest := line[len(name):]
		if rest != "" && rest[0] != ' ' && rest[0] != '{' {
			continue
		}
		f := strings.Fields(line)
		var v float64
		if _, err := fmt.Sscan(f[len(f)-1], &v); err == nil {
			sum += v
		}
	}
	return sum
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

// ── replicated ──────────────────────────────────────────────────────────────

func TestReplicatedMembership_AddRemoveNoKeyLost(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	secret := secretFile(t)
	used := map[int]bool{}
	nodes := newMNodes(t, []string{"r1", "r2", "r3", "r4"}, used)
	var ports []int
	for _, n := range nodes[:3] {
		ports = append(ports, n.port)
	}
	peers3 := peersFlag([]string{"r1", "r2", "r3"}, ports)
	repl := func(id, peers string, extra ...string) []string {
		return append([]string{"-mode", "replicated", "-node-id", id, "-peers", peers,
			"-consistency", "quorum", "-cluster-secret-file", secret}, extra...)
	}
	for _, n := range nodes[:3] {
		n.args = repl(n.id, peers3)
		n.start()
	}
	time.Sleep(2 * time.Second) // gossip / replication connections

	// Writes through r1 and r2 (each originates its own keys).
	g := startLoad([]string{nodes[0].addr(), nodes[1].addr()}, 4)
	time.Sleep(3 * time.Second)

	// ── r4 joins under load and must receive every key.
	n4 := nodes[3]
	n4.args = repl(n4.id, "", "-join", nodes[2].addr())
	n4.start()
	all4 := map[string]string{"r1": "member", "r2": "member", "r3": "member", "r4": "member"}
	for _, n := range nodes {
		waitMembers(t, n, all4, 60*time.Second)
	}
	t.Logf("r4 joined; %d writes acknowledged so far", g.n.Load())
	time.Sleep(3 * time.Second)

	// ── negative cases.
	if c, b := addMember(nodes[1], n4); c != http.StatusConflict {
		t.Fatalf("duplicate add: HTTP %d %s, want 409", c, b)
	}
	if c, b := removeMember(nodes[1], "nobody", ""); c != http.StatusNotFound {
		t.Fatalf("remove unknown: HTTP %d %s, want 404", c, b)
	}

	// ── remove r2 (a writer target) through r3's admin API: drained first.
	g.setAddrs([]string{nodes[0].addr(), nodes[1].addr(), nodes[3].addr()})
	c, b := removeMember(nodes[2], "r2", "")
	if c != 200 {
		t.Fatalf("remove r2: HTTP %d %s", c, b)
	}
	want3 := map[string]string{"r1": "member", "r3": "member", "r4": "member"}
	for _, id := range []string{"r1", "r3", "r4"} {
		waitMembers(t, nodeByID(nodes, id), want3, 30*time.Second)
	}
	if m, err := getMembers(nodes[1]); err != nil || m.SelfStatus != "removed" {
		t.Fatalf("r2 after removal: %+v %v", m, err)
	}
	g.setAddrs([]string{nodes[0].addr(), nodes[3].addr()})
	time.Sleep(2 * time.Second)
	g.halt()
	t.Logf("load: %d acknowledged, %d errors/retries", g.n.Load(), g.errs.Load())
	// r2 refuses writes now.
	cl := newTextClient(t, nodes[1].addr())
	if resp := cl.send("PUT after-removal x"); !strings.HasPrefix(resp, "ERR") {
		t.Fatalf("removed r2 accepted a write: %q", resp)
	}
	cl.close()

	members := []string{nodes[0].addr(), nodes[2].addr(), nodes[3].addr()}
	ks := waitConvergedAddrs(t, members, 90*time.Second)
	checkAcked(t, "r1/r3/r4", g, ks)
	t.Logf("%d keys on r1, r3 and r4 (r4 joined late)", len(ks))

	// ── restart every node with its ORIGINAL flags: the persisted
	// membership wins over --peers; r2 stays removed.
	for _, n := range nodes {
		n.kill()
	}
	for _, n := range nodes {
		n.start()
	}
	for _, id := range []string{"r1", "r3", "r4"} {
		waitMembers(t, nodeByID(nodes, id), want3, 30*time.Second)
	}
	if m, err := getMembers(nodes[1]); err != nil || m.SelfStatus != "removed" {
		t.Fatalf("r2 after restart: %+v %v", m, err)
	}
	ks2 := waitConvergedAddrs(t, members, 60*time.Second)
	checkAcked(t, "after restart", g, ks2)
}
