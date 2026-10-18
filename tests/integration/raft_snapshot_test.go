package integration_test

// raft_snapshot_test.go — raft snapshots on a real 3-process cluster: a
// follower that was down while the leader wrote, deleted and compacted its
// log catches up through (chunked) InstallSnapshot and then matches the
// leader key for key; and the same with the follower crashing at every stage
// of the install (VELTRIX_TEST_CRASH_AT_SNAPSHOT_STAGE) before it restarts.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	veltrixclient "github.com/VeltrixDB/veltrixdb/client"
)

type snapProc struct {
	t       *testing.T
	id      string
	port    int
	metrics int
	dataDir string
	peers   string
	srv     *testServer
	exited  chan error
}

// start launches the node on its (persistent) data dir with extra env.
func (p *snapProc) start(env ...string) {
	t := p.t
	t.Helper()
	flags := []string{
		"-addr", fmt.Sprintf("127.0.0.1:%d", p.port),
		"-metrics-addr", fmt.Sprintf("127.0.0.1:%d", p.metrics),
		"-data", p.dataDir,
		"-wal-flush-window-ms", "1",
		"-vlog-flush-window-ms", "1",
	}
	flags = append(flags, raftArgs(t, p.id, p.peers,
		"-raft-snapshot-threshold", "64", "-raft-snapshot-chunk-kb", "16")...)
	bin := serverBinary()
	if bin == "" {
		bin = builtBinary(t)
	}
	cmd := exec.Command(bin, flags...)
	cmd.Dir = moduleRoot(t)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", p.id, err)
	}
	p.exited = make(chan error, 1)
	go func(c *exec.Cmd, ch chan error) { ch <- c.Wait() }(cmd, p.exited)
	// DataDir left empty: stop() must not delete the persistent dir.
	p.srv = &testServer{Addr: fmt.Sprintf("127.0.0.1:%d", p.port), cmd: nil, t: t}
	p.srv.cmd = cmd
}

func (p *snapProc) waitReady() {
	p.t.Helper()
	if err := p.srv.waitReady(20 * time.Second); err != nil {
		p.t.Fatalf("%s: %v", p.id, err)
	}
}

// kill SIGKILLs the process (a crash: no shutdown checkpoint).
func (p *snapProc) kill() {
	if p.srv != nil && p.srv.cmd != nil && p.srv.cmd.Process != nil {
		_ = p.srv.cmd.Process.Kill()
		<-p.exited
	}
	p.srv = nil
}

// waitExit waits for the process to exit on its own and returns the error.
func (p *snapProc) waitExit(timeout time.Duration) (error, bool) {
	select {
	case err := <-p.exited:
		p.srv = nil
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func startSnapCluster(t *testing.T) []*snapProc {
	t.Helper()
	used := map[int]bool{}
	ids := []string{"z1", "z2", "z3"}
	ports := []int{reserveBlock(t, used), reserveBlock(t, used), reserveBlock(t, used)}
	pf := peersFlag(ids, ports)
	var procs []*snapProc
	for i, id := range ids {
		dir, err := os.MkdirTemp("", "veltrix-snap-")
		if err != nil {
			t.Fatal(err)
		}
		p := &snapProc{t: t, id: id, port: ports[i], metrics: ports[i] + 4, dataDir: dir, peers: pf}
		procs = append(procs, p)
	}
	t.Cleanup(func() {
		for _, p := range procs {
			p.kill()
			os.RemoveAll(p.dataDir)
		}
	})
	for _, p := range procs {
		p.start()
	}
	for _, p := range procs {
		p.waitReady()
	}
	return procs
}

func addrsOf(procs []*snapProc) []string {
	var out []string
	for _, p := range procs {
		if p.srv != nil {
			out = append(out, p.srv.Addr)
		}
	}
	return out
}

// writeAll runs fn(i) for i in [0,n) on 8 binary connections to addr.
func writeAll(t *testing.T, addr string, n int, fn func(c *veltrixclient.BinaryConn, i int) error) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := veltrixclient.DialBinary(addr, 5*time.Second)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			for i := w; i < n; i += 8 {
				if err := fn(c, i); err != nil {
					errs <- fmt.Errorf("op %d: %w", i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// keyspace returns every key/value a node serves (RANGE over everything).
func keyspace(addr string) (map[string]string, error) {
	c, err := veltrixclient.DialBinary(addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	out := map[string]string{}
	for start := ""; ; {
		kvs, err := c.RangeScan(start, "", 1000, false)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			out[kv.Key] = string(kv.Value)
		}
		if len(kvs) < 1000 {
			return out, nil
		}
		start = kvs[len(kvs)-1].Key + "\x00"
	}
}

func diffKeyspaces(a, b map[string]string) []string {
	var d []string
	for k, v := range a {
		if bv, ok := b[k]; !ok {
			d = append(d, "missing "+k)
		} else if bv != v {
			d = append(d, "value "+k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			d = append(d, "extra "+k)
		}
	}
	sort.Strings(d)
	if len(d) > 10 {
		d = append(d[:10], fmt.Sprintf("… %d more", len(d)-10))
	}
	return d
}

// waitSameKeyspace waits until every running node serves exactly the
// leader's keyspace and returns it.
func waitSameKeyspace(t *testing.T, procs []*snapProc, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		_, leaderAddr := waitForRaftLeader(t, addrsOf(procs), 20*time.Second)
		want, err := keyspace(leaderAddr)
		if err == nil {
			ok := true
			for _, a := range addrsOf(procs) {
				if a == leaderAddr {
					continue
				}
				got, err := keyspace(a)
				if err != nil {
					ok, last = false, err.Error()
					break
				}
				if d := diffKeyspaces(want, got); len(d) > 0 {
					ok, last = false, a+": "+strings.Join(d, ", ")
					break
				}
			}
			if ok {
				return want
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("keyspaces did not converge within %v: %s", timeout, last)
	return nil
}

func leaderProc(t *testing.T, procs []*snapProc) *snapProc {
	t.Helper()
	_, addr := waitForRaftLeader(t, addrsOf(procs), 20*time.Second)
	for _, p := range procs {
		if p.srv != nil && p.srv.Addr == addr {
			return p
		}
	}
	t.Fatal("leader not found")
	return nil
}

// churn writes, overwrites, deletes and TTL-writes through the leader until it
// has compacted its log (a new raft_snapshot.dat), so a follower that was
// down can only catch up through InstallSnapshot.
func churn(t *testing.T, procs []*snapProc, round int) {
	t.Helper()
	l := leaderProc(t, procs)
	snapPath := filepath.Join(l.dataDir, "raft", "raft_snapshot.dat")
	tag := fmt.Sprintf("r%d", round)
	writeAll(t, l.srv.Addr, 1500, func(c *veltrixclient.BinaryConn, i int) error {
		switch {
		case i%5 == 0:
			return c.Delete(fmt.Sprintf("base%05d", (i*7+round*13)%3000))
		case i%5 == 1:
			return c.Put(fmt.Sprintf("base%05d", (i*11+round)%3000), []byte(tag+"-over"), -1)
		case i%5 == 2:
			return c.Put(fmt.Sprintf("short-%s-%04d", tag, i), []byte("expires"), 2)
		default:
			return c.Put(fmt.Sprintf("%s-%05d", tag, i), []byte(strings.Repeat("v", 200)), 3600)
		}
	})
	// A snapshot captured after all of the above is past anything the
	// stopped follower has; pad until the leader takes one.
	before := time.Now()
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; ; i++ {
		writeAll(t, l.srv.Addr, 16, func(c *veltrixclient.BinaryConn, j int) error {
			return c.Put(fmt.Sprintf("pad%02d", j), []byte(fmt.Sprint(i)), -1)
		})
		if fi, err := os.Stat(snapPath); err == nil && fi.ModTime().After(before) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("leader did not compact its log")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRaftSnapshot_FollowerCatchUpKeyForKey: a follower is SIGKILLed while
// the leader writes, deletes, overwrites and compacts; restarted, it catches
// up through chunked InstallSnapshot (16 KB chunks) and serves exactly the
// leader's keyspace — deleted keys gone, and keys written with a 2 s TTL
// expire on it too (v1.16 restored them immortal).
func TestRaftSnapshot_FollowerCatchUpKeyForKey(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	procs := startSnapCluster(t)
	l := leaderProc(t, procs)
	writeAll(t, l.srv.Addr, 3000, func(c *veltrixclient.BinaryConn, i int) error {
		return c.Put(fmt.Sprintf("base%05d", i), []byte(fmt.Sprintf("v%d", i)), -1)
	})
	waitSameKeyspace(t, procs, 30*time.Second)

	var f *snapProc
	for _, p := range procs {
		if p != l {
			f = p
			break
		}
	}
	f.kill()
	churn(t, procs, 1)
	t0 := time.Now()
	f.start()
	f.waitReady()
	want := waitSameKeyspace(t, procs, 60*time.Second)
	t.Logf("follower caught up in %v (%d keys)", time.Since(t0).Round(time.Millisecond), len(want))
	if _, err := os.Stat(filepath.Join(f.dataDir, "raft", "raft_snapshot.dat")); err != nil {
		t.Fatalf("follower has no snapshot — it did not catch up through InstallSnapshot: %v", err)
	}
	// The 2 s TTL keys expire everywhere, the restored follower included.
	time.Sleep(3 * time.Second)
	got := waitSameKeyspace(t, procs, 30*time.Second)
	for k := range got {
		if strings.HasPrefix(k, "short-") {
			t.Fatalf("%s outlived its TTL", k)
		}
	}
}

// TestRaftSnapshot_InstallCrashAtEveryStage: the follower crashes (exit 86,
// no cleanup) at each stage of an install, is restarted, and converges to the
// leader's keyspace.
func TestRaftSnapshot_InstallCrashAtEveryStage(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	procs := startSnapCluster(t)
	l := leaderProc(t, procs)
	writeAll(t, l.srv.Addr, 3000, func(c *veltrixclient.BinaryConn, i int) error {
		return c.Put(fmt.Sprintf("base%05d", i), []byte(fmt.Sprintf("v%d", i)), -1)
	})
	waitSameKeyspace(t, procs, 30*time.Second)
	var f *snapProc
	for _, p := range procs {
		if p != l {
			f = p
			break
		}
	}
	for round, stage := range []string{"install-verified", "install-renamed", "install-log-rewritten", "fsm-restore-partial", "install-restored"} {
		f.kill()
		churn(t, procs, round+2)
		f.start("VELTRIX_TEST_CRASH_AT_SNAPSHOT_STAGE=" + stage)
		err, exited := f.waitExit(60 * time.Second)
		if !exited {
			t.Fatalf("%s: the follower never reached the crash stage", stage)
		}
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 86 {
			t.Fatalf("%s: follower exited with %v, want the injected crash (86)", stage, err)
		}
		f.start()
		f.waitReady()
		waitSameKeyspace(t, procs, 60*time.Second)
		t.Logf("crash at %s: restarted follower converged", stage)
	}
}
