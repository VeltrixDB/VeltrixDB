package main

// raft_applied_test.go — exactly-once apply across restarts (CLAUDE.md
// invariant 65).  A raft node used to replay its whole retained log over an
// engine that already held the effects of those entries, so every
// non-idempotent command was applied twice on restart: counters doubled,
// lists grew, and a version-guarded TXN re-applied after the PUT it guarded
// against failed its guard.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// waitApplied waits until node i has applied everything the leader has.
func (c *snapTestCluster) waitApplied(i int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		l := c.waitLeader()
		want := l.node.SnapshotStatus().LastApplied
		c.mu.Lock()
		n := c.nodes[i].node
		c.mu.Unlock()
		if n != nil && n.SnapshotStatus().LastApplied >= want {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("node %d did not catch up to applied index %d", i, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// nonIdempotentWorkload submits n-tailOps INCRs of "ctr" and RPUSHes to
// "list"; per node, a TXN guarded by the key version read from that node
// (the same on every node — invariant 66 — so every guard holds everywhere);
// then tailOps more INCRs and RPUSHes, so a snapshot taken meanwhile leaves
// some of each in the retained log tail.  Afterwards every node must hold n
// in the counter and the list, and "after" in its guarded key.
func nonIdempotentWorkload(t *testing.T, c *snapTestCluster, n int) {
	t.Helper()
	workload(t, c, n, true)
}

// workload is nonIdempotentWorkload; guarded=false leaves out the per-node
// SETIF TXNs.
func workload(t *testing.T, c *snapTestCluster, n int, guarded bool) {
	t.Helper()
	push := func(m int) {
		for i := 0; i < m; i++ {
			c.submit(fsmCmd{Op: opIncr, Key: "ctr", Delta: 1, TTL: -1})
			c.submit(fsmCmd{Op: opRPush, Key: "list", Value: []byte(strconv.Itoa(i))})
		}
	}
	push(n - tailOps)
	for i := range c.nodes {
		if !guarded {
			break
		}
		k := fmt.Sprintf("guarded-%d", i)
		c.submit(fsmCmd{Op: opPut, Key: k, Value: []byte("before"), TTL: -1})
		c.waitApplied(i, 20*time.Second)
		ver := c.nodes[i].engine.KeyVersion(k)
		c.submit(fsmCmd{Op: opTxn, TxnOps: []fsmTxnOp{
			{Op: "SETIF", Key: k, Value: []byte("after"), TTL: -1, ExpectedVersion: ver},
			{Op: "SET", Key: k + "-side", Value: []byte("x"), TTL: -1},
		}})
	}
	push(tailOps)
}

const tailOps = 5

func checkNonIdempotent(t *testing.T, what string, i int, e *storage.StorageEngine, n int) {
	t.Helper()
	checkWorkload(t, what, i, e, n, true)
}

func checkWorkload(t *testing.T, what string, i int, e *storage.StorageEngine, n int, guarded bool) {
	t.Helper()
	if v, err := e.Get("ctr"); err != nil || string(v) != strconv.Itoa(n) {
		t.Errorf("%s: node %d counter = %q (%v), want %d", what, i, v, err, n)
	}
	if got := e.LLen("list"); got != int64(n) {
		t.Errorf("%s: node %d list length = %d, want %d", what, i, got, n)
	}
	k := fmt.Sprintf("guarded-%d", i)
	if !guarded {
		return
	}
	if v, err := e.Get(k); err != nil || string(v) != "after" {
		t.Errorf("%s: node %d %s = %q (%v), want \"after\" (the TXN applied once)", what, i, k, v, err)
	}
}

// TestRaftRestart_NoDoubleApply: INCR, RPUSH and a version-guarded TXN on a
// 3-node cluster, then every node is restarted in turn (clean stop) and must
// hold exactly the effect of each command applied once — with no snapshot
// (the whole log is retained) and with snapshots (a tail after the snapshot).
func TestRaftRestart_NoDoubleApply(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold uint64
	}{{"no-snapshot", 0}, {"snapshot", 16}} {
		t.Run(tc.name, func(t *testing.T) {
			const n = 40
			c := newSnapTestCluster(t, 3, tc.threshold)
			c.waitLeader()
			nonIdempotentWorkload(t, c, n)
			for i := range c.nodes {
				c.waitApplied(i, 20*time.Second)
				checkNonIdempotent(t, "before restart", i, c.nodes[i].engine, n)
			}
			for i := range c.nodes {
				c.stop(i)
				c.start(i)
				c.waitApplied(i, 30*time.Second)
				// A few more heartbeats: anything replayed is applied by now.
				time.Sleep(200 * time.Millisecond)
				checkNonIdempotent(t, "after restart", i, c.nodes[i].engine, n)
			}
		})
	}
}

// TestRaftRestart_SnapshotRestoreSkipped: a node whose engine already holds
// its snapshot's state (its applied index ≥ the snapshot index, no restore
// in progress) does not run the O(keys) restore at startup, and still ends
// up key-for-key equal to the leader.
func TestRaftRestart_SnapshotRestoreSkipped(t *testing.T) {
	c := newSnapTestCluster(t, 3, 16)
	c.waitLeader()
	workload(t, c, 40, false)
	for i := 0; i < 50; i++ {
		c.submit(fsmCmd{Op: opPut, Key: fmt.Sprintf("k%02d", i), Value: []byte("v"), TTL: 3600})
		if i%3 == 0 {
			c.submit(fsmCmd{Op: opDelete, Key: fmt.Sprintf("k%02d", i/2)})
		}
	}
	for i := range c.nodes {
		c.waitApplied(i, 20*time.Second)
	}
	for i := range c.nodes {
		if !hasSnapshotFile(c.nodes[i].raftDir) {
			t.Fatalf("node %d has no snapshot", i)
		}
		c.stop(i)
		c.start(i)
		st := c.nodes[i].node.SnapshotStatus()
		if st.StartupRestores != 0 || st.StartupRestoreSkips != 1 {
			t.Fatalf("node %d: StartupRestores=%d StartupRestoreSkips=%d, want 0/1", i, st.StartupRestores, st.StartupRestoreSkips)
		}
		c.waitApplied(i, 30*time.Second)
		checkWorkload(t, "after restart", i, c.nodes[i].engine, 40, false)
	}
	waitEnginesEqual(t, c, 30*time.Second)
}

// TestRaftRestart_UpgradeFromUnmarkedEngine: engines written without the
// applied index (a pre-fix build) are restarted with it.  Without a
// snapshot the engine is reset and the whole log applied once; with one, the
// snapshot is restored and the tail applied.  Either way every command is
// applied exactly once, the marker exists afterwards, and the next restart
// neither resets nor restores.
func TestRaftRestart_UpgradeFromUnmarkedEngine(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold uint64
	}{{"no-snapshot", 0}, {"snapshot", 16}} {
		t.Run(tc.name, func(t *testing.T) {
			const n = 30
			c := newSnapTestClusterApplied(t, 3, tc.threshold, false)
			c.waitLeader()
			// Versions surviving a rebuild from the log are covered by
			// TestRaftDeterminism_GuardsSurviveRestartAndRebuild.
			workload(t, c, n, false)
			for i := range c.nodes {
				c.waitApplied(i, 20*time.Second)
			}
			c.noApplied = false // the upgrade
			for i := range c.nodes {
				c.stop(i)
				c.start(i)
				st := c.nodes[i].node.SnapshotStatus()
				if tc.threshold == 0 && st.StartupResets != 1 {
					t.Fatalf("node %d: StartupResets=%d, want 1", i, st.StartupResets)
				}
				if tc.threshold > 0 && st.StartupRestores != 1 {
					t.Fatalf("node %d: StartupRestores=%d, want 1 (no marker: full restore)", i, st.StartupRestores)
				}
				c.waitApplied(i, 30*time.Second)
				time.Sleep(200 * time.Millisecond)
				checkWorkload(t, "after the upgrade restart", i, c.nodes[i].engine, n, false)
			}
			for i := range c.nodes {
				c.stop(i)
				c.start(i)
				st := c.nodes[i].node.SnapshotStatus()
				if st.StartupResets != 0 || st.StartupRestores != 0 {
					t.Fatalf("node %d second restart: resets=%d restores=%d, want 0/0", i, st.StartupResets, st.StartupRestores)
				}
				c.waitApplied(i, 30*time.Second)
				checkWorkload(t, "after the second restart", i, c.nodes[i].engine, n, false)
			}
		})
	}
}

// TestRaftRestart_SnapshotNodeRestartTime measures the restart of a
// single-node raft whose snapshot holds VELTRIX_RESTART_BENCH_KEYS keys
// (128 B values): with the applied index (the startup restore is skipped)
// and without it (the pre-fix path: every start restores the whole engine
// from raft_snapshot.dat, O(keys)).  Env-gated: it writes ~N × 150 B.
func TestRaftRestart_SnapshotNodeRestartTime(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("VELTRIX_RESTART_BENCH_KEYS"))
	if n <= 0 {
		t.Skip("set VELTRIX_RESTART_BENCH_KEYS (e.g. 1000000)")
	}
	root := t.TempDir()
	engDir, raftDir := filepath.Join(root, "data"), filepath.Join(root, "raft")
	val := bytes.Repeat([]byte("v"), 128)

	e := snapTestEngineApplied(t, engDir, true)
	t0 := time.Now()
	batch := make([]storage.MultiPutRequest, 0, 4096)
	for i := 0; i < n; i++ {
		batch = append(batch, storage.MultiPutRequest{Key: fmt.Sprintf("key%09d", i), Value: val, TTL: -1})
		if len(batch) == cap(batch) || i == n-1 {
			for _, err := range e.MultiPut(batch) {
				if err != nil {
					t.Fatal(err)
				}
			}
			batch = batch[:0]
		}
	}
	t.Logf("preloaded %d keys in %s", n, time.Since(t0).Round(time.Millisecond))

	start := func(eng *storage.StorageEngine) (*consensus.RaftNode, time.Duration) {
		t1 := time.Now()
		node, err := consensus.NewRaftNodeWithOptions("solo", nil, raftDir, newRaftFSM(eng), consensus.NewTCPTransport(map[string]string{}),
			consensus.Options{SnapshotThreshold: 64})
		if err != nil {
			t.Fatal(err)
		}
		return node, time.Since(t1)
	}
	node, _ := start(e)
	deadline := time.Now().Add(5 * time.Minute)
	for i := 0; node.SnapshotStatus().LastIncludedIndex == 0; i++ {
		if time.Now().After(deadline) {
			t.Fatal("no snapshot")
		}
		b, _ := encodeCmd(fsmCmd{Op: opPut, Key: fmt.Sprintf("pad%04d", i%100), Value: []byte("x"), TTL: -1})
		if err := node.Submit(b); err != nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	st := node.SnapshotStatus()
	t.Logf("snapshot idx=%d size=%d MB capture=%s", st.LastIncludedIndex, st.LastSnapshotBytes>>20, st.LastCapture.Round(time.Millisecond))
	node.Stop()
	_ = e.Close()

	for _, mode := range []struct {
		name    string
		applied bool
	}{{"after (applied index)", true}, {"before (no applied index: full restore)", false}} {
		t2 := time.Now()
		eng := snapTestEngineApplied(t, engDir, mode.applied)
		open := time.Since(t2)
		node, raftStart := start(eng)
		s := node.SnapshotStatus()
		t.Logf("%-42s engine open+replay %8s  raft start %8s  total %8s  (startup restores=%d skips=%d)",
			mode.name, open.Round(time.Millisecond), raftStart.Round(time.Millisecond),
			(open + raftStart).Round(time.Millisecond), s.StartupRestores, s.StartupRestoreSkips)
		if mode.applied && s.StartupRestoreSkips != 1 {
			t.Errorf("with the applied index the restore was not skipped")
		}
		if !mode.applied && s.StartupRestores != 1 {
			t.Errorf("without the applied index the snapshot was not restored")
		}
		node.Stop()
		_ = eng.Close()
	}
}
