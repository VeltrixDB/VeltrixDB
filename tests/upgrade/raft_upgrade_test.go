package upgrade_test

// raft_upgrade_test.go — --mode=raft clusters that mix the old (v1.16) and
// the new server binary:
//
//	TestUpgrade_RaftRolling              3 × old, loaded past the snapshot
//	                                     threshold; followers swapped one by one,
//	                                     then the leader, with writes running.
//	TestUpgrade_RaftOldFollowerSnapshot  new leader → v1.16 follower that needs
//	                                     a snapshot (single-shot path, < 256 MB).
//	TestUpgrade_RaftNewFollowerFromOldLeader
//	                                     v1.16 leader → new follower that needs a
//	                                     snapshot (v1.16 single-shot install).
//	TestDowngrade_Raft                   the documented downgrade: v1.16 started
//	                                     on new raft files (must fail loudly),
//	                                     then wipe + re-sync, followers first.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newSnapThreshold is VELTRIX_RAFT_SNAPSHOT_THRESHOLD for the new binary.
// v1.16 has no such knob: its threshold is fixed at 8192 entries, so the
// scenarios write until raft_snapshot.gob exists on every old node.
const newSnapThreshold = "1000"

func raftCluster(t *testing.T, dir string, ids ...string) []*node {
	t.Helper()
	nodes := make([]*node, len(ids))
	for i, id := range ids {
		nodes[i] = newNode(t, dir, id, nil, "VELTRIX_RAFT_SNAPSHOT_THRESHOLD="+newSnapThreshold)
	}
	var peers []string
	for _, n := range nodes {
		peers = append(peers, fmt.Sprintf("%s@127.0.0.1:%d", n.id, n.port))
	}
	for _, n := range nodes {
		n.args = []string{"-mode", "raft", "-node-id", n.id, "-peers", strings.Join(peers, ",")}
	}
	return nodes
}

func raftWriter(nodes []*node) func() writer {
	return func() writer { return &raftClient{nodes: nodes} }
}

func snapFile(n *node, ver string) string {
	if ver == verOld {
		return filepath.Join(n.dataDir, "raft", "raft_snapshot.gob")
	}
	return filepath.Join(n.dataDir, "raft", "raft_snapshot.dat")
}

// fillUntilSnapshot writes filler keys until every node in want has a
// snapshot file of its version (or fails after max ops).
func fillUntilSnapshot(t *testing.T, nodes []*node, want []*node, m *model, prefix string, max int) int {
	t.Helper()
	written := 0
	for {
		all := true
		for _, n := range want {
			if !fileExists(snapFile(n, n.ver)) {
				all = false
			}
		}
		if all {
			return written
		}
		if written >= max {
			var st []string
			for _, n := range want {
				st = append(st, fmt.Sprintf("%s(%s) snapshot=%v", n.id, n.ver, fileExists(snapFile(n, n.ver))))
			}
			t.Fatalf("no snapshot after %d filler writes: %s", written, strings.Join(st, ", "))
		}
		a, u := runOps(t, raftWriter(nodes), m, 16, fillerOps(prefix, written, 2000))
		if u > 0 {
			t.Logf("filler: %d acknowledged, %d unknown", a, u)
		}
		written += 2000
		time.Sleep(500 * time.Millisecond) // the capture runs after apply
	}
}

// syncRaft writes a marker through the leader and waits until every running
// node applied it (raft applies in log order, so everything before it too).
func syncRaft(t *testing.T, nodes []*node, tag string) {
	t.Helper()
	rc := &raftClient{nodes: nodes}
	defer rc.close()
	key, val := "sync/"+tag, fmt.Sprintf("%d", time.Now().UnixNano())
	if resp, out := rc.write("PUT "+key+" "+val, isOKReply, 60*time.Second); out != acked {
		t.Fatalf("sync marker %s not acknowledged: %q", tag, resp)
	}
	for _, n := range nodes {
		if !n.running() {
			continue
		}
		if err := waitKey(n.addr(), key, val, 90*time.Second); err != nil {
			t.Fatalf("node %s (%s) did not catch up: %v\n%s", n.id, n.ver, err, tailFile(n.currentLog(), 30))
		}
	}
}

// verifyCluster: all running nodes key-for-key equal, every acknowledged
// write / delete / counter as the model says, TTL probes still set.
func verifyCluster(t *testing.T, nodes []*node, m *model, label string) map[string]map[string]string {
	t.Helper()
	all, diffs := waitConverged(nodes, 60*time.Second)
	if len(diffs) > 0 {
		t.Errorf("%s: nodes are not key-for-key equal:\n%s", label, joinLines(diffs))
	} else {
		t.Logf("%s: all %d nodes key-for-key equal (%d keys)", label, len(nodes), len(all[nodes[0].id]))
	}
	for _, n := range nodes {
		if kv, ok := all[n.id]; ok {
			reportAll(t, m.check(label+" "+n.id+"("+n.ver+")", kv))
		}
		reportAll(t, m.ttlProblems(label+" "+n.id+"("+n.ver+")", n.addr()))
	}
	return all
}

func logEvidence(t *testing.T, nodes []*node, patterns ...string) {
	t.Helper()
	for _, n := range nodes {
		for _, p := range patterns {
			for _, l := range n.grepLogs(p) {
				t.Logf("  log %s", l)
			}
		}
	}
}

func checkNoPanics(t *testing.T, nodes []*node) {
	t.Helper()
	for _, n := range nodes {
		for _, p := range []string{"panic:", "fatal error:", "DATA RACE"} {
			if ls := n.grepLogs(p); len(ls) > 0 {
				t.Errorf("node %s logged %q:\n%s", n.id, p, joinLines(ls))
			}
		}
	}
}

// ── rolling upgrade ─────────────────────────────────────────────────────────

// TestUpgrade_RaftRollingNoSnapshot is the same rolling upgrade on a v1.16
// cluster whose log never reached the 8192-entry snapshot threshold, so each
// restart replays the whole raft log onto the WAL-recovered engine.
func TestUpgrade_RaftRollingNoSnapshot(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := raftCluster(t, dir, "n1", "n2", "n3")
	for _, n := range nodes {
		n.env = []string{"VELTRIX_RAFT_SNAPSHOT_THRESHOLD=0"} // the new build: never snapshot either
		n.start(verOld)
	}
	waitLeader(t, nodes, 30*time.Second)
	m := newModel()
	a, u := runOps(t, raftWriter(nodes), m, 16, baseOps("pre", 1000, 0))
	t.Logf("base load: %d acknowledged, %d unknown (no snapshot: %v)", a, u, !fileExists(snapFile(nodes[0], verOld)))
	syncRaft(t, nodes, "loaded")
	verifyCluster(t, nodes, m, "v1.16 baseline")
	bg := startBackground(raftWriter(nodes), m, "bg")
	for round := 0; round < 3; round++ {
		lead := waitLeader(t, nodes, 30*time.Second)
		var target *node
		for _, n := range nodes {
			if n.ver == verOld && (n != lead || round == 2) {
				target = n
				break
			}
		}
		t.Logf("upgrade %d/3: %s (leader %s)", round+1, target.id, lead.id)
		target.swap(verNew)
		syncRaft(t, nodes, "after-upgrade-"+target.id)
		time.Sleep(2 * time.Second)
	}
	bg.stop(t)
	syncRaft(t, nodes, "upgraded")
	verifyCluster(t, nodes, m, "after rolling upgrade (no snapshot)")
	checkNoPanics(t, nodes)
}

func TestUpgrade_RaftRolling(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := raftCluster(t, dir, "n1", "n2", "n3")
	for _, n := range nodes {
		n.start(verOld)
	}
	lead := waitLeader(t, nodes, 30*time.Second)
	t.Logf("v1.16 cluster up, leader %s", lead.id)

	// 1. Load: keys, deletes, counters, TTL keys, then fill past v1.16's
	// fixed 8192-entry threshold so every node has a snapshot + compacted log.
	m := newModel()
	a, u := runOps(t, raftWriter(nodes), m, 16, baseOps("pre", 3000, 240))
	t.Logf("base load: %d acknowledged, %d unknown", a, u)
	filled := fillUntilSnapshot(t, nodes, nodes, m, "pre", 60000)
	t.Logf("filler writes until every v1.16 node has raft_snapshot.gob: %d", filled)
	// Writes after the snapshot live only in the log tail.
	runOps(t, raftWriter(nodes), m, 8, baseOps("tail", 300, 0))
	syncRaft(t, nodes, "loaded")
	for _, n := range nodes {
		fi, _ := os.Stat(snapFile(n, verOld))
		lf, _ := os.Stat(filepath.Join(n.dataDir, "raft", "raft_log.dat"))
		t.Logf("%s before upgrade: raft_snapshot.gob %d bytes, raft_log.dat %d bytes", n.id, fi.Size(), lf.Size())
	}
	logEvidence(t, nodes, "snapshot taken")
	verifyCluster(t, nodes, m, "v1.16 baseline")
	if t.Failed() {
		t.Fatal("baseline on v1.16 already inconsistent — not an upgrade problem")
	}

	// 2-3. Rolling upgrade with writes running: followers first, leader last.
	bg := startBackground(raftWriter(nodes), m, "bg")
	for round := 0; round < 3; round++ {
		lead = waitLeader(t, nodes, 30*time.Second)
		var target *node
		for _, n := range nodes {
			if n.ver == verOld && (n != lead || round == 2) {
				target = n
				break
			}
		}
		role := "follower"
		if target == lead {
			role = "LEADER"
		}
		t.Logf("upgrade %d/3: %s (%s), leader is %s", round+1, target.id, role, lead.id)
		target.swap(verNew)
		syncRaft(t, nodes, fmt.Sprintf("after-upgrade-%s", target.id))
		if !fileExists(snapFile(target, verNew)) {
			t.Errorf("%s: no raft_snapshot.dat after upgrade (raft_snapshot.gob not migrated?)", target.id)
		}
		time.Sleep(2 * time.Second) // keep writing in the mixed cluster
	}
	bg.stop(t)
	syncRaft(t, nodes, "upgraded")
	logEvidence(t, nodes, "migrated", "restored snapshot", "NOT sending snapshot", "single-shot", "installed snapshot")
	verifyCluster(t, nodes, m, "after rolling upgrade")

	// A new-format snapshot is captured on the new build, and a restart of
	// every node (each restores its engine from raft_snapshot.dat) keeps it.
	before := map[string]time.Time{}
	for _, n := range nodes {
		fi, _ := os.Stat(snapFile(n, verNew))
		before[n.id] = fi.ModTime()
	}
	runOps(t, raftWriter(nodes), m, 16, fillerOps("post", 0, 3000))
	syncRaft(t, nodes, "post-fill")
	time.Sleep(time.Second)
	for _, n := range nodes {
		fi, _ := os.Stat(snapFile(n, verNew))
		t.Logf("%s: raft_snapshot.dat %d bytes, recaptured on the new build: %v", n.id, fi.Size(), fi.ModTime().After(before[n.id]))
	}
	for _, n := range nodes {
		n.swap(verNew)
		syncRaft(t, nodes, "restart-"+n.id)
	}
	verifyCluster(t, nodes, m, "after restarting every upgraded node")
	checkNoPanics(t, nodes)
}

// ── v1.16 follower ← new leader (single-shot snapshot) ─────────────────────

func TestUpgrade_RaftOldFollowerSnapshot(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := raftCluster(t, dir, "n1", "n2", "n3")
	old := nodes[2]
	nodes[0].start(verNew)
	nodes[1].start(verNew)
	old.start(verOld)
	lead := waitLeader(t, nodes, 30*time.Second)
	for lead == old { // the scenario needs a new-build leader
		old.swap(verOld)
		lead = waitLeader(t, nodes, 30*time.Second)
	}
	t.Logf("leader %s (new), v1.16 follower %s", lead.id, old.id)

	m := newModel()
	runOps(t, raftWriter(nodes), m, 16, baseOps("pre", 2000, 0))
	syncRaft(t, nodes, "pre")

	// The v1.16 follower misses deletes, updates, counters and TTL keys, and
	// the leader compacts past its log position (threshold 1000).
	old.stop()
	var ops []op
	for i := 1; i < 2000; i += 10 { // delete keys the follower still holds
		ops = append(ops, op{kind: 'D', key: fmt.Sprintf("pre/k/%05d", i)})
	}
	for i := 2; i < 2000; i += 10 {
		ops = append(ops, op{kind: 'P', key: fmt.Sprintf("pre/k/%05d", i), val: "updated"})
	}
	ops = append(ops, baseOps("down", 1500, 0)...)
	runOps(t, raftWriter(nodes), m, 16, ops)
	fillUntilSnapshot(t, nodes, []*node{nodes[0], nodes[1]}, m, "down", 20000)
	runOps(t, raftWriter(nodes), m, 16, fillerOps("down2", 0, 2500)) // a second compaction
	syncRaft(t, nodes, "down")

	old.start(verOld)
	syncRaft(t, nodes, "rejoined")
	logEvidence(t, nodes, "single-shot", "NOT sending snapshot", "installed snapshot", "rejecting oversized")
	if len(lead.grepLogs("single-shot snapshot")) == 0 {
		t.Errorf("leader %s never sent a single-shot snapshot to %s — the follower caught up some other way?", lead.id, old.id)
	}

	// Documented: the v1.16 single-shot format has no TTLs, so TTL'd keys are
	// immortal on the v1.16 follower. Everything else must match.
	all, diffs := waitConverged(nodes, 60*time.Second)
	if len(diffs) > 0 {
		t.Errorf("v1.16 follower %s differs from the new nodes after a single-shot snapshot:\n%s", old.id, joinLines(diffs))
	}
	for _, n := range nodes {
		reportAll(t, m.check(n.id+"("+n.ver+")", all[n.id]))
	}
	for _, n := range nodes[:2] {
		reportAll(t, m.ttlProblems(n.id+"("+n.ver+")", n.addr()))
	}
	if p := m.ttlProblems(old.id+"(old)", old.addr()); len(p) > 0 {
		t.Logf("DOCUMENTED LIMITATION (v1.16 follower, single-shot format has no TTL):\n%s", joinLines(p))
	}
	checkNoPanics(t, nodes)
}

// ── new follower ← v1.16 leader (v1.16 single-shot install) ────────────────

func TestUpgrade_RaftNewFollowerFromOldLeader(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := raftCluster(t, dir, "n1", "n2", "n3")
	nw := nodes[2]
	nodes[0].start(verOld)
	nodes[1].start(verOld)
	nw.start(verNew)
	lead := waitLeader(t, nodes, 30*time.Second)
	for lead == nw {
		nw.swap(verNew)
		lead = waitLeader(t, nodes, 30*time.Second)
	}
	t.Logf("leader %s (v1.16), new follower %s", lead.id, nw.id)

	m := newModel()
	runOps(t, raftWriter(nodes), m, 16, baseOps("pre", 2000, 0))
	syncRaft(t, nodes, "pre")

	nw.stop()
	var ops []op
	for i := 1; i < 2000; i += 10 {
		ops = append(ops, op{kind: 'D', key: fmt.Sprintf("pre/k/%05d", i)})
	}
	for i := 2; i < 2000; i += 10 {
		ops = append(ops, op{kind: 'P', key: fmt.Sprintf("pre/k/%05d", i), val: "updated"})
	}
	ops = append(ops, baseOps("down", 1500, 0)...)
	runOps(t, raftWriter(nodes), m, 16, ops)
	fillUntilSnapshot(t, nodes, []*node{nodes[0], nodes[1]}, m, "down", 60000)
	syncRaft(t, nodes, "down")

	nw.start(verNew)
	syncRaft(t, nodes, "rejoined")
	logEvidence(t, nodes, "installed snapshot", "restored snapshot")
	if len(nw.grepLogs("installed snapshot")) == 0 {
		t.Errorf("new follower %s did not install a snapshot from the v1.16 leader", nw.id)
	}
	all, diffs := waitConverged(nodes, 60*time.Second)
	if len(diffs) > 0 {
		t.Errorf("new follower %s differs from the v1.16 nodes:\n%s", nw.id, joinLines(diffs))
	}
	for _, n := range nodes {
		reportAll(t, m.check(n.id+"("+n.ver+")", all[n.id]))
		p := m.ttlProblems(n.id+"("+n.ver+")", n.addr())
		if n != nw {
			reportAll(t, p)
		} else if len(p) > 0 {
			// Inherent: the v1.16 snapshot format has no TTL field. Not in
			// the docs, which mention TTL loss only toward a v1.16 follower.
			t.Logf("UNDOCUMENTED LIMITATION (new follower installing a v1.16 leader's snapshot loses TTLs):\n%s", joinLines(p))
		}
	}
	checkNoPanics(t, nodes)
}

// ── downgrade ───────────────────────────────────────────────────────────────

func TestDowngrade_Raft(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := raftCluster(t, dir, "n1", "n2", "n3")
	for _, n := range nodes {
		n.start(verNew)
	}
	lead := waitLeader(t, nodes, 30*time.Second)
	m := newModel()
	runOps(t, raftWriter(nodes), m, 16, baseOps("pre", 2000, 0))
	fillUntilSnapshot(t, nodes, nodes, m, "pre", 20000)
	runOps(t, raftWriter(nodes), m, 8, baseOps("tail", 200, 0))
	syncRaft(t, nodes, "loaded")
	verifyCluster(t, nodes, m, "new-build baseline")

	// (a) What the docs forbid: v1.16 started on the new raft files. It must
	// refuse to start (or at least not serve stale data as if it were current).
	var follower *node
	for _, n := range nodes {
		if n != lead {
			follower = n
			break
		}
	}
	follower.stop()
	err := follower.tryStart(verOld, 15*time.Second)
	if err != nil {
		t.Logf("v1.16 on the new raft files of %s refused to start (good):\n%v", follower.id, err)
	} else {
		time.Sleep(3 * time.Second)
		if !follower.running() {
			t.Logf("v1.16 on the new raft files of %s exited after start:\n%s", follower.id, tailFile(follower.currentLog(), 15))
		} else {
			kv, serr := scanAll(follower.addr())
			ref, _ := scanAll(lead.addr())
			d := diffKV(lead.id, ref, follower.id+"(v1.16 on new files)", kv, 10)
			t.Errorf("v1.16 STARTED on the new raft files of %s (scan err=%v); difference to the leader:\n%s\nlog:\n%s",
				follower.id, serr, joinLines(d), tailFile(follower.currentLog(), 15))
			follower.kill()
		}
	}

	// (b) The documented path, followers first, leader last: stop, wipe the
	// whole --data, start v1.16 with the same id/peers, let it re-sync.
	order := []*node{}
	for _, n := range nodes {
		if n != lead {
			order = append(order, n)
		}
	}
	order = append(order, lead)
	for _, n := range order {
		n.stop()
		n.wipe()
		n.start(verOld)
		syncRaft(t, nodes, "downgraded-"+n.id)
		t.Logf("downgraded %s to v1.16 (wiped), leader now %s", n.id, waitLeader(t, nodes, 30*time.Second).id)
	}
	logEvidence(t, nodes, "single-shot", "NOT sending snapshot", "installed snapshot")
	all, diffs := waitConverged(nodes, 60*time.Second)
	if len(diffs) > 0 {
		t.Errorf("after downgrade the v1.16 nodes differ:\n%s", joinLines(diffs))
	}
	for _, n := range nodes {
		reportAll(t, m.check(n.id+"(v1.16)", all[n.id]))
		if p := m.ttlProblems(n.id+"(v1.16)", n.addr()); len(p) > 0 {
			t.Logf("TTL state after downgrade on %s (docs: TTLs are lost through a single-shot snapshot):\n%s", n.id, joinLines(p))
		}
	}
	checkNoPanics(t, nodes)
}

// ── minimal reproduction: TTLs inside a v1.16 snapshot ─────────────────────

// TestUpgrade_RaftSnapshotTTLRepro is the smallest reproduction of the TTL
// loss seen in TestUpgrade_RaftRolling: one raft node, one TTL key, enough
// writes for a v1.16 snapshot, restart. The "old→old" control shows what a
// plain v1.16 restart does on the same data; "old→new" is the upgrade.
func TestUpgrade_RaftSnapshotTTLRepro(t *testing.T) {
	requireEnabled(t)
	for _, next := range []string{verOld, verNew} {
		t.Run("old→"+next, func(t *testing.T) {
			dir := testDir(t)
			n := raftCluster(t, dir, "n1")[0]
			n.start(verOld)
			waitLeader(t, []*node{n}, 30*time.Second)
			m := newModel()
			runOps(t, raftWriter([]*node{n}), m, 1, []op{{kind: 'H', key: "repro", val: "v", ttl: 86400}})
			fillUntilSnapshot(t, []*node{n}, []*node{n}, m, "fill", 20000)
			before, _ := one(n.addr(), "HTTL repro f")
			n.stop()
			n.start(next)
			waitLeader(t, []*node{n}, 30*time.Second)
			after, _ := one(n.addr(), "HTTL repro f")
			got, _ := one(n.addr(), "HGET repro f")
			t.Logf("HTTL repro f: %s before restart (v1.16, snapshot on disk), %s after restart on %s; HGET = %q", before, after, next, got)
			switch {
			case after != "-1":
			case next == verOld:
				// Control: v1.16's own Restore puts snapshot pairs with
				// ttl -1 on every start. Not an upgrade bug, but it shows the
				// legacy blob cannot carry the TTL.
				t.Logf("CONTROL (v1.16 bug, not an upgrade bug): a v1.16 restart drops the TTL too: HTTL %s → %s", before, after)
			default:
				t.Errorf("TTL lost across the upgrade restart old→%s: HTTL %s → %s (the engine's WAL still had the expiry; restoreLegacyBlob rewrote the key immortal)", next, before, after)
			}
		})
	}
}

// TestDowngrade_RaftNoWipeEmptyLog: the edge of the "v1.16 on new raft files"
// check. A new-build follower that caught up through InstallSnapshot and has
// received nothing since has raft_snapshot.dat and an EMPTY raft_log.dat —
// v1.16 then sees no index gap to complain about. Does it start, and what
// does it serve?
func TestDowngrade_RaftNoWipeEmptyLog(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := raftCluster(t, dir, "n1", "n2", "n3")
	for _, n := range nodes {
		n.start(verNew)
	}
	lead := waitLeader(t, nodes, 30*time.Second)
	var f *node
	for _, n := range nodes {
		if n != lead {
			f = n
			break
		}
	}
	m := newModel()
	runOps(t, raftWriter(nodes), m, 16, baseOps("pre", 1500, 0))
	syncRaft(t, nodes, "pre")
	f.stop()
	var ops []op
	for i := 1; i < 1500; i += 10 {
		ops = append(ops, op{kind: 'D', key: fmt.Sprintf("pre/k/%05d", i)})
	}
	ops = append(ops, fillerOps("down", 0, 2500)...)
	runOps(t, raftWriter(nodes), m, 16, ops)
	// No writes from here on: f installs a snapshot and its log stays empty.
	f.start(verNew)
	if _, diffs := waitConverged(nodes, 60*time.Second); len(diffs) > 0 {
		t.Fatalf("follower did not catch up:\n%s", joinLines(diffs))
	}
	logEvidence(t, []*node{f}, "installed snapshot")
	f.stop()
	lf, _ := os.Stat(filepath.Join(f.dataDir, "raft", "raft_log.dat"))
	t.Logf("%s raft_log.dat = %d bytes (8 = header only), raft_snapshot.dat present=%v", f.id, lf.Size(), fileExists(snapFile(f, verNew)))
	if lf.Size() != 8 {
		t.Skipf("raft_log.dat is not empty (%d bytes) — the edge case was not reached this run", lf.Size())
	}
	if err := f.tryStart(verOld, 15*time.Second); err != nil {
		t.Logf("v1.16 refused the new files (good): %v", err)
		return
	}
	logEvidence(t, []*node{f}, "[raft] node="+f.id+" started")
	time.Sleep(3 * time.Second)
	if !f.running() {
		t.Logf("v1.16 exited:\n%s", tailFile(f.currentLog(), 10))
		return
	}
	t.Errorf("v1.16 STARTED without complaint on new raft files with an empty log (it does not see raft_snapshot.dat)")
	syncRaft(t, nodes, "after-old-start")
	logEvidence(t, nodes, "single-shot", "installed snapshot")
	all, diffs := waitConverged(nodes, 30*time.Second)
	if len(diffs) > 0 {
		t.Errorf("v1.16 node on unwiped new files differs from the cluster:\n%s", joinLines(diffs))
	}
	reportAll(t, m.check(f.id+"(v1.16, not wiped)", all[f.id]))
}

// TestDowngrade_RaftNoSnapshotNoWipe: v1.16 already reads raft_log.dat /
// raft_meta.dat (they shipped in v1.16); only raft_snapshot.dat is new. A
// new-build node that never took a snapshot is therefore started on v1.16
// WITHOUT the documented wipe, followers first, leader last.
func TestDowngrade_RaftNoSnapshotNoWipe(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	nodes := raftCluster(t, dir, "n1", "n2", "n3")
	for _, n := range nodes {
		n.env = []string{"VELTRIX_RAFT_SNAPSHOT_THRESHOLD=0"}
		n.start(verNew)
	}
	waitLeader(t, nodes, 30*time.Second)
	m := newModel()
	runOps(t, raftWriter(nodes), m, 16, baseOps("pre", 1000, 0))
	syncRaft(t, nodes, "loaded")
	for round := 0; round < 3; round++ {
		lead := waitLeader(t, nodes, 30*time.Second)
		var target *node
		for _, n := range nodes {
			if n.ver == verNew && (n != lead || round == 2) {
				target = n
				break
			}
		}
		if fileExists(snapFile(target, verNew)) {
			t.Fatalf("%s has a snapshot; the scenario needs none", target.id)
		}
		target.stop()
		if err := target.tryStart(verOld, 20*time.Second); err != nil {
			t.Fatalf("v1.16 on %s's unwiped, snapshot-free new raft files: %v", target.id, err)
		}
		syncRaft(t, nodes, "downgraded-"+target.id)
		t.Logf("downgraded %s to v1.16 without a wipe", target.id)
	}
	verifyCluster(t, nodes, m, "after no-wipe downgrade (no snapshot)")
	checkNoPanics(t, nodes)
}

// TestRaftRestartIncrRepro: is the INCR double-apply seen in the no-snapshot
// scenarios a version problem or a plain restart problem? One raft node, no
// snapshot, 10 × INCR, clean restart; old→old, new→new and old→new.
func TestRaftRestartIncrRepro(t *testing.T) {
	requireEnabled(t)
	for _, c := range [][2]string{{verOld, verOld}, {verNew, verNew}, {verOld, verNew}} {
		t.Run(c[0]+"→"+c[1], func(t *testing.T) {
			dir := testDir(t)
			n := raftCluster(t, dir, "n1")[0]
			n.env = []string{"VELTRIX_RAFT_SNAPSHOT_THRESHOLD=0"}
			n.start(c[0])
			waitLeader(t, []*node{n}, 30*time.Second)
			var last string
			for i := 0; i < 10; i++ {
				last, _ = one(n.addr(), "INCR ctr 1")
			}
			n.stop()
			n.start(c[1])
			waitLeader(t, []*node{n}, 30*time.Second)
			syncRaft(t, []*node{n}, "restarted")
			got, _ := one(n.addr(), "GET ctr")
			t.Logf("INCR ctr ×10 → %s; after a clean restart %s→%s: GET ctr = %s", last, c[0], c[1], got)
			if got != "10" {
				t.Errorf("counter %s after restart %s→%s, want 10 (raft log re-applied onto the WAL-recovered engine)", got, c[0], c[1])
			}
		})
	}
}
