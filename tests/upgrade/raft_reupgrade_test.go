package upgrade_test

import (
	"strings"
	"testing"
	"time"
)

// TestRaftDowngradeReupgradeIncr: new build → v1.16 (no wipe, no snapshot) →
// new build, INCRs on each. v1.16 installs the new build's raft applied-index
// marker as a live key and its clean-shutdown checkpoint rewrites it at the
// top version; the re-upgraded node must not trust that copy (it would cut
// nothing and re-apply every entry v1.16 applied after it). With no trusted
// marker it resets the engine and applies the log once, so the counter is
// exactly the number of acknowledged INCRs — which also erases v1.16's own
// restart double-apply (TestRaftRestartIncrRepro old→old). The reserved key
// must not be visible on the re-upgraded node.
func TestRaftDowngradeReupgradeIncr(t *testing.T) {
	requireEnabled(t)
	dir := testDir(t)
	n := raftCluster(t, dir, "n1")[0]
	n.env = []string{"VELTRIX_RAFT_SNAPSHOT_THRESHOLD=0"}
	acked := 0
	incr := func(times int) {
		for i := 0; i < times; i++ {
			if r, err := one(n.addr(), "INCR ctr 1"); err == nil && isNumReply(r) {
				acked++
			} else {
				t.Fatalf("INCR: %q %v", r, err)
			}
		}
	}

	n.start(verNew)
	waitLeader(t, []*node{n}, 30*time.Second)
	incr(10)
	n.stop()

	if err := n.tryStart(verOld, 30*time.Second); err != nil {
		t.Fatalf("v1.16 on the new build's unwiped, snapshot-free data: %v", err)
	}
	waitLeader(t, []*node{n}, 30*time.Second)
	syncRaft(t, []*node{n}, "on-v1.16")
	onOld, _ := one(n.addr(), "GET ctr")
	incr(5)
	n.stop()
	if fileExists(snapFile(n, verOld)) {
		t.Fatal("v1.16 took a snapshot; the scenario needs none")
	}

	n.start(verNew)
	waitLeader(t, []*node{n}, 30*time.Second)
	syncRaft(t, []*node{n}, "re-upgraded")
	got, _ := one(n.addr(), "GET ctr")
	t.Logf("INCR ×10 on new, ×5 on v1.16 (GET on v1.16 after its restart: %s); after re-upgrade GET ctr = %s", onOld, got)
	if want := "15"; got != want {
		t.Errorf("counter %s after new → v1.16 → new, want %s (%d acknowledged INCRs)", got, want, acked)
	}
	kv, err := scanAll(n.addr())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	for k := range kv {
		if strings.Contains(k, "veltrix/raft-applied") {
			t.Errorf("reserved marker key %q is visible after the re-upgrade", k)
		}
	}
	for _, l := range n.grepLogs("raft-applied") {
		t.Logf("log: %s", l)
	}
}
