package upgrade_test

// standalone_upgrade_test.go — one node, same --data, binary swapped:
//
//	TestUpgrade_Standalone     v1.16 → new after a clean stop and after a
//	                           crash (SIGKILL): WAL replays, TTLs survive.
//	TestDowngrade_Standalone   new → v1.16 by the documented path (one run
//	                           with --wal-format=text-legacy, clean stop), and
//	                           directly (no text-legacy step) after a clean
//	                           stop and after a crash.

import (
	"testing"
	"time"
)

func directWriter(n *node) func() writer {
	return func() writer { return &directClient{n: n} }
}

const standaloneShortTTL = 15 // seconds: the exp/ keys must be gone after this

// verifyNode checks one node against the model, waiting for the short-TTL
// keys to expire first so an immortalised key is caught.
func verifyNode(t *testing.T, n *node, m *model, label string, wantTTL bool) {
	t.Helper()
	m.mu.Lock()
	var latest time.Time
	for _, e := range m.expKeys {
		if e.After(latest) {
			latest = e
		}
	}
	m.mu.Unlock()
	if d := time.Until(latest.Add(3 * time.Second)); d > 0 {
		time.Sleep(d)
	}
	m.mu.Lock()
	m.immortalOK = !wantTTL
	m.mu.Unlock()
	kv, err := scanAll(n.addr())
	if err != nil {
		t.Fatalf("%s: scan: %v", label, err)
	}
	reportAll(t, m.check(label, kv))
	p := m.ttlProblems(label, n.addr())
	if wantTTL {
		reportAll(t, p)
	} else if len(p) > 0 {
		t.Logf("%s: TTLs (expected lost on this path):\n%s", label, joinLines(p))
	}
	if !t.Failed() {
		t.Logf("%s: %d keys, all acknowledged writes, deletes and counters present; TTLs kept=%v", label, len(kv), len(p) == 0)
	}
}

func TestUpgrade_Standalone(t *testing.T) {
	requireEnabled(t)
	for _, crash := range []bool{false, true} {
		name := "clean-stop"
		if crash {
			name = "crash"
		}
		t.Run(name, func(t *testing.T) {
			dir := testDir(t)
			n := newNode(t, dir, "s1", nil)
			n.start(verOld)
			m := newModel()
			a, u := runOps(t, directWriter(n), m, 8, baseOps("s", 3000, standaloneShortTTL))
			t.Logf("v1.16 load: %d acknowledged, %d unknown", a, u)
			if crash {
				n.kill()
				t.Logf("v1.16 killed (SIGKILL): full WAL replay on the next start")
			} else {
				n.stop()
			}
			n.start(verNew)
			verifyNode(t, n, m, name+": new build on v1.16 data", true)

			// The new build keeps writing on the upgraded dir, and a second
			// restart (now on a mixed text/binary WAL checkpoint) holds.
			runOps(t, directWriter(n), m, 8, baseOps("s2", 500, 0))
			n.kill()
			n.start(verNew)
			verifyNode(t, n, m, name+": new build after crash restart", true)
			checkNoPanics(t, []*node{n})
		})
	}
}

func TestDowngrade_Standalone(t *testing.T) {
	requireEnabled(t)
	cases := []struct {
		name       string
		textLegacy bool // the documented step: one run with --wal-format=text-legacy
		crash      bool // how the new build was stopped before v1.16 starts
	}{
		{"documented-text-legacy", true, false},
		{"direct-clean-stop", false, false},
		{"direct-after-crash", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := testDir(t)
			n := newNode(t, dir, "s1", nil)
			n.start(verNew)
			m := newModel()
			runOps(t, directWriter(n), m, 8, baseOps("s", 3000, standaloneShortTTL))
			if tc.textLegacy {
				n.stop()
				n.args = []string{"-wal-format", "text-legacy"}
				n.start(verNew)
				n.stop() // the clean-shutdown checkpoint rewrites wal.log as text
				n.args = nil
			} else if tc.crash {
				n.kill()
			} else {
				n.stop()
			}
			if err := n.tryStart(verOld, 30*time.Second); err != nil {
				t.Fatalf("v1.16 does not start on the downgraded dir: %v", err)
			}
			// Docs: text-legacy drops TTLs (keys come back immortal).
			verifyNode(t, n, m, tc.name+": v1.16 on new-build data", !tc.textLegacy)
			checkNoPanics(t, []*node{n})
		})
	}
}
