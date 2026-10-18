package jepsen

// nemesis_test.go — the fault injector. One fault at a time, so a majority
// is always intact once the fault heals; every injection and heal is
// recorded in the history.
//
//	kill-leader / kill-random   SIGKILL, down 0.5–3 s, restart on the same data dir
//	pause-leader / pause-random SIGSTOP 1–4 s, then SIGCONT. A paused process
//	                            stands in for a partition on one host: its
//	                            peers time out on it, and a paused leader
//	                            wakes up still believing it leads.
//	snapshot-crash (raft)       kill a follower, let the leader compact its log
//	                            past it (low --raft-snapshot-threshold), restart
//	                            it with VELTRIX_TEST_CRASH_AT_SNAPSHOT_STAGE so it
//	                            exits mid-InstallSnapshot, then restart it for
//	                            real — or, if it never reached the stage, kill it
//	                            again 0–500 ms after starting
//	partition                   only with a Partitioner (partition_test.go);
//	                            none exists on macOS without root
//
// Clock skew is not injected: it needs root (settimeofday) or libfaketime.

import (
	"fmt"
	"math/rand"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

var snapshotStages = []string{"install-verified", "install-renamed", "install-log-rewritten", "fsm-restore-partial", "install-restored"}

type nemesis struct {
	c      *cluster
	h      *History
	rng    *rand.Rand
	faults []string
	part   Partitioner

	mu     sync.Mutex
	counts map[string]int
	errs   []string
}

func newNemesis(c *cluster, h *History, seed int64, faults []string, part Partitioner) *nemesis {
	return &nemesis{c: c, h: h, rng: rand.New(rand.NewSource(seed)), faults: faults, part: part, counts: map[string]int{}}
}

func (nm *nemesis) count(k string) {
	nm.mu.Lock()
	nm.counts[k]++
	nm.mu.Unlock()
}

func (nm *nemesis) fail(format string, a ...interface{}) {
	nm.mu.Lock()
	nm.errs = append(nm.errs, fmt.Sprintf(format, a...))
	nm.mu.Unlock()
}

func (nm *nemesis) summary() string {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	var ks []string
	for k, v := range nm.counts {
		ks = append(ks, fmt.Sprintf("%s=%d", k, v))
	}
	sort.Strings(ks)
	return strings.Join(ks, " ")
}

func (nm *nemesis) dur(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(nm.rng.Int63n(int64(hi-lo)+1))
}

// sleep waits d or until stop closes; false when stopped.
func sleepOr(d time.Duration, stop <-chan struct{}) bool {
	select {
	case <-time.After(d):
		return true
	case <-stop:
		return false
	}
}

func (nm *nemesis) randomUp() *node {
	var up []*node
	for _, n := range nm.c.nodes {
		if n.isUp() {
			up = append(up, n)
		}
	}
	if len(up) == 0 {
		return nil
	}
	return up[nm.rng.Intn(len(up))]
}

func (nm *nemesis) leaderOrRandom() *node {
	if nm.c.cfg.mode == "raft" {
		if l := nm.c.leader(); l != nil {
			return l
		}
	}
	return nm.randomUp()
}

// run injects faults until stop closes, leaving the cluster healed.
func (nm *nemesis) run(stop <-chan struct{}) {
	defer nm.heal()
	if !sleepOr(nm.dur(1*time.Second, 2*time.Second), stop) {
		return
	}
	for {
		f := nm.faults[nm.rng.Intn(len(nm.faults))]
		nm.inject(f, stop)
		if !sleepOr(nm.dur(500*time.Millisecond, 2500*time.Millisecond), stop) {
			return
		}
	}
}

func (nm *nemesis) restart(n *node, why string) {
	if err := n.start(false); err != nil {
		nm.fail("restart %s (%s): %v", n.id, why, err)
		return
	}
	if err := n.waitReady(30 * time.Second); err != nil {
		nm.fail("restart %s (%s): %v\n%s", n.id, why, err, n.logTail(30))
		return
	}
	nm.h.Nemesis("restart", n.id, why)
}

func (nm *nemesis) inject(f string, stop <-chan struct{}) {
	switch f {
	case "kill-leader", "kill-random":
		n := nm.randomUp()
		if f == "kill-leader" {
			n = nm.leaderOrRandom()
		}
		if n == nil {
			return
		}
		nm.h.Nemesis("kill", n.id, f)
		n.kill()
		nm.count(f)
		sleepOr(nm.dur(500*time.Millisecond, 3*time.Second), stop)
		nm.restart(n, f)

	case "pause-leader", "pause-random":
		n := nm.randomUp()
		if f == "pause-leader" {
			n = nm.leaderOrRandom()
		}
		if n == nil {
			return
		}
		if err := n.pause(); err != nil {
			return
		}
		nm.h.Nemesis("pause", n.id, f)
		nm.count(f)
		sleepOr(nm.dur(1*time.Second, 4*time.Second), stop)
		if err := n.resume(); err == nil {
			nm.h.Nemesis("resume", n.id, f)
		}

	case "snapshot-crash":
		l := nm.c.leader()
		if l == nil {
			return
		}
		var fol *node
		for _, n := range nm.c.nodes {
			if n != l && n.isUp() {
				fol = n
				break
			}
		}
		if fol == nil {
			return
		}
		nm.h.Nemesis("kill", fol.id, "snapshot-crash: down while the leader compacts")
		fol.kill()
		// Down long enough for the leader to compact past it at the
		// workload's write rate.
		sleepOr(nm.dur(2*time.Second, 4*time.Second), stop)
		stage := snapshotStages[nm.rng.Intn(len(snapshotStages))]
		if err := fol.start(true, "VELTRIX_TEST_CRASH_AT_SNAPSHOT_STAGE="+stage); err != nil {
			nm.fail("snapshot-crash start %s: %v", fol.id, err)
			return
		}
		nm.h.Nemesis("start", fol.id, "crash hook at "+stage)
		err, exited := fol.waitExit(10 * time.Second)
		switch {
		case exited:
			if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 86 {
				nm.h.Nemesis("crashed", fol.id, "mid-InstallSnapshot at "+stage)
				nm.count("snapshot-crash-at-" + stage)
			} else {
				nm.fail("snapshot-crash: %s exited with %v, want the injected crash (86)\n%s", fol.id, err, fol.logTail(40))
			}
		default:
			// No install reached the stage (the follower caught up from the
			// log, or the transfer is still running): restart it anyway,
			// usually mid-transfer.
			sleepOr(nm.dur(0, 500*time.Millisecond), stop)
			nm.h.Nemesis("kill", fol.id, "snapshot-crash: stage "+stage+" not reached; kill during catch-up")
			fol.kill()
			nm.count("snapshot-crash-missed")
		}
		nm.restart(fol, "snapshot-crash")

	case "partition":
		if nm.part == nil {
			return
		}
		n := nm.randomUp()
		if n == nil {
			return
		}
		var others []*node
		for _, o := range nm.c.nodes {
			if o != n {
				others = append(others, o)
			}
		}
		if err := nm.part.Isolate(n, others); err != nil {
			nm.fail("partition %s: %v", n.id, err)
			return
		}
		nm.h.Nemesis("partition", n.id, nm.part.Name())
		nm.count(f)
		sleepOr(nm.dur(2*time.Second, 6*time.Second), stop)
		if err := nm.part.Heal(); err != nil {
			nm.fail("heal partition: %v", err)
		}
		nm.h.Nemesis("heal-partition", n.id, "")
	}
}

// heal resumes every paused node, restarts every stopped one and heals any
// partition.
func (nm *nemesis) heal() {
	if nm.part != nil {
		_ = nm.part.Heal()
	}
	for _, n := range nm.c.nodes {
		n.mu.Lock()
		paused := n.running && n.paused
		n.mu.Unlock()
		if paused {
			_ = n.resume()
			nm.h.Nemesis("resume", n.id, "heal")
		}
		if !n.isRunning() {
			nm.restart(n, "heal")
		}
	}
}
