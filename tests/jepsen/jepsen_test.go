package jepsen

// jepsen_test.go — the cluster tests. Each runs a 3-node cluster of real
// server processes for VELTRIX_JEPSEN_DURATION (default 20s) with seed
// VELTRIX_JEPSEN_SEED (default: random, logged). Skipped in -short mode.

import (
	"os"
	"strings"
	"testing"
	"time"
)

var raftFaults = []string{"kill-leader", "kill-random", "pause-leader", "pause-random", "snapshot-crash"}

func raftCluster(linReads bool) clusterConfig {
	return clusterConfig{
		mode:          "raft",
		linReads:      linReads,
		snapThreshold: 100, // compact often, so a restarted follower needs InstallSnapshot
		snapChunkKB:   4,   // multi-chunk transfers even for a small keyspace
		pipeline:      envBool("VELTRIX_RAFT_PIPELINE"),
	}
}

// TestJepsenRaftLinearizable: raft with --linearizable-reads must be
// linearizable — reads, writes, CAS and INCR per key — and keep every
// acknowledged write and increment.
func TestJepsenRaftLinearizable(t *testing.T) {
	runJepsen(t, runConfig{
		cluster: raftCluster(true),
		clients: 8, keys: 5, rotate: 5 * time.Second,
		faults: raftFaults,
		pRead:  35, pWrite: 20, pCAS: 25, pIncr: 10,
		checkReads: true,
		opTimeout:  6 * time.Second,
	})
}

// TestJepsenRaftLocalReads: raft without --linearizable-reads. Reads are
// served from local state and may be stale by design (docs/replication.md),
// so reads go to random nodes and are checked only for validity (no value
// from nowhere or from the future); writes, CAS, INCR and the final leader
// reads must still be linearizable, and followers must converge.
func TestJepsenRaftLocalReads(t *testing.T) {
	runJepsen(t, runConfig{
		cluster: raftCluster(false),
		clients: 8, keys: 5, rotate: 5 * time.Second,
		faults: raftFaults,
		pRead:  35, pWrite: 20, pCAS: 25, pIncr: 10,
		readsAnywhere: true,
		opTimeout:     6 * time.Second,
	})
}

// TestJepsenReplicated: replicated mode is documented as NOT linearizable
// (no leader, every node accepts writes, replicas apply in arrival order).
// What it claims: --consistency=strong acknowledges after all RF copies have
// the write, quorum after RF/2+1. So: every acknowledged add must be on
// at least that many nodes once the cluster has healed, nothing appears that
// no client wrote, and reads never return a value no client wrote.
// Convergence is reported, not asserted.
func TestJepsenReplicated(t *testing.T) {
	for _, tc := range []struct {
		consistency string
		copies      int
	}{{"strong", 3}, {"quorum", 2}} {
		t.Run(tc.consistency, func(t *testing.T) {
			runJepsen(t, runConfig{
				cluster: clusterConfig{mode: "replicated", consistency: tc.consistency},
				clients: 8, keys: 5, rotate: 5 * time.Second,
				faults: []string{"kill-random", "pause-random"},
				pRead:  30, pWrite: 20, pCAS: 0, pIncr: 0, // the rest: set adds
				opTimeout:        12 * time.Second, // > the 10 s replication timeout
				replicatedCopies: tc.copies,
			})
		})
	}
}

// TestJepsenNegativeControl_StaleReads checks the harness end to end against
// a real server: raft WITHOUT --linearizable-reads, reads sent to random
// nodes, and the reads put into the linearizability check — a property this
// configuration documents it does not have. Paused followers serve stale
// reads once resumed, so the checker must report a violation. Opt-in
// (VELTRIX_JEPSEN_NEGATIVE=1): a quiet run can, rarely, see no stale read.
func TestJepsenNegativeControl_StaleReads(t *testing.T) {
	if !envBool("VELTRIX_JEPSEN_NEGATIVE") {
		t.Skip("set VELTRIX_JEPSEN_NEGATIVE=1 to run the negative control")
	}
	runJepsen(t, runConfig{
		cluster: raftCluster(false),
		clients: 8, keys: 3, rotate: 5 * time.Second,
		faults: []string{"pause-random", "kill-random"},
		pRead:  50, pWrite: 40, pCAS: 0, pIncr: 0,
		readsAnywhere: true,
		checkReads:    true,
		opTimeout:     6 * time.Second,
		negative:      true,
	})
}

// TestRecheckHistory re-runs the linearizability check on a saved
// history.json (VELTRIX_JEPSEN_RECHECK=path). VELTRIX_JEPSEN_RECHECK_READS=0
// drops non-final reads, as TestJepsenRaftLocalReads does.
func TestRecheckHistory(t *testing.T) {
	path := os.Getenv("VELTRIX_JEPSEN_RECHECK")
	if path == "" {
		t.Skip("set VELTRIX_JEPSEN_RECHECK=<history.json>")
	}
	ops, err := LoadHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	reads := os.Getenv("VELTRIX_JEPSEN_RECHECK_READS") != "0"
	timeout := envDuration(t, "VELTRIX_JEPSEN_CHECK_TIMEOUT", 60*time.Second)
	for k, kops := range byKey(ops) {
		m := RegisterModel
		switch {
		case strings.HasPrefix(k, "c"):
			m = CounterModel
		case !strings.HasPrefix(k, "r"):
			continue
		}
		r := CheckKey(m, k, kops, reads, timeout)
		switch r.Verdict {
		case NotLinearizable:
			t.Error(FormatResult(r))
		default:
			t.Logf("%s: %v (%d ops, %d info, %v)", k, r.Verdict, r.Ops, r.Info, r.Elapsed.Round(time.Millisecond))
		}
	}
}
