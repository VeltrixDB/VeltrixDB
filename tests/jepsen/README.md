# Jepsen-style consistency tests

`tests/jepsen` runs a 3-node VeltrixDB cluster of real server processes
under faults, records every client operation, and checks the history. It
follows the approach of [Jepsen](https://jepsen.io) (clients, a nemesis,
checkers), written in Go with the standard library only. Everything is in
`_test.go` files, so nothing here ships in a binary.

## Running

```bash
# Checker unit tests only (no server, a few seconds):
go test ./tests/jepsen/ -short -timeout 120s

# Default run: every mode, 20 s of workload each (~3.5 min in total on an M-series Mac):
go test ./tests/jepsen/ -v -timeout 15m

# One mode, a longer run, a fixed seed:
VELTRIX_JEPSEN_DURATION=5m VELTRIX_JEPSEN_SEED=42 \
  go test ./tests/jepsen/ -v -run TestJepsenRaftLinearizable -timeout 20m
```

| Variable | Default | Meaning |
|---|---|---|
| `VELTRIX_JEPSEN_DURATION` | `20s` | Workload length per test (Go duration). Cut automatically to fit `-timeout` |
| `VELTRIX_JEPSEN_SEED` | random, logged | Seeds the clients' op choices and the nemesis' fault schedule. Timing is not reproducible, so a seed reproduces the schedule, not the interleaving |
| `VELTRIX_JEPSEN_CHECK_TIMEOUT` | `60s` | Linearizability search budget per key. A key that runs out is reported as `unknown`, not as a failure |
| `VELTRIX_JEPSEN_OUT` | unset | Directory for histories; with it, every run (pass or fail) keeps `<Test>-<seed>/history.json` and the node logs |
| `VELTRIX_JEPSEN_KEEP` | unset | Keep the history of a passing run in the temp dir too |
| `VELTRIX_JEPSEN_KEEP_DATA` | unset | On failure, keep the nodes' data dirs |
| `VELTRIX_SERVER_BIN` | unset | Pre-built server binary; otherwise `./cmd/server` is built once per run |
| `VELTRIX_RAFT_PIPELINE` | `false` | `--raft-pipeline` on the raft nodes |
| `VELTRIX_JEPSEN_NEGATIVE` | unset | Enables `TestJepsenNegativeControl_StaleReads` (below) |
| `VELTRIX_JEPSEN_PARTITION_CMD` / `VELTRIX_JEPSEN_HEAL_CMD` | unset | Partition hook (below) |

`-short` skips every cluster test. Nodes listen on ports in [12000, 20000)
(below every OS's ephemeral range, and disjoint from `tests/integration`).

On failure the test prints each problem; for a linearizability violation it
prints where the search got stuck (the register's state after the longest
partial linearization and the ops none of which can follow) and the minimal
failing sub-history. The full history (`history.json`: every op with invoke /
complete times, the nemesis events, the findings), `minimal-<key>.json` and
the three server logs are written to `$VELTRIX_JEPSEN_OUT/<Test>-<seed>/`, or
a `veltrix-jepsen-out-*` temp dir whose path is printed.

## The harness

**Clients.** 8 clients, each on its own connection, using the text protocol.
Per op they choose read (`GET`), write (`PUT`, unique value), compare-and-swap
(`CAS key expected new`, usually expecting a recently seen value), increment
(`INCR`) or set add (`PUT` of a unique key under `s/`). Register keys are 5 per
*generation*, and a new generation starts every 5 s (`r<gen>.<k>`, counters
`c<gen>`): contention on few keys, but per-key histories stay short enough to
check. Each op records invoke and complete times and its outcome:

- **ok** — acknowledged (`OK`, `MISMATCH`, `NOTFOUND`, a number, a value).
- **fail** — definitely not applied: the connection could not be opened, or a
  read got an error (reads have no effect).
- **info** — unknown: a timeout, a connection error, any `ERR` reply to a
  write, and `MOVED` to a write. The raft server returns `MOVED` both before a
  write enters the log and after (a leader that steps down fails its pending
  commit waiters with `ErrNotLeader`, but a later leader may still commit the
  entry), so a `MOVED` write is not known to have failed. After an info op the
  client reconnects as a new process.

Reads follow `MOVED` to the named leader until their deadline (6 s). Writes
are never retried; the client sends its next op to the leader it was told.

**Nemesis** (one fault at a time; each heals before the next):

| Fault | What happens |
|---|---|
| `kill-leader`, `kill-random` | SIGKILL, down 0.5–3 s, restart on the same data dir |
| `pause-leader`, `pause-random` | SIGSTOP 1–4 s, then SIGCONT. On one host this stands in for a partition: peers time out, and a paused leader wakes up still believing it leads |
| `snapshot-crash` (raft) | Kill a follower; while it is down the leader compacts its log (`--raft-snapshot-threshold 100`, `--raft-snapshot-chunk-kb 4`); restart it with `VELTRIX_TEST_CRASH_AT_SNAPSHOT_STAGE=<random stage>` so it dies mid-InstallSnapshot (exit 86); restart it for real. If the stage is never reached, it is killed again 0–500 ms into its catch-up |
| `partition` | Only with the hook below |

After the workload the nemesis heals everything (SIGCONT, restart). For raft,
the harness then waits for a leader, commits a barrier write through it, and
reads every key from the leader; these *final reads* go into the history.

**Checkers** (all in this package, no dependencies):

- **Linearizability** (`linearizable_test.go`): per key, the Wing & Gong
  search with Lowe's memoisation of (linearized set, state) — the algorithm
  Porcupine and Knossos use. Models: a CAS register (read / write / CAS with
  ok / mismatch / notfound results) and a counter (INCR returns the new
  value). Info ops get a return time of +∞: they may take effect any time
  after their invocation, or never. Linearizability is local, so checking
  each key alone is exact. A violation is shrunk to the shortest failing
  prefix (sound: linearizability is prefix-closed) and then greedily stripped
  of reads and failed CASes that are not needed (sound: removing an
  observation only adds linearizations). Writes are never dropped. Before
  the full search, a reduced history without the info writes / CASes whose
  value nothing reads or expects is tried; "linearizable" there carries over
  (such an op can be placed last, i.e. never), any other verdict is
  re-checked on the full history.
- **Counter**: final value ∈ [Σ ok increments, Σ ok + Σ info increments];
  no two ok INCRs return the same value; an INCR that completed before
  another started returned a smaller value; the values below the largest ok
  result that no ok INCR returned are no more than the info INCRs. Counter
  keys also go through the WGL search, unless they have more than 12 info
  INCRs: info INCRs commute, so the search is exponential in their number,
  and such a key is covered by these checks alone (logged).
- **Set**: every acknowledged add is present at the end; every present key
  was written by some client (no value from nowhere).
- **Read validity** (where reads may be stale): every value read from a
  register key was written by an op invoked before the read completed that
  did not definitely fail.
- **Copies** (replicated): every acknowledged add is on at least N nodes.

## The tests

| Test | Configuration | Checked |
|---|---|---|
| `TestJepsenRaftLinearizable` | raft, `--linearizable-reads` | Every key linearizable including reads; counters; set |
| `TestJepsenRaftLocalReads` | raft, local reads (default), reads sent to random nodes | Writes, CAS, INCR and final leader reads linearizable; ordinary reads only for validity, since docs/replication.md states follower reads may be stale; counters; set; all nodes converge to the same keyspace |
| `TestJepsenReplicated/strong`, `/quorum` | `--mode=replicated`, kill / pause only | What docs/replication.md claims and no more: an acknowledged write is on RF (strong) or RF/2+1 (quorum) nodes, here checked once the cluster has healed; no set key or read value that no client wrote. Replicated mode is documented as not linearizable (no leader; replicas apply in arrival order), so there is no linearizability or CAS / INCR check, and convergence is logged, not asserted (a replica that was down while its primary restarted is documented never to catch up) |
| `TestJepsenNegativeControl_StaleReads` | raft, local reads, reads in the linearizability check | Opt-in. Must FIND a violation (stale reads from resumed followers): shows the harness detects real anomalies on a real server |
| `TestRecheckHistory` | none | Opt-in (`VELTRIX_JEPSEN_RECHECK=<history.json>`, `VELTRIX_JEPSEN_RECHECK_READS=0` to drop non-final reads): re-runs the linearizability check on a saved history |
| `TestChecker*`, `TestCheck*` | none (unit tests, run in `-short`) | Known linearizable and non-linearizable histories (stale reads, lost writes, double CAS, info writes and CASes applied 0 or 1 times, counter duplicates); 300 random histories linearizable by construction; 200 with one corrupted read that must be rejected and kept in the minimal sub-history; the shrinker; the search timeout; the counter, set, copies and read-validity checkers |

## Limitations

- **One host.** All nodes share one disk, one kernel and one clock; faults are
  process-level only.
- **No real network partitions on macOS.** Packet filtering needs root (`pfctl`
  on macOS, iptables / nftables on Linux). SIGSTOP covers "a node stops
  answering", but not a node that keeps running while cut off, nor
  asymmetric or partial partitions. The hook: set
  `VELTRIX_JEPSEN_PARTITION_CMD` and `VELTRIX_JEPSEN_HEAL_CMD`; the nemesis
  then adds a `partition` fault and runs them through `sh -c` with `NODE_ID`,
  `NODE_PORT`, `NODE_REPL_PORT` (P+1), `NODE_RAFT_PORT` (P+2),
  `NODE_GOSSIP_PORT` (P+3) and `OTHER_PORTS` set. On Linux as root:

  ```bash
  export VELTRIX_JEPSEN_PARTITION_CMD='for p in $NODE_RAFT_PORT $NODE_REPL_PORT; do iptables -I INPUT -i lo -p tcp --dport $p -j DROP -m comment --comment jepsen; done'
  export VELTRIX_JEPSEN_HEAL_CMD='iptables -S INPUT | grep jepsen | sed s/^-A/-D/ | while read r; do iptables $r; done'
  ```

  On loopback every node has the same IP, so a port rule can only drop
  connections *to* the node: its own outgoing raft / replication connections
  keep working, which makes this an asymmetric partition. For a symmetric
  one, run each node in its own network namespace or container (its own
  IP) and drop by address.
- **No clock skew.** Changing the clock needs root (`settimeofday`) or
  libfaketime; not injected. Raft here does not depend on clocks for safety
  (no leader leases), so the main exposure would be TTLs.
- **Durability is per process, not per machine.** SIGKILL loses nothing the
  OS page cache holds, and on macOS `fsync` does not flush the drive cache
  anyway (CLAUDE.md invariant 40): power loss is not simulated.
- **Bounded histories.** Keys rotate every 5 s so each key's history stays at
  a few hundred ops; a key whose search exceeds `VELTRIX_JEPSEN_CHECK_TIMEOUT`
  is reported as unknown. Longer runs make more keys, not longer ones.
- **Stale reads are not bounded** in `TestJepsenRaftLocalReads`: only their
  validity is checked.
- **Scope.** Single-key operations over the text protocol. Multi-key TXN,
  MPUT, namespaces, search and the binary protocol are not covered; the
  final keyspace reads use the binary `RANGE`.
