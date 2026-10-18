# Rolling-upgrade tests (v1.16 ↔ current build)

Real server processes, mixed binaries, the same `--data` dir across binary
swaps. Every test is skipped unless `VELTRIX_UPGRADE_TEST=1`.

```bash
# Build v1.16 from the tag (git archive, read-only) and this checkout, run all:
VELTRIX_UPGRADE_TEST=1 go test ./tests/upgrade/ -v -timeout 30m

# Reuse prebuilt binaries and keep data dirs + node logs in a known place:
VELTRIX_UPGRADE_TEST=1 \
VELTRIX_OLD_BIN=/path/veltrix-v116 VELTRIX_NEW_BIN=/path/veltrix-new \
VELTRIX_UPGRADE_WORKDIR=/tmp/veltrix-upgrade VELTRIX_UPGRADE_KEEP=1 \
  go test ./tests/upgrade/ -v -run TestUpgrade_RaftRolling -timeout 15m
```

| Variable | Meaning |
|----------|---------|
| `VELTRIX_UPGRADE_TEST=1` | enable the tests |
| `VELTRIX_OLD_BIN` | old server binary; otherwise `git archive $VELTRIX_OLD_REF \| tar -x` + `go build ./cmd/server` |
| `VELTRIX_OLD_REF` | tag to build when `VELTRIX_OLD_BIN` is unset (default `v1.16`) |
| `VELTRIX_NEW_BIN` | new server binary; otherwise `go build ./cmd/server` of this checkout |
| `VELTRIX_UPGRADE_WORKDIR` | binaries, data dirs and logs (default: a new temp dir) |
| `VELTRIX_UPGRADE_KEEP=1` | keep a passing test's data dirs and logs (a failing test always keeps them and prints the path) |

Each node writes one log per incarnation, `logs/<id>.<NN>.<old|new>.log`;
the first line is the exact command line. Nodes run in their own process
group and are SIGKILLed on test cleanup. If `go test -timeout` fires, the
cleanup does not run: `pkill -f veltrix-v116; pkill -f veltrix-new`.

Do not run two `go test ./tests/upgrade/` processes at the same time: each
reserves its 6-port blocks only inside its own process, so the other can take
a port in between (seen as `bind: address already in use` on startup).

## Scenarios

| Test | What it does | Checks |
|------|--------------|--------|
| `TestUpgrade_RaftRolling` | 3 × v1.16 raft. Keys, deletes, INCR counters, TTL keys (1 day, read back with `HTTL`) and 240 s TTL keys; filler writes until every node has `raft_snapshot.gob` (v1.16's threshold is fixed at 8192 entries); more writes that live only in the log tail. Then, with a writer running, the followers are swapped to the new binary one at a time (SIGTERM, same `--data`), then the leader. Then a new-format snapshot is captured and every node restarted once more. | all nodes key-for-key equal; every acknowledged write present, every acknowledged delete absent, counters within [acknowledged, acknowledged + unknown]; `HTTL` > 0 for every TTL key; `raft_snapshot.gob` migrated |
| `TestUpgrade_RaftSnapshotTTLRepro` | smallest reproduction of the TTL finding: one raft node, one TTL key, a v1.16 snapshot, restart on v1.16 (control) and on the new build | `HTTL` after the restart |
| `TestUpgrade_RaftRollingNoSnapshot` | the rolling upgrade on a cluster that never snapshotted (`VELTRIX_RAFT_SNAPSHOT_THRESHOLD=0` on the new build; under 8192 entries on v1.16) | as above |
| `TestRaftRestartIncrRepro` | control, not an upgrade test: one raft node, no snapshot, 10 × INCR, clean restart old→old / new→new / old→new | `GET` = 10 |
| `TestUpgrade_RaftOldFollowerSnapshot` | new leader + new follower + v1.16 follower. The v1.16 follower is stopped while keys it holds are deleted / updated and the leader compacts past it, then restarted | the leader logs `sent single-shot snapshot`; v1.16 follower key-for-key equal to the new nodes; TTL loss on the v1.16 follower is reported as the documented limitation |
| `TestUpgrade_RaftNewFollowerFromOldLeader` | v1.16 leader + v1.16 follower + new follower; the new follower misses deletes / updates / compaction and catches up through the v1.16 leader's single-shot InstallSnapshot | `installed snapshot` on the new follower; key-for-key equal; TTLs |
| `TestUpgrade_ReplicatedRolling` | 3 × v1.16 `--mode=replicated --consistency=quorum`, each node writing its own keys; nodes swapped one at a time. The writer of the node being swapped is stopped and its writes are confirmed on both peers first (drain); the other two keep writing while it is down | as for raft |
| `TestUpgrade_Standalone` | v1.16 → new on the same `--data`, after a clean stop and after SIGKILL; then more writes and a crash restart on the new build | WAL replays every acknowledged write / delete / counter; TTLs kept; 15 s TTL keys expire |
| `TestDowngrade_Standalone` | new → v1.16: the documented path (one run with `--wal-format=text-legacy`, clean stop), and directly (no text-legacy) after a clean stop and after SIGKILL | data intact on v1.16; TTLs kept on the direct path, dropped (documented) on the text-legacy path |
| `TestDowngrade_RaftNoSnapshotNoWipe` | new-build raft nodes that never snapshotted, started on v1.16 without a wipe (v1.16 already reads `raft_log.dat` / `raft_meta.dat`) | v1.16 starts, re-joins, key-for-key equal |
| `TestDowngrade_RaftNoWipeEmptyLog` | tries to produce a new-build node with `raft_snapshot.dat` and an EMPTY `raft_log.dat` (the one shape v1.16 cannot detect), then starts v1.16 on it; skips when the log is not empty after the install | v1.16 must refuse |
| `TestDowngrade_Raft` | 3 × new raft with snapshots. (a) v1.16 started on a new node's raft dir without a wipe — must refuse. (b) The documented path, followers first, leader last: stop, wipe `--data`, start v1.16 with the same id/peers, re-sync | (a) exits with an error; (b) key-for-key equal, every acknowledged write; TTL state reported |

"Acknowledged" means the server answered `OK` (or a number, for INCR).
A write that got an error, a timeout or a dropped connection has an unknown
outcome and is not checked; INCRs with an unknown outcome widen the allowed
counter range instead of being retried.

## Operational procedure the tests follow

- **Raft:** followers first, one at a time, leader last. After each swap the
  node must apply a marker written through the leader before the next node
  is touched.
- **Replicated:** one node at a time; stop sending writes to the node and
  wait until its writes are on every peer before stopping it (the retention
  buffer is in memory — a write applied locally and not yet shipped is lost
  to the peers if the process stops, upgrade or not); wait until every
  replica is `SYNC` in `TOPOLOGY` after it restarts.
- **Clean stop** is SIGTERM (the WAL checkpoint runs).
