# Node Lifecycle: Failover, Addition, Removal, and Crash Handling

This document explains exactly what happens at each stage of a node's life in a VeltrixDB cluster — from joining to leaving, and from graceful shutdown to sudden crash.

**What `cmd/server` does vs. the library.** A server starts from its persisted membership when it has one, else from `--peers id@host:port,...`, and members are added and removed **online** through `POST` / `DELETE /admin/cluster/members` (CLI: `veltrix cluster add-node` / `remove-node`) — see the runbook below and CLAUDE.md invariant 67. Gossip still does not carry membership: changes go through the raft log (`--mode=raft`) or a versioned membership document every node persists (`--mode=replicated`). In `--mode=replicated`, `--auto-rebalance` (default true) reacts to membership changes (a node added to or removed from the map) and, after a 3 s debounce, runs `Rebalance` + `MigrateToNewOwners` (`cmd/server/rebalancer.go`, retried with backoff when a destination is not reachable yet); node state changes (SUSPECT / FAILED / RECOVERING / ACTIVE) never start a migration. In `--mode=raft` the flag is a no-op: every node holds the full raft-replicated state, so the transfer agent is marked raft-managed, `MigrateToNewOwners` refuses (`ErrMigrationRaftMode`) and inbound `/transfer/keys` batches get HTTP 403 (invariant 62; before this, killing the leader of a loaded 3-node raft cluster deleted about two thirds of the keys on each survivor). The library calls further down (`AddNodeAndRebalance`, `RemoveNodeAndRebalance`, `ForceRemoveNode`, `RaftNode.AddMember`) are what the server uses underneath; an embedding program can call them directly.

---

## Adding and Removing Nodes Online (runbook)

Every membership endpoint is on the admin/metrics listener with the same auth as every other `/admin/*` endpoint (`--admin-token`, else loopback only):

| | |
|--|--|
| `GET /admin/cluster/members` / `veltrix cluster members` | members, roles (`voter` / `learner` in raft, `member` in replicated), version, leader or coordinator, this node's status (`voter`, `learner`, `member`, `joining`, `draining`, `removed`) |
| `POST /admin/cluster/members` `{"id":"n4","addr":"10.0.0.14:9000","wait_ms":60000}` / `veltrix cluster add-node n4@10.0.0.14:9000` | add a node; `addr` is its client `--addr` (the other listeners are derived from it: +1 replication, +2 raft, +3 gossip, +5 transfer) |
| `DELETE /admin/cluster/members/n2[?force=true&wait_ms=N]` / `veltrix cluster remove-node n2 [--force]` | remove a node |

Answers: `200` done, `202` raft learner added but not promoted yet (it keeps catching up; poll `members`), `400` bad request / last voter, `404` unknown id, `409` duplicate id, a change already in progress (changes serialise: retry), a concurrent replicated change, or a failed drain, `503` no reachable quorum / no leader / coordinator unreachable. Any node accepts a request and forwards it to the raft leader or the replicated-mode coordinator over the transfer listener; forwarding, `--join` and every replicated-mode change are **node-to-node traffic and need `--cluster-secret-file` (or `--cluster-mtls`) on every node** (HMAC-signed, invariant 56). Without one, a raft change still works when sent to the leader's admin endpoint; nothing else does.

Change one node at a time.

### Raft: add a node

1. Start the new node **empty** with `--join` instead of (or in addition to) `--peers`:
   ```bash
   veltrixdb --mode=raft --node-id=n4 --addr=10.0.0.14:9000 --data=/data \
     --join=10.0.0.11:9000,10.0.0.12:9000 --cluster-secret-file=/etc/veltrix/secret
   ```
   (`--advertise-addr=host:port` when `--addr` has no host, e.g. `:9000`.) A `--join` node starts with an **empty raft configuration**: it is not a voter in its own view, so it never campaigns and votes for nobody — an empty node can never form a cluster of its own or win an election with an empty log. It asks the listed members to add it (they forward to the leader) and retries every 2 s until a configuration that includes it arrives. `--join` is ignored on a node that already has persisted membership or is named in `--peers` (a bootstrap member), so one pod template can carry both flags.
2. The leader appends `n4` as a **learner**: it is replicated to — log entries, or a chunked InstallSnapshot when the leader already compacted what it needs — but it is not counted toward any quorum, so a slow or empty newcomer never stalls commits.
3. When a catch-up round ends within 1 s (its matchIndex reached the leader's last index as of the round's start), the leader **promotes** it to voter with a second configuration entry. A leader change in between does not strand it: learners are part of the persisted configuration and any leader promotes them.
4. Without a cluster secret, add it explicitly instead: `veltrix --addr <leader-admin> cluster add-node n4@10.0.0.14:9000` (the call returns once it is a voter, or `202` after `wait_ms`).
5. Check: `veltrix cluster members` on any node lists `n4  voter`, `committed=true`. Every node has updated its raft transport, MOVED redirects, `TOPOLOGY` and partition map (voters only) from the configuration, which carries each member's client address. Raft mode moves no keys: the new node gets everything through the log.

### Raft: remove a node

`veltrix cluster remove-node n2` (any node; forwarded to the leader). One configuration entry; nothing is migrated. The removed node gets no further entries — stop it, and wipe its data before it is ever re-added (`--join`).

* **Removing the leader** is allowed: it keeps leading, without counting itself, until the removal commits under the new configuration's quorum, then steps down; the remaining voters elect a new leader (one election timeout plus the pre-vote round, about 0.5–1.5 s).
* **Refused** with `503` when the voters that would remain do not form a quorum the leader heard from in the last 1.6 s (e.g. removing a healthy node from a 3-node cluster whose third node is down would leave two voters that need each other), and with `400` for the last voter.
* **A removed node that keeps running, or is restarted** on its old data and flags, may never have seen its removal and still think it is a voter. It cannot disrupt the cluster: elections start with a **pre-vote** that members refuse for a server outside their configuration (or while they hear from a leader), so it never increments its term, and real vote requests from non-members are refused before members look at the term.

### Replicated: add a node

1. Start it empty: `--mode=replicated --node-id=r4 --addr=10.0.0.24:9000 --join=10.0.0.21:9000 --cluster-secret-file=... --consistency=<same>`. Until it is added it is `joining` and refuses writes.
2. The membership coordinator (the member with the lowest ID) writes membership version v+1 and pushes it to every member; each persists it (`<--data>/cluster_membership.json`) and applies it:
   * its replication engine registers `r4` as a replica that first receives a catch-up of **every write this member originated** (its change feed: current values and tombstones), then live batches — writes made meanwhile are retained and replayed after the catch-up, in order (invariant 61);
   * its partition map gains `r4`, and the auto-rebalancer copies **every key** it holds to `r4` (in this mode the placement RF is every member: the replication engine sends every write to every member and reads are local). Copies are applied **put-if-absent** and marked as peer writes, so a copy never overwrites a newer write or resurrects a delete that the replication stream delivered, and never re-enters `r4`'s own catch-up feed.
3. Done when every member shows `r4` `SYNC` (`veltrix replication`, `resync_pending=false`) and logged `[transfer] migration done`; `r4` then serves every key. Cost: each member sends every key it holds (put-if-absent drops the duplicates).

### Replicated: remove a node

`veltrix cluster remove-node r2` (any node; forwarded to the coordinator):

1. **Drain**: `r2` refuses new writes ("draining") and the coordinator waits (default 2 min, `--wait-ms`) until every write `r2` originated has been acknowledged by every replica (`ReplicationEngine.Drained`). If that does not happen, nothing is removed (`409`) and `r2` serves writes again.
2. Version v+1 without `r2` is pushed; members drop it from replication (recovery state rewritten at once, its tombstone-GC watermark forgotten) and from the partition map.
3. `r2` sees itself removed: it refuses writes from then on (also after a restart), and its own migration sends every key it holds to the remaining members (put-if-absent) and deletes each locally only after all of them acknowledged it (invariant 62).

Move clients off `r2` before removing it. The drain needs every one of `r2`'s replicas to be `SYNC`, so while another member is down nothing can be drained: remove (or bring back) the dead member first. **A dead node** cannot be drained: `--force` / `?force=true` removes it without the drain — writes it acknowledged but never replicated (possible with `--consistency=eventual`) are lost. When the dead node is the coordinator itself, the next-lowest ID runs its forced removal.

Replicated mode has no consensus: changes are serialised by the coordinator, which first checks that no reachable member already has a newer version (`409` otherwise). A member that missed a push (down, partitioned) adopts newer versions at startup — before it registers any replica — and every 10 s from the members it knows; two versions with the same number (two coordinators racing while a version was still propagating) resolve deterministically to the one written by the higher coordinator ID.

### Where membership is stored, and what wins

| Mode | Persisted in | Precedence |
|------|--------------|------------|
| raft | configuration entries in `raft/raft_log.dat`; after compaction in the `raft/raft_snapshot.dat` header | Once any online change was made, the persisted configuration wins over `--peers` on every restart. A cluster that never changed membership online keeps following `--peers` (the bootstrap list), also after snapshots. |
| replicated | `<--data>/cluster_membership.json` (version, coordinator, members), written by the first online change | Once the file exists it wins over `--peers`. |

`--join` only applies to a node with no persisted membership that is not named in `--peers`.

### Recovering from a bad membership (dangerous)

* **Raft, quorum permanently lost** (e.g. two of three voters destroyed): stop every survivor; start **one** of them with `--force-new-cluster`. It appends a configuration holding only itself in a new term, elects itself and commits its whole log — including entries that were never committed. Remove the flag once it is up, then wipe every other former member and add it back with `--join`. Never force two nodes (two clusters), and never force while the old quorum may still be running (divergent histories).
* **Replicated**: `--force-new-cluster` moves `cluster_membership.json` aside (`.reset-<unix time>`) and starts from `--peers`. Do it on every node with the same `--peers`, otherwise they disagree; the next online change writes a new file.
* A `cluster_membership.json` that does not parse stops the node at startup with this advice.

### Kubernetes

The operator (VeltrixDB-Kubernetes-Operator) still scales by changing `--peers` in the pod template, which restarts every pod. That only works while the cluster never changed membership online (persisted membership wins afterwards). The intended use of this API: keep `--peers` at the bootstrap members, add `--join=<bootstrap pod addresses>` and `--advertise-addr=$(POD_FQDN):<port>` to every pod (both are ignored where they do not apply), scale up by raising the replica count only — new pods join by themselves (or the operator calls `POST /admin/cluster/members`) — and scale down by calling `DELETE /admin/cluster/members/<pod>` for the highest ordinal, waiting for `200`, then lowering the replica count and deleting that pod's volume.

---

## Node Failover (Leader Goes Down)

**Scenario**: The current Raft leader (e.g., `node-1`) becomes unavailable — either crashed, OOM-killed, or network-isolated.

### Timeline

```
T=0 ms   Leader (node-1) stops sending heartbeats
T=50 ms  Followers expect heartbeat every 50 ms — none arrives
T=400 ms Follower election timer fires (randomized 400–800 ms)
         First follower to fire becomes candidate

T=400 ms node-2 startElection():
         ├─ ps.CurrentTerm++    (term 2)
         ├─ ps.VotedFor = "node-2"
         ├─ role = Candidate
         ├─ persist term + vote → raft_meta.dat (before any RequestVote)
         ├─ resetElectionTimer()
         └─ Send RequestVote{term=2, lastLogIndex=X, lastLogTerm=Y}
                  to node-3 (and node-1, which may be dead)

T=401 ms node-3 receives RequestVote:
         ├─ term 2 > currentTerm 1 → becomeFollower(2)
         ├─ node-2 log is up-to-date (§5.4.1 check passes)
         ├─ VoteGranted = true
         └─ resetElectionTimer()

T=401 ms node-2 receives VoteGranted from node-3:
         votes = 2 (self + node-3) ≥ quorum (3/2 + 1 = 2)  →  becomeLeader()

T=401 ms node-2 becomeLeader():
         ├─ role = Leader
         ├─ LeaderID = "node-2"
         ├─ nextIndex[node-3] = lastLogIndex + 1
         ├─ matchIndex[node-3] = 0
         ├─ Append no-op entry (term=2)   ← Raft §5.4.2
         └─ append no-op to raft_log.dat, fsync, then broadcastAppendEntries()
                                           ← sent at once, includes no-op

T≈402 ms node-2's no-op entry ACKed by node-3 (quorum) — one round trip
         ├─ maybeAdvanceCommit() → commitIndex advances past no-op
         └─ New leader ready to accept client writes
```

### What Clients Experience

- **Writes to a non-leader**: answered `MOVED <leader-addr> <leader-id>`, or `MOVED - (leader unknown, retry)` while no leader is known (`cmd/server/coordinator.go`); writes to the dead old leader simply fail to connect. Clients retry with backoff until they reach the new leader.
- **New writes to `node-2`**: Accepted and committed normally once it is elected (~400–800 ms after the last heartbeat) plus one round trip for the no-op.
- **Reads from followers**: served from local state (whatever the follower has applied). Stale reads are possible if a follower hasn't received the latest commits yet. With `--linearizable-reads`, followers redirect every read (not only GET) to the leader, which runs a ReadIndex fence.
- **Reads from the new leader**: held until the new leader has applied its term's no-op (typically one heartbeat round, bounded by 2 s), so they include every write the old leader acknowledged. Before this barrier a GET immediately after failover could return "not found" for an acknowledged write (`TestRaftClusterFailover`).
- **Searches**: fail with `search incomplete: ... did not answer` naming the dead node until the failure detector marks it failed, then run on the remaining nodes (`--search-allow-partial` answers without it straight away).

### No Data Loss

Any write that received an `OK` response from the old leader was committed by quorum — at least 2 of 3 nodes fsynced it to `raft_log.dat`. The new leader will apply those entries before accepting new writes.

Writes that received `MOVED` were not submitted. Writes that timed out or lost their connection may or may not have been committed. Clients with **at-least-once** semantics should retry with idempotent operations (use `SetIfNotExists` / `CompareAndSwap` for exactly-once semantics).

### The No-Op Entry (Why It Matters)

When `node-2` becomes leader, it cannot immediately commit any uncommitted entries from term 1 — Raft §5.4.2 prohibits committing prior-term entries directly. The no-op entry in term 2 propagates through the cluster; once committed, it implicitly commits all prior-term entries that precede it. Until the no-op commits (one replication round trip) the new leader holds local reads (`WaitLeaderApplied`, up to 2 s).

---

## Node Addition

**Scenario**: Adding `node-4` to an existing 3-node cluster.

### Phase 1: Register the Node

```go
pm.AddNodeAndRebalance("node-4", "10.0.0.4", 9000, "10.0.0.4:9005", 256, ta)
```

(`cmd/server` derives the transfer listener as client port + 5, e.g. `:9005`; `--transfer-addr` overrides it locally.) This runs, on the partition map it is called on:

```
1. pm.AddNodeWithTransfer("node-4", "10.0.0.4", 9000, "10.0.0.4:9005")
   ├─ Node{state=ACTIVE} added to Nodes map
   ├─ 64 virtual nodes added to ConsistentHashRing
   ├─ pm.Version++, pm.epoch++ (split-brain fencing)
   └─ transfer address registered

2. pm.Rebalance(256)
3. go ta.MigrateToNewOwners()      (only if ta != nil)
```

Gossip does **not** propagate the new member: every node's partition map must have `AddNode("node-4", ...)` called on it — `cmd/server` does that on every node when it applies a membership change (runbook above). Once node-4 is known, gossip carries its liveness, gossip address and rack.

### Phase 2: Rebalance (Partition Reassignment)

```
pm.Rebalance(256)
    │
    ▼
256 partitions reassigned round-robin over the ACTIVE nodes (sorted by ID):
    Before: node-1, node-2, node-3 each own 85–86 partitions primary
    After:  node-1, node-2, node-3, node-4 each own 64 partitions primary
    (no data has moved yet)

Key ownership itself comes from the consistent-hash ring, not this table:
node-4's 64 vnodes take roughly a quarter of the key space.
```

### Phase 3: Data Migration (Background)

```
TransferAgent.MigrateToNewOwners()   [runs in background goroutine]
    │
    ▼
ScanKeys() → enumerate all live keys on the node that owns ta
    │
    ▼
For each key (placement = GetReplicasForKey: RF ring successors, rack-aware;
              @vec/@txt/@idx keys route as their record):
    now  = replicas under the current membership
    prev = replicas under the agent's baseline membership (before node-4)
    if local node ∈ now:  copy to now − prev (here: node-4, if it became a replica)
    else:                 send to every member of now; delete locally only
                          after all of them acknowledged
    (@vecns/ and @idxdef/ keys are copied to every destination, never moved)

    ▼
Fan out to each destination in parallel via HTTP:
    POST http://10.0.0.4:9005/transfer/keys
    Body: {src:"node-1", epoch:E, keys:[{k,v,ttl}, ...]}  (500 keys per batch)
    (HMAC-signed with --cluster-secret-file; a stale epoch gets 409)
    │
    ▼
node-4 receives and puts each key into its local StorageEngine
    │
    ▼
node-1 deletes the keys it is no longer a replica of, once every replica acked them
```

With RF=3, node-1 stops being a replica only of keys for which node-4 took one of the three slots from it; the rest are copied to node-4 and kept. (With 3 nodes and RF=3 every node is a replica of every key and nothing would ever move.)

**During migration**, the ring routes requests for re-owned keys to `node-4` as soon as it is added, but a key is only on `node-4` once its batch has been delivered: until then a read routed to `node-4` can miss it. `node-1` keeps its copy until every replica acknowledged it (failed batches are not deleted and are retried on the next migration).

### Phase 4: Raft Group Expansion

In raft mode none of the phases above run (every node already holds everything); the change is the raft configuration. On the leader:

```go
err := raftNode.AddMember("node-4", "10.0.0.4:9000", knownAddrs, 60*time.Second)
// 1. appends a configuration with node-4 as a LEARNER and waits for it to
//    commit (learners are replicated to but never counted toward quorum);
// 2. waits until the leader promotes it to VOTER (a catch-up round ≤ 1 s)
//    and that entry commits.  ErrPromotionPending after the wait: it is a
//    learner and is still being promoted.  ErrNotLeader, ErrMemberExists,
//    ErrConfigChangeInProgress, ErrNoQuorum otherwise.
err = raftNode.RemoveMember("node-2") // ErrMemberNotFound, ErrLastVoter, ErrNoQuorum
```

`AddServer` / `RemoveServer` are the older idempotent wrappers (nil when the member already is / is not one). `AddPeer` is deprecated: it changes only the local view, without consensus. `cmd/server` calls `AddMember` / `RemoveMember` from the admin API (runbook above).

---

## Node Removal (Graceful Departure)

**Scenario**: `node-3` is being decommissioned — hardware replaced, cluster shrinking.

### Step 1: Drain, Remove from Ring, Rebalance

```go
pm.RemoveNodeAndRebalance("node-3", 256, node3TransferAgent)
```

Internally:
```
1. pm.UpdateNodeState("node-3", NodeStateDraining)
   └─ state DRAINING, so health checks and metrics see a deliberate departure
      (routing is unchanged until step 2: the ring ignores node state)

2. pm.RemoveNode("node-3")
   ├─ Delete from Nodes map
   ├─ Remove 64 virtual nodes from ring   ← new requests stop routing here
   └─ pm.Version++, pm.epoch++

3. pm.Rebalance(256)
   └─ node-3's partitions reassigned to node-1 and node-2

4. [background] node3TransferAgent.MigrateToNewOwners()
   └─ Scan node-3's local keys → send each to every member of its current
      replica set → delete it once all of them acknowledged
```

The surviving members should also run `MigrateToNewOwners` (the auto-rebalancer does, on the "removed" event) so that replicas which inherited node-3's slots get copies from them too. (`cmd/server` does all of this on every node when a node is removed online — in replicated mode after draining it; see the runbook.)

**Critical**: The `TransferAgent` passed to `RemoveNodeAndRebalance` MUST be `node-3`'s own agent (`ta.localNodeID == "node-3"`); any other agent is rejected with an error. `nil` skips evacuation. Like node addition, this changes only the partition map it is called on — every node's map must drop node-3.

### Step 2: Evacuation Completes

```
[transfer] evacuating node=node-3
[transfer] migration done  copied=0  moved=847291  kept-unacked=0  errors=false
[transfer] evacuation complete node=node-3
```

Once all keys are transferred, `node-3` can be safely shut down. Its Raft log entries are already replicated to `node-1` and `node-2`, so no committed data is lost. In raft mode, also remove it from the Raft configuration with `RemoveServer("node-3")` on the leader.

---

## Node Crash (Unclean Failure)

**Scenario**: `node-2` crashes (OOM kill, hardware failure, kernel panic). It stops sending heartbeats without a graceful DRAINING transition.

### Detection Timeline

```
T=0 s     node-2 crashes (last heartbeat recorded via gossip, 1 s rounds)
T=1 s     FailureDetector.checkNodeHealth() tick (every 1 s) on node-1 and node-3
          timeSinceHeartbeat > SuspectThreshold (3 s): NOT YET
T≈3–4 s   first tick with timeSinceHeartbeat > 3 s
          → UpdateNodeState("node-2", NodeStateSuspect)
T≈10–11 s first tick with timeSinceHeartbeat > FailureThreshold (10 s)
          → UpdateNodeState("node-2", NodeStateFailed)
          → failedNodes["node-2"] = true
          → push "node-2" to recoveryQueue
```

### If node-2 Was the Raft Leader

The Raft election timer fires within 400–800 ms after heartbeats stop. A new leader is elected (see "Node Failover" section above). No write is lost as long as it was committed (quorum acknowledged).

### If node-2 Was a Follower

The remaining 2 nodes (leader + 1 follower) still form a quorum for a 3-node cluster. Writes continue normally. The dead node's absence only becomes a problem if a second node also fails — at that point quorum is lost.

### Recovery Attempts

The `backgroundRecoveryWorker` attempts to recover the failed node:

```go
func (fd *FailureDetector) attemptNodeRecovery(nodeID string) bool {
    node := fd.partitionMap.Nodes[nodeID]          // under pm.mu.RLock
    if fd.pingNode(node.Address, node.Port) {       // TCP "PING\n", needs a reply, 2 s timeout
        // Node is back online
        fd.partitionMap.UpdateNodeState(nodeID, NodeStateRecovering)
        fd.triggerRebalance()
        return true
    }
    return false
}
```

Up to `MaxRecoveryRetries` (3) attempts are made. A failed attempt re-queues the node immediately, so the attempts run back to back, each bounded by the 2 s ping timeout. If all fail, the node stays `FAILED` until a heartbeat arrives (see step 6 below) or an operator intervenes.

### RECOVERING → ACTIVE

Every `RecoveryInterval` (5 s) the same worker pings each `RECOVERING` node (`promoteRecoveredNodes`, `cluster/failure_detection.go`). After `RecoveryConfirmations` (2) consecutive successful pings, with a heartbeat younger than `SuspectThreshold` (3 s), the node becomes `ACTIVE` (`veltrixdb_failure_detector_nodes_reactivated_total`); a failed ping resets the count. The transition triggers `Rebalance`, which assigns partitions to ACTIVE nodes only, so the node owns partitions again. No data moves: the node never left the ring or the membership, it kept its data, and the auto-rebalancer ignores state changes (before invariant 62 it ran `MigrateToNewOwners` on FAILED / RECOVERING / ACTIVE events, which deleted keys on the survivors). Search fan-out (`PartitionMap.SearchPeers`) skips only FAILED nodes, so a node is searched again as soon as it is RECOVERING. A RECOVERING node whose heartbeats stop is marked SUSPECT / FAILED by the heartbeat checker as usual.

### Crash Recovery on the Crashed Node

When `node-2` restarts:

```
1. RaftNode starts (NewRaftNodeWithOptions)
   ├─ Restore raft_snapshot.dat if present (engine made exactly the snapshot + lastIncludedIndex/Term; raft_snapshot.gob is migrated first)
   └─ loadState(): raft_meta.dat → (CurrentTerm, VotedFor); replay raft_log.dat → retained
      log tail (torn tail truncated; a legacy raft_state.gob is migrated first)

2. StorageEngine starts → replayWAL()
   ├─ Open wal.log on each disk
   ├─ Decode every record with one reader: binary records (the default,
   │  CRC32C over the whole record) and legacy 10-field text records
   │  (6/7/8-field still accepted) may be mixed in one file
   ├─ A torn or corrupt record ends replay; everything before it is kept
   │  and the stop offset is logged
   └─ applyWALReplay() → rebuild shardedIndex in RAM
      (after a crash wal.log holds every record since the last clean-shutdown
       checkpoint — replay needed)

3. RaftNode starts as Follower
   ├─ Election timer starts (400–800 ms)
   └─ Current leader sends AppendEntries → node-2 becomes follower again

4. Leader backfills missing log entries:
   ├─ AppendEntries consistency check fails → leader backs nextIndex["node-2"] down
   ├─ If the entries node-2 needs were compacted into the leader's snapshot,
   │  the leader streams its snapshot instead (chunked InstallSnapshot;
   │  node-2's engine becomes exactly the snapshot — see replication.md#snapshots)
   └─ node-2 applies all missed entries to StateMachine (StorageEngine)

5. node-2 is caught up → counts toward quorum again

6. FailureDetector.RecordHeartbeat("node-2")   (gossip from node-2 again)
   └─ FAILED → UpdateNodeState("node-2", NodeStateRecovering)
      (searched again from here on; key routing uses the ring, which
       still contains it)

7. Recovery worker, every RecoveryInterval (5 s): 2 consecutive successful
   pings + fresh heartbeat → UpdateNodeState("node-2", NodeStateActive)
   └─ Rebalance: node-2 owns partitions again (no data moves — state
      changes never trigger migration, invariant 62)

8. --mode=replicated only: each peer's ReplicationEngine had marked node-2
   FAILED on its first failed send; its recovery worker probes node-2 with
   backoff (1 s doubling to 30 s), moves it to SYNC_PENDING, replays the
   writes node-2 missed and returns it to SYNC (docs/replication.md,
   Replica Recovery)
```

### Force-Remove (Node Never Comes Back)

If `node-2` cannot be recovered (hardware is destroyed):

```go
pm.ForceRemoveNode("node-2", 256)
```

This removes the node from the ring and rebalances partition assignments to `node-1` and `node-3`. No data evacuation — surviving replicas are the source of truth. `ForceRemoveNode` itself copies nothing; a `MigrateToNewOwners()` pass on each survivor afterwards (the auto-rebalancer runs one on the "removed" event) copies every key the survivor holds to replicas that newly need it, restoring the replica count where spare nodes exist. That pass deletes a key only on a survivor that left the key's replica set, and only after every current replica acknowledged it.

---

## Node Recovery (Returning After Failure)

When a crashed node comes back online after repair:

```
1. raft_meta.dat / raft_log.dat still present → node knows its last term, voted-for and log
2. WAL replay rebuilds the index (last clean-shutdown checkpoint + every record after it)
3. Node joins as Follower
4. Leader sends missing AppendEntries to catch up the log
5. Once matchIndex[recovered_node] == leader.lastLogIndex:
   ├─ Committed entries are re-applied to StorageEngine
   └─ Node is fully caught up
6. Peers' failure detectors see its heartbeat again → RECOVERING
   (if it had been marked FAILED; a SUSPECT node goes straight back to ACTIVE)
7. Two consecutive successful recovery pings → ACTIVE, rebalanced back into
   the partition table
8. It never left the ring (only ForceRemoveNode / RemoveNode take it out),
   so keys it owns route to it throughout
```

During the catch-up phase, the recovering node serves reads from its potentially-stale local state. Clients that need strong consistency should read from the leader or use quorum reads.

---

## Clean Shutdown, Restart and Upgrades

### Clean shutdown

`StorageEngine.Close()` (SIGTERM / normal exit) drains in this order: write batcher → C++ batch engine → io_uring bridge (if enabled) → defrag and compaction workers → segment files and VLogs → WALs. Each WAL is then rewritten as a **compacted checkpoint** — one record per live key, written to a temp file and atomically renamed — so the next startup replays O(live keys), not O(total writes). A crash during the checkpoint leaves the old `wal.log` intact. The native off-heap index is freed **last**, after the checkpoint (its final reader); the Go GC cannot reclaim it.

A node killed without a clean shutdown (SIGKILL, OOM, power loss) keeps its full WAL and replays it on restart, as described above.

### Before starting a node (Linux, cgo builds)

cgo builds keep the index off the Go heap, with one `mmap` per large shard array. Set `vm.max_map_count ≥ 262144` (`scripts/sysctl.conf`); with the default 65530, inserts can fail with `ENOMEM` at large key counts. `VELTRIXDB_INDEX=map` falls back to the Go map index.

### Network front-end in cluster modes

`--mode=raft` and `--mode=replicated` require `--net=go` (the default). `cmd/server` exits at startup if `--net=cpp|uring|poll` is combined with any mode other than `--mode=standalone` (or with `--auth-config`): the C++ front-end writes straight to the local engine and would bypass Raft and replication.

### Upgrade and rollback (WAL format)

New builds write **binary WAL records** by default and replay both binary and text, so an upgraded node reads its existing text `wal.log` and appends binary records after it — a rolling upgrade needs no extra step.

Rolling back depends on the target release (checked against the tags: binary WAL since v1.6, TTL-in-WAL — version-2 records and the 11th text field — since v1.14):

- **v1.14 and later, v1.16 included: no WAL step.** They read binary v1 and v2 records directly, keep TTLs, and do so after a clean stop or after a crash (`tests/upgrade` `TestDowngrade_Standalone`). Do **not** run `--wal-format=text-legacy` first: it only throws the TTLs away (every TTL'd key becomes immortal).
- **v1.13 and older** cannot read the record of a key with a TTL (and v1.5 and older no binary record at all): replay stops there and drops every later record. For those targets:

```
1. Restart the current build once with --wal-format=text-legacy
2. Stop it cleanly (the clean-shutdown checkpoint rewrites wal.log as text, without TTLs)
3. Start the older build
```

Keys written with a TTL come back immortal on such an older build. `--wal-format=text` keeps TTLs and is not a rollback mode. Rolling back a node that was not stopped cleanly after step 1 leaves records in `wal.log` that the old build cannot replay.

A raft node's WAL also holds the applied-index marker (`storage/raft_applied.go`). v1.16 installs it as a live key `"\x00veltrix/raft-applied"` (visible in `KEYS` / `SCAN` / `DBSIZE`); re-upgrading such a node needs no wipe: the new build trusts only a marker whose embedded version matches its record, so the copy v1.16 rewrote is ignored and the log is applied once, and the stray key is dropped on replay — [replication.md](replication.md#applied-index-exactly-once-apply-across-restarts), Downgrade. Raft and replicated rollbacks also have their own rules (raft: [replication.md](replication.md#persistence), Rollback).

### Rolling upgrade

One node at a time; a node is "done" only when it has caught up, not when its process is up. `tests/upgrade` follows exactly this procedure from v1.16 (README there).

**Raft** (`--mode=raft`):

1. Upgrade the **followers first, one at a time, the leader last** (the leader steps down when stopped; the next one is already upgraded).
2. Stop the node with SIGTERM (clean stop: the WAL checkpoint runs), swap the binary, start it on the same `--data`, `--node-id` and `--peers`.
3. **Wait until the restarted node has applied a fresh write before upgrading the next:** write a marker key through the leader after the restart and read it back on the restarted node (followers serve local reads unless `--linearizable-reads`), or watch `veltrixdb_raft_applied_index` on it pass the leader's commit index at the time of the write. A process that is up but still restoring a snapshot or replaying its log has not caught up, and taking a second node down then leaves the cluster with one up-to-date copy.
4. Do not let a v1.16 follower lag past the leader's log compaction during the upgrade: an upgraded leader can only catch it up with a v1.16 single-shot snapshot, which loses TTLs and does not apply deletes on it ([replication.md](replication.md#snapshots), Mixed versions). Upgrade a lagging v1.16 follower first.

**Replicated** (`--mode=replicated`):

1. One node at a time. Stop it with SIGTERM, swap the binary, start it on the same `--data`.
2. **Wait until every replica shows `SYNC`** (`TOPOLOGY`, or `GET /admin/cluster` → `replication[].state`, `resync_pending=false`) on every node before the next one.
3. **How the stopped node is caught up.** While a node is down, each peer keeps the writes it originated and the node has not acknowledged in its in-memory retention buffer, and persists a per-replica acknowledgement watermark in `<--data>/replication_state.json` once a second ([replication.md](replication.md#recovery-state-across-a-restart), CLAUDE.md invariant 61). If a peer restarts while the node is still down — for example because it was upgraded next — it starts that replica `FAILED` with a pending catch-up from the persisted watermark, and when the node returns, the peer replays its own writes since then from the change feed (deletes included, while their tombstones are retained) before returning it to `SYNC`. Before this build the buffer was lost on such a restart and the returning node never received those writes. A replica away for longer than the tombstone retention (2 × `GCGracePeriodSec`) gets a full resync and is flagged (`FULL RESYNC`, `delete_gap`) — rebuild it from a healthy node.
4. **Draining.** Between nodes that both run this build, draining is no longer needed for correctness: writes the stopping node acknowledged but had not shipped yet are shipped from its change feed after it restarts (its watermark for each peer is persisted). It is still worth stopping client writes to the node and letting its peers acknowledge them first: until the node is back, those writes exist only on it (with `--consistency=eventual`), and a node that never returns takes them with it. **When the node being stopped still runs v1.16, drain it** — it has no `replication_state.json`, so on its first start of the new build every replica is taken as caught up as of then and anything it had not shipped is never sent.

---

## Summary: What Each Event Guarantees

| Event | Data Loss? | Write Downtime | Read Downtime |
|-------|-----------|----------------|---------------|
| Leader failover (Raft 3-node) | None (committed writes safe) | ~400–800 ms election + one round trip for the no-op | None (followers serve reads) |
| Node addition | None | None | None |
| Graceful removal (DRAINING) | None | None | None |
| Node crash (follower, RF=3) | None (2 of 3 have data) | None | None |
| Node crash (leader, RF=3) | None (committed writes) | ~400–800 ms | None |
| Two nodes crash (RF=3) | Possible (quorum lost) | Indefinite until 1 recovers | Degraded |
| Force-remove dead node | None (surviving replicas) | None | None |
| Online add (raft, `--join`) | None | None (a learner is never counted toward quorum) | None |
| Online remove (raft, incl. the leader) | None | Leader removal: one election (~0.5–1.5 s) | None |
| Online add (replicated) | None | None | The new node misses keys until its catch-up / migration finished |
| Online remove (replicated, drained) | None | The removed node refuses writes from the drain on | None |
| Forced remove of a dead replicated node | Its acknowledged but unreplicated writes (`eventual`) | None | None |

---

## Relevant Prometheus Metrics

| Metric | Meaning |
|--------|---------|
| `veltrixdb_failure_detector_nodes_failed_total` | Counter: nodes marked FAILED by the heartbeat monitor |
| `veltrixdb_failure_detector_nodes_recovered_total` | Counter: nodes that recovered after being marked FAILED. Failed minus recovered over a window = nodes still down |
| `veltrixdb_failure_detector_false_positives_total` | Nodes suspected then recovered (flapping) |
| `veltrixdb_failure_detector_nodes_reactivated_total` | RECOVERING nodes promoted back to ACTIVE |
| `veltrixdb_cluster_nodes_total` | Gauge: registered cluster nodes |
| `veltrixdb_cluster_partition_migrations_total` | Key batches (≤ 500 keys) a new owner acknowledged during migration (`TransferAgent.sendBatches`); keys moved per run are in the `[transfer] migration done  moved=N` log line |
| `veltrixdb_cluster_rebalances_total` | Partition map rebalances triggered |

There is no gauge for nodes currently in SUSPECT state. In raft mode the term,
leadership and indexes are exported as `veltrixdb_raft_term`,
`veltrixdb_raft_is_leader`, `veltrixdb_raft_commit_index` and
`veltrixdb_raft_applied_index` ([monitoring.md](monitoring.md)); the leader's id
is in `GET /admin/cluster` (`raft.leader_id`) or `veltrix nodes`.
