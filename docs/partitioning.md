# Partitioning and Rebalancing in VeltrixDB

VeltrixDB uses **consistent hashing with virtual nodes** to distribute data across cluster nodes. This document covers how keys are routed, how partitions are assigned, and how the cluster rebalances when nodes are added or removed.

---

## Single-Node Partitioning (Within a Node)

Before reaching cluster-level partitioning, every key is routed to a specific **shard** and **disk** on the local node:

```
key  ──► FNV-1a hash ──► shard_id = hash & 0x1FFF  (0..8191)
                       ──► disk_idx = shard_id % numDisks
```

8192 in-memory shards each hold their own `sync.RWMutex`. This allows concurrent reads and writes to different key ranges without global locking. The C++ `kNumShards` constant and Go `numShards` constant are both 8192 — they must always match.

---

## Cluster-Level Partitioning

Cluster routing only applies in `--mode=raft` or `--mode=replicated`, and those modes need the Go network front-end (`--net=go`, the default). The server refuses to start with `--net=cpp|uring|poll` outside `--mode=standalone`, because the C++ front-end writes straight to the local engine and would bypass Raft/replication routing.

The server does not forward a request to the key's owner. Routing is done by the cluster-aware client (`client/client.go`): it fetches the topology (`TOPOLOGY` command), builds the same ring (64 virtual nodes) and sends each key to `ring.GetNode(cluster.HashKey(key))`. In raft mode the leader takes every write and followers answer `MOVED`. Membership is static: each server registers the nodes named in `--peers` (and `--seeds`) at startup.

### Consistent Hash Ring

The `ConsistentHashRing` (`cluster/partition_map.go`) maps a continuous 64-bit hash space onto physical nodes using **virtual nodes**:

```go
type ConsistentHashRing struct {
    virtualNodes map[uint64]string  // hash → node_id
    sortedKeys   []uint64           // sorted for binary search
    replicas     int                // virtual nodes per physical node (default: 64)
}
```

**Virtual nodes** (default 64 per physical node) provide two benefits:
1. **Load balance**: each physical node owns ~`64 / totalVirtualNodes` of the ring. More virtual nodes → more even distribution.
2. **Gradual rebalancing**: adding one physical node moves `1/N` of the ring's data (where N = new node count) instead of bulk-moving half the data.

### How Keys Are Routed

```
key = "user:42"
    │
    ▼
hash = fmix64(FNV-1a(RoutingKey("user:42")))   →   uint64 hash value
    │
    ▼
Binary search in sortedKeys for first virtualNode hash >= key hash
    │
    ▼
If none found, wrap around to sortedKeys[0]   (ring property)
    │
    ▼
nodeID = virtualNodes[foundHash]              →  "node-2"
    │
    ▼
Route request to node-2
```

`RoutingKey` (`cluster/partition_map.go`) is the key itself, except for
derived search keys, which route as the record they belong to:
`@vec/<ns>/<id>` and `@txt/<ns>/<id>` route as `<id>`, and
`@idx/<rule>/<value>/<primary>` as `<primary>`. The hash is FNV-1a passed
through the MurmurHash3 `fmix64` finalizer (`hashValue`), so sequential keys
spread across the ring instead of landing on one arc. A rebalance therefore keeps a record,
its vector, its text document and its secondary-index entries on one node,
so each node can evaluate search filters on its own data. Server and cluster
client use the same function.

### PartitionMap

The `PartitionMap` ties the ring to partition metadata:

```go
type PartitionMap struct {
    Version            uint64                    // increments on every topology change
    Timestamp          int64                     // last modification time (ns)
    Nodes              map[string]*Node          // nodeID → Node
    Partitions         map[uint32]*PartitionInfo // 256 partitions
    Ring               *ConsistentHashRing
    ReplicationFactor  int                       // default: 3
}
```

**256 hash partitions** divide the uint64 range into equal-sized buckets:

```
partition_i covers hash range: [i × (MaxUint64/256), (i+1) × (MaxUint64/256))
```

Each partition has a primary node and up to `ReplicationFactor - 1` replica nodes. The partition table is metadata (topology views, admin); key placement and migration use the ring (`GetNodeForKey`), not the table.

### PartitionInfo

```go
type PartitionInfo struct {
    PartitionID   uint32
    HashRange     [2]uint64  // [start, end)
    PrimaryNode   string     // which node is primary
    ReplicaNodes  []string   // backup nodes (up to RF-1)
    Version       uint64
    LastModified  int64
}
```

### Getting Replicas for a Key

```go
// Returns [primaryNode, replica1, replica2, ...]
nodes, err := pm.GetReplicasForKey(key)
```

Internally: hash the key, list the distinct physical nodes clockwise from it on the ring, then pick `ReplicationFactor` of them with the same rack-aware `pickReplicas` as `Rebalance` (first copy = ring successor).

---

## Node States

Each cluster node cycles through these states:

| State | Meaning |
|-------|---------|
| `ACTIVE` | Fully operational, accepting reads and writes |
| `SUSPECT` | No heartbeat for 3 s — health uncertain; back to ACTIVE on the next heartbeat |
| `FAILED` | No heartbeat for 10 s — left out of `Rebalance`'s partition table and of search fan-out (it stays on the ring) |
| `RECOVERING` | A FAILED node heartbeated again, or answered the recovery ping. Like FAILED it is left out of `Rebalance`; nothing in `cluster/` moves it back to ACTIVE |
| `DRAINING` | Set by `RemoveNodeAndRebalance` just before the node is removed from the ring |

States are derived locally: each node's `FailureDetector` checks heartbeat ages every second. Heartbeats arrive through **gossip** — every second a node exchanges digests with 3 random peers, and a node whose heartbeat counter advanced in a digest counts as alive (so liveness spreads transitively). Receivers do not adopt the sender's view of node states. With `--auto-rebalance` (default on), joins, removals, and transitions to FAILED / ACTIVE / RECOVERING trigger `Rebalance` + `MigrateToNewOwners` after a 3 s debounce (`cmd/server/rebalancer.go`).

---

## Rebalancing

Rebalancing is triggered by `pm.Rebalance(partitionCount)` after any topology change (node addition or removal).

### How Rebalance Works

```go
func (pm *PartitionMap) Rebalance(partitionCount uint32) error {
    // 1. Collect all ACTIVE nodes, sorted by node ID
    nodeList := [active nodes from pm.Nodes, sorted by ID]

    // 2. Clear existing partition assignments
    pm.Partitions = make(map[uint32]*PartitionInfo)

    // 3. For each partition 0..partitionCount-1:
    for i := range partitions {
        start := i % len(nodeList)                 // round-robin primary
        replicas := pickReplicas(nodeList, start, pm.ReplicationFactor)

        pm.Partitions[i] = &PartitionInfo{
            PrimaryNode:  replicas[0],
            ReplicaNodes: replicas[1:],
            HashRange:    [rangeStart, rangeEnd),
        }
    }

    pm.Version++
}
```

Partitions are assigned round-robin over the ID-sorted node list, so every member that runs `Rebalance` independently computes the same table, and the primary count per node differs by at most one. With 256 partitions and 3 nodes, each node gets ~85 primary partitions plus ~170 replica assignments.

`pickReplicas` walks forward from the primary and is rack-aware: a replica never shares a failure domain (`--rack-id`) with an earlier copy unless every remaining node would. With no racks configured it reduces to plain round-robin.

### After Rebalance: Data Migration

`Rebalance()` only updates routing metadata. No data moves during rebalance. Data migration is handled by `TransferAgent.MigrateToNewOwners()`:

```
MigrateToNewOwners()
    │
    ▼
ScanKeys() → all local keys
    │
    ▼
For each key:
    owner = pm.GetNodeForKey(key)  ← using updated ring
    if owner == localNodeID: skip
    else: queue for transfer to owner
    │
    ▼
Fan out to destination nodes in parallel:
    ├─ Batch 500 keys × ≤64 KB average ≈ 32 MB per HTTP POST /transfer/keys (JSON)
    ├─ Destination receives batch → calls store.Put(key, value, ttl) for each
    │   (any failed Put → HTTP 500 for the batch)
    └─ After confirmed delivery: store.Delete(key) locally
```

**Safety guarantee**: `Put` on destination happens before `Delete` on source. Only keys in batches the destination acknowledged with HTTP 200 are deleted; on the first failed batch the rest of that destination's keys stay local and the next `MigrateToNewOwners()` call retries them. A "connection refused" is retried 3 times, 100 ms apart; each HTTP call times out after 60 s.

**Epoch fencing**: every batch carries the sender's membership epoch (advanced by each AddNode/RemoveNode and by newer epochs seen in gossip); a receiver whose epoch is newer refuses it with HTTP 409.

**Pinned keys**: `@vecns/<ns>` (vector namespace settings) and `@idxdef/<name>` (secondary-index definitions) are needed on every node, so they are *copied* to each destination — placed at the head of its key list, ahead of the migrated keys — and never deleted locally.

**Search indexes follow the keys**: the destination's `Put` and the source's `Delete` run the engine's search hooks, so migrated vectors and documents become searchable on the new owner and disappear from the old one. (Before these hooks, migrated vectors stayed searchable on the source and were unsearchable on the destination.)

**Authentication**: with `--cluster-secret-file` (or `VELTRIXDB_CLUSTER_SECRET`; at least 16 bytes, whitespace trimmed) every request on the transfer listener — `/transfer/keys` and the distributed-search endpoint `/internal/search` — carries `X-Veltrix-Cluster-Time` (unix seconds) and `X-Veltrix-Cluster-Auth` (hex HMAC-SHA256 over time, method, path and body), and is refused (HTTP 401) if it does not verify or is more than 5 minutes off the receiver's clock. `/transfer/health` stays open. Without a secret or mTLS (`--cluster-mtls`) the server logs `[transfer] WARNING: listener <addr> is unauthenticated (no --cluster-secret-file and no mTLS) ...`.

**Concurrency**: Each destination node receives its batch in a separate goroutine. A cluster-wide migration is parallel across all `N-1` destination nodes simultaneously.

---

## Adding a Node

```
1. pm.AddNode(nodeID, address, port)
   ├─ Create Node{state=ACTIVE}
   ├─ Add 64 virtual nodes to ConsistentHashRing
   └─ pm.Version++, epoch++

2. pm.Rebalance(256)
   ├─ Recompute partition assignments with new node included
   └─ Re-deals every partition round-robin over the new node list (most
      primaries change; key placement follows the ring, where ~1/N of keys move)

3. ta.MigrateToNewOwners()  [background goroutine]
   ├─ Scan all local keys
   ├─ Identify keys now owned by new node
   └─ Stream to new node in 500-key batches
        ├─ New node receives via POST /transfer/keys
        └─ Source deletes after confirmed delivery
```

Gossip does not carry membership: every member must run `AddNode` itself (the server does it for `--peers` at startup). Because `Rebalance` is deterministic, members with the same node set compute the same table.

**Shortcut**: `pm.AddNodeAndRebalance(nodeID, addr, port, transferAddr, 256, ta)` combines steps 1–3 in one call.

The migration runs in the background. Reads/writes continue normally during migration — a client that has refreshed its topology routes new requests to the new node at once, while existing keys are transferred concurrently (a key read on the new owner before its batch lands is not found yet).

---

## Removing a Node (Graceful)

```
1. pm.UpdateNodeState(nodeID, NodeStateDraining)
   └─ Health checks and metrics see graceful departure, not sudden failure

2. pm.RemoveNode(nodeID)
   ├─ Delete from Nodes map
   └─ Remove 64 virtual nodes from ring

3. pm.Rebalance(256)
   └─ Reassign departed node's partitions to surviving nodes

4. ta.MigrateToNewOwners()  [departing node's own TransferAgent]
   └─ Evacuate all local keys to their new owners
      (ta.localNodeID MUST == nodeID — using another node's TA would scan wrong data)
```

**Important**: `ta` must be the **departing node's own TransferAgent**. The `RemoveNodeAndRebalance()` function enforces this:

```go
if ta != nil && ta.localNodeID != nodeID {
    return fmt.Errorf("ta.localNodeID %q != nodeID %q", ta.localNodeID, nodeID)
}
```

**Shortcut**: `pm.RemoveNodeAndRebalance(nodeID, 256, ta)` does all four steps.

---

## Force-Removing a Dead Node

For a node that crashed or is unreachable (cannot gracefully evacuate its data):

```
pm.ForceRemoveNode(nodeID, 256)
    ├─ Remove from ring immediately
    └─ Rebalance (surviving nodes take over partition ownership)
    (no data migration — surviving replicas are the source of truth)
```

Nothing re-creates the lost copies afterwards: the Replication Engine's anti-entropy only re-sends un-acked writes to lagging replicas and never copies existing data, so the replication factor stays reduced for keys the dead node held.

---

## Gossip Protocol

Cluster topology changes (node state, partition map version) propagate via **epidemic gossip**:

```
Every 1 second, each node:
    1. Advance its own heartbeat counter
    2. Build GossipDigest{sender, epoch, map_version, ts, nodes{state, hb, addrs, rack}}
    3. Select 3 random peer nodes (fanout=3)
    4. Per peer: one short TCP connection — send digest (JSON), merge the reply
```

A receiver records a heartbeat for the sender and for every node whose counter advanced, adopts gossip addresses and racks (self-reported entries win), ignores nodes it does not know, and advances its epoch to a newer remote one. A digest with a stale epoch still counts as liveness but its metadata is ignored. `map_version` is sent but not acted on: no node fetches a partition map from a peer.

---

## Partition Map Versioning

Every mutation to the partition map increments `pm.Version` (reported in topology and gossip). The version is not used to reject anything; the fencing check is the separate membership **epoch** (`cluster/epoch.go`), which transfer batches carry (stale → HTTP 409).

```go
// Version increments on:
pm.AddNode(...)      → Version++
pm.RemoveNode(...)   → Version++
pm.UpdateNodeState(...)  → Version++ (on actual state change)
pm.UpdateNodeHeartbeat(...) → Version++ (SUSPECT → ACTIVE only)
pm.Rebalance(...)    → Version++
```

---

## Configuration Reference

| Parameter | Default | Effect |
|-----------|---------|--------|
| `ReplicationFactor` | 3 | Copies of each partition (1 primary + 2 replicas) |
| `VirtualNodesPerNode` | 64 | Virtual nodes per physical node on the ring |
| `PartitionCount` | 256 | Total hash partitions |
| `GossipInterval` / `Fanout` | 1 s / 3 | Gossip tick interval and peers per round |
| `HeartbeatInterval` | 1 s | Failure-detector check interval |
| `SuspectThreshold` | 3 s | Heartbeat age before SUSPECT |
| `FailureThreshold` | 10 s | Heartbeat age before FAILED |
| `rebalanceDebounce` | 3 s | Auto-rebalance debounce (`--auto-rebalance`, default true) |
| `transferBatchSize` | 500 keys | Keys per HTTP migration batch |
| `transferHTTPTimeout` | 60 s | Timeout per migration HTTP call |
