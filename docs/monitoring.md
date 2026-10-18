# Monitoring

Every server serves Prometheus metrics on `--metrics-addr` (default `:2112`)
at `/metrics`, next to `/healthz` and `/readyz`. The endpoint is never
authenticated. The scrape config is `monitoring/servicemonitor.yaml`, and the
alerts are in `monitoring/prometheus-rules.yaml`. The Helm chart ships its own
copy of the rules (see [Helm chart](#helm-chart)).

## What `/metrics` serves

The server uses a private registry (`cmd/server/metrics_setup.go`). Every
series carries `node="<--node-id>"`.

| Family | Source | Notes |
|---|---|---|
| `veltrixdb_storage_*`, `_cache_*`, `_vlog_*`, `_scrub_*`, `_server_*`, `_cluster_*`, `_failure_detector_*` | `metrics/prometheus.go` | Storage, cache, VLog GC, scrubber, connections, partition map, failure detector |
| `veltrixdb_replication_*` | `metrics/prometheus.go` | `--mode=replicated` only (see [replication.md](replication.md)) |
| `go_*`, `process_*` | client_golang Go and process collectors (`metrics/runtime.go`) | `process_*` reads `/proc`, so it is Linux only. On macOS only `process_start_time_seconds` is served, taken when the metrics package initialises. |
| `veltrixdb_runtime_*`, `veltrixdb_process_*`, `veltrixdb_system_*`, `veltrixdb_disk_*` | `metrics/hardware.go` | Read by the Grafana dashboards. The `_system_*` and `_disk_*` series, and the `veltrixdb_process_*` gauges, read `/proc`, so they are Linux only. `_disk_*` covers the devices that `--data-dirs` are mounted from. |
| `veltrixdb_search_*` | `metrics/search.go` | Every mode |
| `veltrixdb_raft_*` | `metrics/raft.go` | `--mode=raft` only |

A scrape of a fresh node takes about 8 ms (median of 10, macOS). The search
collector's scrape cost grows with the number of namespaces, not with the
number of vectors.

### Search

The coordinating node counts each client vector (`VSEARCH`, `VSEARCHX`),
text (`TSEARCH`) and hybrid (`HSEARCH`) request once. The fan-out is included
in the count, and the vector and text halves of a hybrid search are not
counted again. Peer-to-peer `/internal/search` calls are not counted.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `veltrixdb_search_requests_total` | counter | `kind` = vector \| text \| hybrid | Requests, failed ones included |
| `veltrixdb_search_errors_total` | counter | `kind`, `reason` = rebuilding \| incomplete \| other | `rebuilding`: refused during the index rebuild (invariant 57). `incomplete`: a peer did not answer the fan-out. `other`: bad query, unknown namespace, filter error |
| `veltrixdb_search_latency_seconds` | histogram | `kind` | Request latency on the coordinating node, fan-out included (buckets 0.5 ms – 5 s) |
| `veltrixdb_search_partial_total` | counter | | Searches answered without every peer because `--search-allow-partial` is set |
| `veltrixdb_search_ready` | gauge | | 1 when the search indexes are complete. 0 while the startup or snapshot-restore rebuild runs (searches are refused) |
| `veltrixdb_search_rebuild_loaded_keys` / `_rebuild_total_keys` | gauge | | Progress of the current or last rebuild. The total stays 0 until the rebuild's scan finishes |
| `veltrixdb_search_vectors` / `_vector_tombstones` / `_vector_namespaces` | gauge | | Live vectors, graph tombstones awaiting compaction, and the number of vector namespaces on this node |
| `veltrixdb_search_namespace_vectors` | gauge | `namespace` | Live vectors per namespace. Only the first 64 namespaces in name order get a series; the totals include every namespace |
| `veltrixdb_search_text_documents` / `_text_namespaces` | gauge | | Indexed text documents and BM25 namespaces |

Recording a request costs three atomic adds and allocates nothing
(`TestSearchMetrics_ObserveDoesNotAllocate`).

### Raft

These metrics are read from `RaftNode.SnapshotStatus()`, `Term()` and
`IsLeader()` at scrape time.

| Metric | Type | Meaning |
|---|---|---|
| `veltrixdb_raft_term` | gauge | Current term |
| `veltrixdb_raft_is_leader` | gauge | 1 on the leader |
| `veltrixdb_raft_commit_index` / `_applied_index` | gauge | Commit and apply position. The difference is how stale local reads are |
| `veltrixdb_raft_snapshot_index` / `_snapshot_term` | gauge | Last index and term covered by this node's snapshot |
| `veltrixdb_raft_log_retained_entries` | gauge | Log entries kept after the snapshot |
| `veltrixdb_raft_snapshot_threshold_entries` | gauge | `--raft-snapshot-threshold` (0 = automatic snapshots off) |
| `veltrixdb_raft_snapshot_captures_total` / `_installs_total` | counter | Snapshots captured by this node, and snapshots installed from a leader |
| `veltrixdb_raft_snapshot_chunks_sent_total` / `_chunks_received_total` | counter | InstallSnapshot chunks |
| `veltrixdb_raft_snapshot_legacy_sent_total` | counter | Single-shot snapshots sent to v1.16 and older followers |
| `veltrixdb_raft_snapshot_last_capture_seconds` / `_last_apply_fence_seconds` / `_last_bytes` | gauge | Duration, Apply stall and file size of the last capture |
| `veltrixdb_raft_snapshot_capture_failures_total` | counter | Captures that ended without a new snapshot (an error, or aborted by an install or shutdown); retried with backoff |
| `veltrixdb_raft_snapshot_sent_total` | counter | Leader: whole-snapshot transfers that completed (chunked: the follower confirmed the install; single-shot: accepted, also in `_legacy_sent_total`) |
| `veltrixdb_raft_snapshot_transfer_failures_total` | counter | Leader: transfers that ended without an install — send, read or follower install failure, a single-shot over 256 MB (`NOT sending snapshot … upgrade peer`), or aborted by a leader change / peer removal |
| `veltrixdb_raft_snapshot_install_failures_total` | counter | Follower: received snapshots not installed (verification, persist or install failure; shutdown aborts not counted) |
| `veltrixdb_raft_snapshot_restore_failures_total` | counter | Follower: failed state-machine restore attempts of an install. Retried until one succeeds; the node serves no usable state meanwhile |

### Replication recovery

`--mode=replicated` only, from `ReplicationMetrics` (see
[replication.md](replication.md#recovery-state-across-a-restart)). Per replica,
the same state is in `GET /admin/cluster`: `replication[].resync_pending` and
`replication[].delete_gap`.

| Metric | Type | Meaning |
|---|---|---|
| `veltrixdb_replication_full_resyncs_total` | counter | Catch-ups that streamed the whole change feed: the replica's watermark was older than the tombstone retention, or `replication_state.json` was unreadable (ERROR `FULL RESYNC`) |
| `veltrixdb_replication_catchup_gaps_total` | counter | Recoveries that could not replay everything the replica missed; deletes may be missing on it |
| `veltrixdb_replication_replicas_delete_gap` | gauge | Replicas flagged with a delete gap since this node started |
| `veltrixdb_replication_replicas_resync_pending` | gauge | Replicas whose change-feed catch-up is pending or running |
| `veltrixdb_replication_state_persists_total` / `_state_persist_errors_total` | counter | Writes of `replication_state.json` (at most one per second), and failed ones |

## Alerts

`monitoring/prometheus-rules.yaml` has 24 rules:

- **storage**: write admission throttle, VLog GC paused with garbage over
  50%, VLog GC read errors, low cache hit rate, read P99 over 10 ms, failed
  peer, cluster shrank, scrubber corruption
- **process**: data disk over 85% full (node-exporter), process restarted
  (`process_start_time_seconds` is less than 5 minutes old; this also fires on
  a first start)
- **search**: rebuild stuck (not ready, and no keys loaded for 30 minutes),
  partial results, `rebuilding` or `incomplete` errors for 10 minutes, P99
  over 1 s
- **raft**: snapshot captures failing (more than 3 failures and no success in
  30 minutes), snapshot transfers failing on the leader (more than 3 failures
  and no completed send in 30 minutes), snapshot install or restore failing on
  a follower, log not compacted (retained log over 4× the threshold for 15
  minutes — the backstop for captures that never start), apply lag over
  10,000 entries, term churn, no leader
- **replication**: full resync / delete gap (critical: deletes may be missing
  on a replica), `replication_state.json` write errors, a replica catch-up
  pending for 30 minutes

`TestMetricsEndpoint` (`cmd/server/metrics_endpoint_test.go`) starts a
single-node raft server and a single-node replicated server and checks that
every metric a rule names is served by one of them.
Its allowlist holds only names from other exporters. A rule that uses a new
metric must be in that test's path, or be added to the allowlist with a
reason. Check the syntax with
`promtool check rules` on the `spec:` part of the file.

## Helm chart

The chart (`VeltrixDB-Helm-Chart`, `templates/prometheusrule.yaml`) carries
its own rule set, filtered on the ServiceMonitor's `job` and the release
namespace. Its process-restart, search and raft groups read metrics that
releases up to v1.16 do not export, so they render only with
`prometheusRule.extendedAlerts=true`. Set it once the image serves
`veltrixdb_search_*`. The raft group also needs `cluster.mode=raft`, and the
`veltrixdb.replication-recovery` group `cluster.mode=replicated`.

## Gaps

- **The search metrics count client requests on the coordinating node only.**
  For a per-node view of fan-out work, compare `veltrixdb_search_*` across
  nodes.
- **Admin-only counters.** CDC and audit drops (`cdc_dropped_total`,
  `audit_dropped_total`) are in `/admin/stats`, not in Prometheus.
