# VeltrixDB Integration Tests

End-to-end tests that start real server processes or wire up real storage
engines and exercise the full stack.

## Running

```bash
# Run all integration tests (allow up to 5 minutes for slow tests):
go test ./tests/integration/... -v -timeout 300s

# Skip slow tests (crash recovery, graceful restart, cluster, distributed,
# search cluster, backup):
go test ./tests/integration/... -v -short -timeout 60s

# Run a specific test:
go test ./tests/integration/... -v -run TestIntegration_PutGet -timeout 60s

# Search soak and crash chaos (skipped unless their variable is set; the
# nightly workflow runs 20m and 10 cycles):
VELTRIX_SOAK_DURATION=2m go test ./tests/integration/ -v -run TestSearchSoak -timeout 10m
VELTRIX_CHAOS_CYCLES=3 go test ./tests/integration/ -v -run TestSearchCrashRecovery -timeout 10m

# Raft tests with pipelined replication on (default: --raft-pipeline=false):
VELTRIX_RAFT_PIPELINE=1 go test ./tests/integration/ -v -run 'TestRaftClusterFailover|TestRaftAutoRebalance' -timeout 10m
```

## Test categories

| File | Tests | Notes |
|------|-------|-------|
| `single_node_test.go` | `TestIntegration_*` | Start a real server process; use the text protocol |
| `distributed_test.go` | `TestRaftCluster*`, `TestReplicated*`, `TestClusterClientRouting` | Multi-process cluster of real server processes |
| `cluster_test.go` | `TestCluster_*` | In-process Raft nodes with real TCP transports; no binary needed |
| `backup_restore_test.go` | `TestIntegration_*Backup*`, `TestIntegration_ManifestValidation` | Storage engine API directly; no TCP server |
| `search_test.go` | `TestSearchCluster_EndToEnd` | 3-node replicated cluster of real server processes signed with `--cluster-secret-file`: vector, filtered vector, BM25, hybrid and `IDXQUERY` from every node; unsigned node-to-node requests rejected; a search with a node down fails naming the peer, then succeeds once the node is marked failed |
| `soak_test.go` | `TestSearchSoak`, `TestSearchCrashRecovery` | Real server process checked against an oracle of acknowledged writes. Soak: concurrent upserts / deletes / searches for `VELTRIX_SOAK_DURATION`, RSS must level off, exact index-vs-oracle match after a clean restart. Crash: `VELTRIX_CHAOS_CYCLES` rounds of SIGKILL during 8 concurrent writers, restart on the same data dir, every acknowledged write (and no acknowledged delete) visible to search and GET. After each check, every id whose write had an unknown outcome at the kill is rewritten to a fresh, fully acknowledged version, so later cycles check it too |

## Requirements

- No special environment variables are required. Set `VELTRIX_SERVER_BIN` to
  a pre-built server binary to skip the build (CI does this).
  `VELTRIX_SOAK_DURATION` and `VELTRIX_CHAOS_CYCLES` enable the two
  `soak_test.go` tests. `VELTRIX_RAFT_PIPELINE` (bool, default false) sets
  `--raft-pipeline` on the nodes of `TestRaftClusterFailover` and
  `TestRaftAutoRebalance_LeaderKillKeepsEveryKey`, which check every node
  reports that mode in its topology.
- Otherwise the server-process tests (`single_node_test.go`,
  `distributed_test.go`, `search_test.go`, `soak_test.go`)
  `go build ./cmd/server` once per test run, so the Go
  toolchain must be available. That build uses cgo by default, so the server
  runs on the native index. CI builds the binary and runs the tests with
  `CGO_ENABLED=0` (Go map index). Set `VELTRIXDB_INDEX=map` to match CI on a
  cgo build.
- The servers use the default Go network front-end (`--net=go`; the tests do
  not pass `--net`). They use the text protocol and the binary search commands,
  neither of which the C++ front-end serves. The C++
  front-end is tested in `netfront/`.
- Cluster and backup tests run entirely in-process.
- No root or special capabilities needed.

## Test tagging

Slow tests are guarded with:

```go
if testing.Short() { t.Skip("...") }
```

Use `-short` to run only the fast subset. `TestSearchSoak` and
`TestSearchCrashRecovery` are skipped unless their environment variable is
set.
