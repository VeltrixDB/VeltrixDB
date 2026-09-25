# VeltrixDB Integration Tests

End-to-end tests that start real server processes or wire up real storage
engines and exercise the full stack.

## Running

```bash
# Run all integration tests (allow up to 5 minutes for slow tests):
go test ./tests/integration/... -v -timeout 300s

# Skip slow tests (crash recovery, cluster, backup):
go test ./tests/integration/... -v -short -timeout 60s

# Run a specific test:
go test ./tests/integration/... -v -run TestIntegration_PutGet -timeout 60s
```

## Test categories

| File | Tests | Notes |
|------|-------|-------|
| `single_node_test.go` | `TestIntegration_*` | Start a real server process; use the text protocol |
| `distributed_test.go` | `TestRaftCluster*`, `TestReplicated*`, `TestClusterClientRouting` | Multi-process cluster of real server processes |
| `cluster_test.go` | `TestCluster_*` | In-process Raft nodes with real TCP transports; no binary needed |
| `backup_restore_test.go` | `TestIntegration_*Backup*` | Storage engine API directly; no TCP server |

## Requirements

- No special environment variables are required. Set `VELTRIX_SERVER_BIN` to
  a pre-built server binary to skip the build (CI does this).
- Otherwise the server-process tests (`single_node_test.go`,
  `distributed_test.go`) `go build ./cmd/server` once per test run, so the Go
  toolchain must be available. That build uses cgo by default, so the server
  runs on the native index. CI builds the binary and runs the tests with
  `CGO_ENABLED=0` (Go map index). Set `VELTRIXDB_INDEX=map` to match CI on a
  cgo build.
- The servers use the default Go network front-end (`--net=go`), because the
  tests speak the text protocol. The C++ front-end is tested in `netfront/`.
- Cluster and backup tests run entirely in-process.
- No root or special capabilities needed.

## Test tagging

Tests that take more than ~10 s are guarded with:

```go
if testing.Short() { t.Skip("...") }
```

Use `-short` to run only the fast subset.
