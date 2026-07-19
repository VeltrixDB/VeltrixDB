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
| `single_node_test.go` | `TestIntegration_*` | Start a real `go run ./cmd/server` process; use the text protocol |
| `cluster_test.go` | `TestCluster_*` | In-process Raft nodes with real TCP transports; no binary needed |
| `backup_restore_test.go` | `TestIntegration_*Backup*` | Storage engine API directly; no TCP server |

## Requirements

- No special environment variables are required.
- The TCP-server tests (`single_node_test.go`) use `go run ./cmd/server`, so the
  Go toolchain must be available.
- Cluster and backup tests run entirely in-process.
- No root or special capabilities needed.

## Test tagging

Tests that take more than ~10 s are guarded with:

```go
if testing.Short() { t.Skip("...") }
```

Use `-short` to run only the fast subset.
