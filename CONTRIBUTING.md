# Contributing to VeltrixDB

Thank you for your interest in contributing. VeltrixDB is a production-grade storage system — changes to the core engine require care. This guide covers everything you need to get started.

---

## Where to start

**Good first issues**: look for the [`good first issue`](https://github.com/VeltrixDB/veltrixdb/labels/good%20first%20issue) label. These are scoped, self-contained, and won't require you to understand the full storage engine.

**Bug reports**: open an issue with the label `bug`. Include: OS, Go version, server flags, reproduction steps, and the full error output.

**Feature requests**: open an issue with the label `enhancement` before writing code. Some features that seem useful have tradeoffs that aren't obvious from the outside — better to align first.

**Security vulnerabilities**: do NOT open a public issue. Email security@veltrixdb.io with details.

---

## Development setup

```bash
git clone https://github.com/VeltrixDB/veltrixdb
cd veltrixdb

# Go-only build (works on macOS and Linux)
go build ./...

# Run tests
go test ./...

# Run the server locally
go run ./cmd/server -addr :9000 -data ./dev-data -cache 256

# E2E tests (requires a running server on :9000)
./tests/e2e/run_all.sh

# Benchmark with pass/fail gates
./scripts/bench.sh
```

For the C++ io_uring layer (Linux only):
```bash
# Requires liburing, GCC/Clang with C++20 support
VERSION=1.0.0 ./scripts/build.sh --output ./dist
```

macOS is fully supported for development with the Go-only build. The C++ layer is Linux-only and not required for most contributions.

---

## Before you submit a PR

### 1. Run the full test suite

```bash
go test ./...
./tests/e2e/run_all.sh
./scripts/bench.sh
```

`bench.sh` has two hard pass/fail gates:
- **Density gate**: `bytes/record ≤ 1.2 × (24 + value_size)` — block packing must be working
- **GC emergency gate**: `gc_emergency_runs Δ == 0` — no GC death spirals

Both gates must pass on your branch before submitting.

### 2. Read CLAUDE.md

[CLAUDE.md](CLAUDE.md) contains the invariants that MUST NOT be broken. Critical ones:

- Shard routing is `FNV-1a(key) & 0x3FF` — never change the hash or bitmask without a full migration
- `WALFlushWindowMs` and `VLogFlushWindowMs` must always be equal
- VLog is written BEFORE WAL in the Put path (crash safety, see Invariant 19)
- `MarkDead` must be called whenever a key's VLog pointer is superseded (Invariant 17)
- `FlagPacked` must be sourced from `entry.IsPacked()` on the superseded entry when calling `MarkDead`
- `--local-ssd-interface=NVME` is required on GKE — document any Kubernetes change accordingly

### 3. No performance regressions

If your change touches the hot path (Put, Get, WAL flush, VLog append), run the load test and include before/after numbers in your PR description:

```bash
# 30-second mixed benchmark
go run ./cmd/loadtest \
  --addr=127.0.0.1:9000 \
  --mode=mixed --concurrency=64 --duration=30 \
  --num-keys=1000000 --value-size=128 --read-ratio=0.7
```

### 4. Keep PRs focused

One logical change per PR. A bug fix should not include unrelated refactoring. If you spot something worth cleaning up while fixing a bug, open a separate PR.

---

## Code style

- Standard `gofmt` formatting — run `gofmt -w .` before committing
- No external dependencies unless strictly necessary (the project uses Go stdlib + `prometheus/client_golang`)
- Comments only where the *why* is non-obvious — well-named identifiers explain the *what*
- Error handling: return errors, don't swallow them; `fmt.Errorf("context: %w", err)` wrapping
- For C++ changes: follow the existing namespace (`veltrix`), use the constructor initializer list pattern for nested structs (see Invariant 12 in CLAUDE.md)

---

## PR process

1. Fork the repo and create a branch from `main`
2. Write your change with tests
3. Ensure all tests pass and bench gates pass
4. Open a PR with:
   - A clear description of what changed and why
   - Before/after numbers if touching a hot path
   - A note on any invariants you verified
5. A maintainer will review within a few days

For large changes, consider opening a draft PR early to get feedback on the approach before finishing the implementation.

---

## Areas actively looking for contribution

- **RESP protocol compatibility** — the single most-requested feature. Implementing a RESP3 layer so Redis clients work without code changes.
- **Raft log snapshots** — prevents slow restarts on large clusters. Needs care around the WAL replay / index rebuild interplay.
- **Native range/prefix scans** — currently a workaround via NSSCAN. A proper range scan API needs thought around the 1024-shard scatter-gather cost.
- **Client SDK improvements** — connection pool tuning, timeout handling, retry logic across the 6 SDKs.
- **Documentation** — tutorials, getting-started guides, and runbooks are always useful.

---

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 license.
