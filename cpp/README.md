# C++ layer — what is actually wired

Read this before changing anything under `cpp/`, and before citing the C++
layer in documentation or benchmarks.

Most of this directory is **not reachable from Go**. That is not obvious from
the file tree, and the top-level docs claimed otherwise for a long time.

## Product surface: four files compiled into the Go package

These are compiled into the Go `storage` package by cgo shims on Linux. Three
of them are reached from Go; `uring_reader.cpp` is compiled in but nothing
calls it.

| Source | Entry point | Reached from Go via |
|--------|-------------|-------------|
| `src/batch_engine.cpp` | `veltrix_batch_engine_*` | `storage/cgo_bridge.go`, `storage/cgo_bridge_pinner.go` — only the fire-and-forget `BatchPut` / `WriteBatcher` path |
| `src/numa_topology.cpp` | `NumaTopology`, `pin_thread_to_cpus` | the batch engine's thread pool (shim `storage/cgo_numa_linux.cpp`) |
| `src/storage_bridge.cpp` | `veltrix_bridge_*` (`include/storage_bridge_capi.h`) | `storage/cgo_bridge_storage_bridge_linux.go` — **opt-in**, `VELTRIXDB_URING_BRIDGE=on\|sqpoll` |
| `src/uring_reader.cpp` | `UringReader` | **nothing** — shim `storage/cgo_uring_linux.cpp` compiles it, no Go call site |

Two more C++ components run in a cgo binary but do not live here: the
off-heap native index (`storage/native_index.cpp`, default on every cgo build
including macOS; kept in `storage/` so the Go build cache tracks it) and the
opt-in network front-end (`netfront/`, `--net=cpp|uring|poll`).

**All four** are compiled **into the Go package** by the cgo shims in
`storage/cgo_*_linux.cpp`. They must NOT also be listed in `CMakeLists.txt` —
that would define their symbols twice when `scripts/build.sh` links the
archive alongside the cgo objects.

`storage_bridge.cpp` was the exception until CI caught it: it was in
`CMakeLists.txt` with no shim, so its `veltrix_bridge_*` symbols existed only
in the static archive. Anything that did not link that archive — including a
plain `CGO_ENABLED=1 go build ./storage/...`, and `go test -race`, which
requires cgo — failed with `undefined reference to 'veltrix_bridge_create'`.

## Not reachable from Go (~5,400 lines)

Compiled by `CMakeLists.txt` into `libveltrixdb_engine.a`, but nothing in Go
calls them. They link into the binary and do nothing.

- `art.cpp` / `art.hpp` — Adaptive Radix Tree index
- `scheduler.cpp` / `scheduler.hpp` — 3-tier priority io_uring scheduler
- `lirs_cache.cpp`, `shard.cpp`, `write_path.cpp`, `defragmenter.cpp`
- `vlog.cpp` — C++ VLog reader/writer
- `ebpf_gc_throttle.cpp` — cgroup/eBPF GC bandwidth throttle
- `allocator.hpp`, `index_entry.hpp`, `lockfree_index.hpp` — headers used only
  by the above

`vlog.cpp` and `ebpf_gc_throttle.cpp` were in *neither* the CMake target nor
any cgo shim until recently, so they were compiled by nothing at all. They are
in the build now purely so they cannot rot further.

## Where the work actually happens

Every path the unreachable files would serve is handled in Go today:

| Advertised C++ component | What actually runs |
|--------------------------|--------------------|
| ART index | `storage/shard.go` — 8192 shards, each an off-heap hash table (`storage/native_index.cpp`) on cgo builds or a Go map otherwise |
| C++ VLog reader / `UringReader` | `storage/vlog.go:ReadValue` (Go `pread`) |
| LIRS cache | `storage/cache.go` + `storage/cache_sharded.go` |
| Defragmenter | `storage/defrag.go` |
| Priority scheduler | none — Go uses ordinary pread/pwrite |

## Before you argue for wiring any of it up

The Go read path was optimised in this branch and is now substantially faster
than when the C++ was written: cache-hit reads went 711 ns → 92 ns and Go map
index lookups 19.4 ns → 9.6 ns with zero allocations. An ART binding has to
beat that plus cgo call overhead, which is a much harder target than it was
against the original numbers. The native index shows the scale: one cgo call
per cache-miss lookup costs it ~19 ns more than the Go map, a price paid for
GC (full GC 21 ms → 0.27 ms at 5M keys), not for lookup speed. Batch across
the cgo boundary or do not cross it.

So: measure the Go path first, and only reach for C++ where a benchmark shows
Go cannot get there.

## Turning it off (and on)

`VELTRIXDB_DISABLE_CGO_ENGINE=1` skips constructing the io_uring bridge and
the C++ batch engine, and selects the Go map index, leaving the pure-Go paths
in place. No effect on a `CGO_ENABLED=0` build, where all are already stubs.

The io_uring VLog bridge alone is opt-in: `VELTRIXDB_URING_BRIDGE=on` (no
SQPOLL) or `=sqpoll`. Default off, because a VLog batch is already one pwrite
and one fdatasync, so the bridge saves at most one syscall per batch. On a
4-CPU runner the C++ storage layer as a whole (bridge with SQPOLL, batch
engine and native index, all on) measured 1.12M keys/s batch writes against
1.77M with all three off; how that splits between the three is not yet
measured. Opt in only after measuring on the target hardware
(`ENGINES="native bridge sqpoll" scripts/net-bench.sh`). Under Kubernetes'
RuntimeDefault seccomp profile `io_uring_setup` is blocked, and an opted-in
bridge falls back to pwrite.

Before either knob existed both were unconditional: with cgo compiled in, every
`NewStorageEngine` created an io_uring bridge with SQPOLL (a busy-polling
kernel thread per ring) and `runtime.NumCPU()` C++ worker threads, with no
opt-out short of rebuilding. Fine for a long-lived server on dedicated
hardware; harmful anywhere that builds many short-lived engines on a small
box. CI proved the point — the `-race` job needs cgo, which silently switched
the engine onto the C++ path, and the SQPOLL rings starved a 2-core runner
into a 30-minute timeout.

## Build and CI

- `scripts/build.sh` (Linux, without `--go-only`) builds the CMake target and
  links it with `CGO_ENABLED=1`.
- The **Docker image is `CGO_ENABLED=1`**: it carries the native index, the
  batch engine and the (opt-in) io_uring bridge from the shims above, but not
  the CMake archive — nothing from the "not reachable" list. The cgo
  directives use `-march=x86-64-v2` on amd64, never `-march=native`;
  `scripts/build.sh` re-adds `-march=native` via `CGO_CXXFLAGS` because it
  builds on the target. `--build-arg CGO_ENABLED=0` gives the old pure-Go
  image.
- CI job `node-6-cpp` compiles the CMake target, the cgo shims and
  `scripts/build.sh`, and runs the storage tests with cgo and
  `VELTRIXDB_URING_BRIDGE=on`. `node-5-race` runs cgo with
  `VELTRIXDB_DISABLE_CGO_ENGINE=1` but `VELTRIXDB_INDEX=native`. Every other
  job is `CGO_ENABLED=0`.

## Recommendation

Decide one of these, rather than leaving it ambiguous a third time:

1. **Delete the unreachable set** (~5,400 lines; `git log` keeps it). Correct
   if nobody is going to write the bindings this quarter. Nothing else depends
   on it — the four shim-compiled files include none of it.
2. **Wire one component and benchmark it** against the Go path it replaces. If
   it does not win by a margin worth the cgo complexity, delete it.

What should not happen is a third year of compiling code nothing calls while
the README advertises it as a shipped feature.
