# C++ layer — what is actually wired

Read this before changing anything under `cpp/`, and before citing the C++
layer in documentation or benchmarks.

Most of this directory is **not reachable from Go**. That is not obvious from
the file tree, and the top-level docs claimed otherwise for a long time.

## Product surface: four files

These have C entry points that the Go `storage` package calls. They are the
C++ layer, as far as any running binary is concerned.

| Source | Entry point | Called from |
|--------|-------------|-------------|
| `src/batch_engine.cpp` | `veltrix_batch_engine_*` | `storage/cgo_bridge.go`, `storage/cgo_bridge_pinner.go` |
| `src/uring_reader.cpp` | `UringReader` | cgo shim `storage/cgo_uring_linux.cpp` |
| `src/numa_topology.cpp` | `NumaTopology`, `pin_thread_to_node` | cgo shim `storage/cgo_numa_linux.cpp` |
| `src/storage_bridge.cpp` | `veltrix_bridge_*` (`include/storage_bridge_capi.h`) | `storage/cgo_bridge_storage_bridge_linux.go` |

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
| ART index | `storage/shard.go` — 8192-shard map, ~9.6 ns lookup, zero-alloc |
| C++ VLog reader | `storage/vlog.go:ReadValue` |
| LIRS cache | `storage/cache.go` + `storage/cache_sharded.go` |
| Defragmenter | `storage/defrag.go` |
| Priority scheduler | none — Go uses ordinary pread/pwrite |

## Before you argue for wiring any of it up

The Go read path was optimised in this branch and is now substantially faster
than when the C++ was written: cache-hit reads went 711 ns → 92 ns and index
lookups 19.4 ns → 9.6 ns with zero allocations. An ART binding has to beat
**9.6 ns plus cgo call overhead** (~50-100 ns per crossing), which is a much
harder target than it was against the original numbers. Batch across the cgo
boundary or do not cross it.

So: measure the Go path first, and only reach for C++ where a benchmark shows
Go cannot get there.

## Build and CI

- `scripts/build.sh` (Linux, without `--go-only`) builds the CMake target and
  links it with `CGO_ENABLED=1`.
- The **published Docker image is `CGO_ENABLED=0`** and contains no C++ at all.
  Do not attribute its measured behaviour to this directory.
- CI job `node-6-cpp` compiles the CMake target, the cgo shims and
  `scripts/build.sh`. Every other job is `CGO_ENABLED=0`.

## Recommendation

Decide one of these, rather than leaving it ambiguous a third time:

1. **Delete the unreachable set** (~5,400 lines; `git log` keeps it). Correct
   if nobody is going to write the bindings this quarter. Nothing else depends
   on it — the four reachable files include none of it.
2. **Wire one component and benchmark it** against the Go path it replaces. If
   it does not win by a margin worth the cgo complexity, delete it.

What should not happen is a third year of compiling code nothing calls while
the README advertises it as a shipped feature.
