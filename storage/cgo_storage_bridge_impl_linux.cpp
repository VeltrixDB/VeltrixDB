/*
 * cgo_storage_bridge_impl_linux.cpp — CGO compile shim for the storage bridge.
 *
 * The _linux filename suffix restricts compilation to GOOS=linux, matching the
 * sibling shims for batch_engine, uring_reader and numa_topology.
 *
 * WHY THIS EXISTS
 *
 * cgo_bridge_storage_bridge_linux.go declares the veltrix_bridge_* C API via
 * storage_bridge_capi.h, but nothing compiled its implementation into the Go
 * package — storage_bridge.cpp lived only in cpp/CMakeLists.txt, whose static
 * library is linked by scripts/build.sh and by nothing else. So any plain
 * `CGO_ENABLED=1 go build ./storage/...` (or `go test -race`, which requires
 * cgo) failed at link time with:
 *
 *     undefined reference to `veltrix_bridge_create'
 *     undefined reference to `veltrix_bridge_submit_batch'
 *     ...
 *
 * Compiling it here, like the other three, makes the Go package
 * self-contained under cgo.
 *
 * storage_bridge.cpp MUST therefore be absent from cpp/CMakeLists.txt — being
 * in both would define these symbols twice when scripts/build.sh links the
 * static library alongside the cgo objects.
 *
 * CXXFLAGS/LDFLAGS are inherited from cgo_bridge_storage_bridge_linux.go
 * (-luring -lstdc++ -lpthread).
 */
#include "../cpp/src/storage_bridge.cpp"
