/*
 * cgo_batch_impl_linux.cpp — CGO compile shim for the batch engine.
 *
 * The `_linux` filename suffix causes the Go toolchain to include this file
 * ONLY when building for GOOS=linux, preventing compilation errors on macOS
 * (which lacks liburing and C++17 thread affinity headers).
 *
 * This file does nothing but #include the real implementation from cpp/src/
 * so that `go build` (CGO_ENABLED=1) on Linux compiles the C++ batch engine
 * automatically as part of the storage package — no separate CMake step needed.
 *
 * The CXXFLAGS in cgo_bridge.go apply to this file because CGO propagates
 * package-level cgo flags to all C/C++ sources in the same directory.
 */
#include "../cpp/src/batch_engine.cpp"
