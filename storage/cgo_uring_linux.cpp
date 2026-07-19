/*
 * cgo_uring_linux.cpp — CGO compile shim for UringReader.
 *
 * The _linux suffix restricts compilation to GOOS=linux so that macOS dev
 * builds never attempt to include liburing headers.
 *
 * CXXFLAGS are inherited from cgo_bridge_pinner.go / cgo_bridge.go:
 *   -std=c++17 -O3 -I${SRCDIR}/../cpp/include
 * LDFLAGS add: -luring  (set in cgo_bridge_pinner.go)
 */
#include "../cpp/src/uring_reader.cpp"
