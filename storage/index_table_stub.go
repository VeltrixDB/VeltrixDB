//go:build !cgo || !go1.21

package storage

// nativeIndexAvailable is false on builds without cgo: the C++ table cannot
// be linked, so every shard uses mapTable. (go1.21 matches the other cgo
// bindings: it is the floor for unsafe.StringData / SliceData.)
const nativeIndexAvailable = false

func newNativeTable() entryTable { return newMapTable() }

// NativeIndexBytes is always 0 without the native index.
func NativeIndexBytes() uint64 { return 0 }
