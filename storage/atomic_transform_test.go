package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Regressions: the atomic ops (CAS / INCR / DECR / SETNX) used to bypass the
// value-transform pipeline (invariant 29). persistAtomicKVSep wrote raw
// plaintext with no FlagCompressed / FlagEncrypted and no WAL XformFlags /
// DiskValueLen, and readUnderShardLockKVSep never decrypted or decompressed —
// so --encrypt-at-rest stored atomic-op values in the clear, and an atomic op
// on a Put-written transformed value misread it on a cache miss.
//
// Not parallel: encryption and the compression algorithm are process-global.

type atomicXformMode struct {
	name     string
	compress bool
	encrypt  bool
}

var atomicXformModes = []atomicXformMode{
	{"encrypt", false, true},
	{"compress", true, false},
	{"compress+encrypt", true, true},
}

func atomicXformEngine(t *testing.T, dir string, m atomicXformMode) *StorageEngine {
	t.Helper()
	cfg := testStorageConfig(dir)
	cfg.DefragInterval = time.Hour // keep GC from relocating records mid-test
	cfg.Compression = "none"
	if m.compress {
		cfg.Compression = "zstd"
	}
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	<-se.ReplayDone
	return se
}

// atomicPayload is a value that is both unique (greppable on disk) and, when
// the mode compresses, long and redundant enough to pass the 256 B threshold.
func atomicPayload(m atomicXformMode, tag string) []byte {
	if m.compress {
		return []byte(strings.Repeat("ATOMIC-PLAINTEXT-"+tag+"|", 40))
	}
	return []byte("ATOMIC-PLAINTEXT-" + tag)
}

// wantXformFlags is what the IndexEntry must carry for a value v in mode m.
func wantXformFlags(m atomicXformMode, v []byte) uint8 {
	var f uint8
	if m.compress && len(v) >= compressionThreshold {
		f |= FlagCompressed
	}
	if m.encrypt {
		f |= FlagEncrypted
	}
	return f
}

func checkEntryXform(t *testing.T, se *StorageEngine, key string, m atomicXformMode, plain []byte) {
	t.Helper()
	e, _, ok := se.index.get(key)
	if !ok {
		t.Fatalf("%s: no index entry", key)
	}
	want := wantXformFlags(m, plain)
	if got := e.Flags & (FlagCompressed | FlagEncrypted); got != want {
		t.Errorf("%s: transform flags = 0x%02x, want 0x%02x", key, got, want)
	}
	if e.UncompressedSize != uint32(len(plain)) {
		t.Errorf("%s: UncompressedSize = %d, want plaintext length %d", key, e.UncompressedSize, len(plain))
	}
	if want != 0 && e.ValueSize == uint32(len(plain)) {
		t.Errorf("%s: ValueSize = %d = plaintext length on a transformed record", key, e.ValueSize)
	}
	if e.CRC32C != computeCRC32C(plain) {
		t.Errorf("%s: CRC32C is not the plaintext CRC", key)
	}
}

func mustGet(t *testing.T, se *StorageEngine, key string, want []byte, when string) {
	t.Helper()
	se.cache.Evict(key) // force the VLog read + inverse transform
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("%s: Get %s: %v", when, key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: Get %s = %d bytes %.40q…, want %d bytes %.40q…", when, key, len(got), got, len(want), want)
	}
}

// Atomic writes must be transformed on disk exactly like Put.
func TestAtomicOps_TransformValuesOnDisk(t *testing.T) {
	for _, m := range atomicXformModes {
		t.Run(m.name, func(t *testing.T) {
			if m.encrypt {
				enableTestEncryption(t)
			}
			dir := t.TempDir()
			se := atomicXformEngine(t, dir, m)

			nx := atomicPayload(m, "setnx")
			if r, err := se.SetIfNotExists("nx", nx, -1); err != nil || r != SetNXCreated {
				t.Fatalf("SETNX = %v, %v", r, err)
			}
			casOld, casNew := atomicPayload(m, "cas-old"), atomicPayload(m, "cas-new")
			if r, err := se.SetIfNotExists("cas", casOld, -1); err != nil || r != SetNXCreated {
				t.Fatalf("SETNX cas = %v, %v", r, err)
			}
			if r, err := se.CompareAndSwap("cas", casOld, casNew, -1); err != nil || r != CASSuccess {
				t.Fatalf("CAS = %v, %v", r, err)
			}
			const ctr = int64(918273645546372819)
			if n, err := se.Increment("ctr", ctr, -1); err != nil || n != ctr {
				t.Fatalf("INCR = %d, %v", n, err)
			}
			ctrBytes := []byte(strconv.FormatInt(ctr, 10))

			checkEntryXform(t, se, "nx", m, nx)
			checkEntryXform(t, se, "cas", m, casNew)
			checkEntryXform(t, se, "ctr", m, ctrBytes)
			mustGet(t, se, "nx", nx, "live")
			mustGet(t, se, "cas", casNew, "live")
			mustGet(t, se, "ctr", ctrBytes, "live")

			if err := se.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "vlog_active.dat"))
			if err != nil {
				t.Fatalf("read vlog: %v", err)
			}
			for name, plain := range map[string][]byte{"setnx": nx, "cas-new": casNew, "cas-old": casOld} {
				if bytes.Contains(raw, plain) {
					t.Errorf("plaintext of the %s value is in vlog_active.dat — atomic op skipped transformForWrite", name)
				}
			}
			if m.encrypt && bytes.Contains(raw, ctrBytes) {
				t.Errorf("INCR counter %s is in vlog_active.dat in plaintext under encryption", ctrBytes)
			}
		})
	}
}

// Mixing Put and atomic ops on one key, across cache misses and restarts
// (clean Close → checkpoint, and dirty kill → raw WAL replay).
func TestAtomicOps_MixedWithPutAcrossRestart(t *testing.T) {
	for _, m := range atomicXformModes {
		for _, dirty := range []bool{false, true} {
			name := m.name + "/clean-restart"
			if dirty {
				name = m.name + "/dirty-restart"
			}
			t.Run(name, func(t *testing.T) {
				if m.encrypt {
					enableTestEncryption(t)
				}
				dir := t.TempDir()
				se := atomicXformEngine(t, dir, m)

				// Put → INCR on a cold cache: INCR must decrypt what Put wrote.
				if err := se.Put("ctr", []byte("100"), -1); err != nil {
					t.Fatalf("Put: %v", err)
				}
				se.cache.Evict("ctr")
				if n, err := se.Increment("ctr", 5, -1); err != nil || n != 105 {
					t.Fatalf("INCR after Put on a cache miss = %d, %v; want 105", n, err)
				}
				// INCR-created → DECR on a cold cache.
				if n, err := se.Increment("ctr2", 7, -1); err != nil || n != 7 {
					t.Fatalf("INCR new = %d, %v", n, err)
				}
				se.cache.Evict("ctr2")
				if n, err := se.Decrement("ctr2", 2, -1); err != nil || n != 5 {
					t.Fatalf("DECR on a cache miss = %d, %v; want 5", n, err)
				}

				// Put (transformed) → CAS on a cold cache: the comparison must see plaintext.
				v1, v2, v3 := atomicPayload(m, "v1"), atomicPayload(m, "v2"), atomicPayload(m, "v3")
				if err := se.Put("k", v1, -1); err != nil {
					t.Fatalf("Put k: %v", err)
				}
				se.cache.Evict("k")
				if r, err := se.CompareAndSwap("k", v1, v2, -1); err != nil || r != CASSuccess {
					t.Fatalf("CAS after Put on a cache miss = %v, %v; want CASSuccess", r, err)
				}
				// SETNX-written → read by Get on a cold cache, and Put over it.
				nx := atomicPayload(m, "nx")
				if r, err := se.SetIfNotExists("nx", nx, -1); err != nil || r != SetNXCreated {
					t.Fatalf("SETNX = %v, %v", r, err)
				}
				mustGet(t, se, "nx", nx, "before restart")
				mustGet(t, se, "k", v2, "before restart")
				mustGet(t, se, "ctr", []byte("105"), "before restart")

				if dirty {
					abandonEngineForCrashTest(se) // simulated kill: no checkpoint
				} else if err := se.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				se2 := atomicXformEngine(t, dir, m)
				t.Cleanup(func() { se2.Close() })

				mustGet(t, se2, "ctr", []byte("105"), "after restart")
				mustGet(t, se2, "ctr2", []byte("5"), "after restart")
				mustGet(t, se2, "k", v2, "after restart")
				mustGet(t, se2, "nx", nx, "after restart")
				checkEntryXform(t, se2, "k", m, v2)
				checkEntryXform(t, se2, "nx", m, nx)

				// Atomic ops on replayed entries, cold cache.
				se2.cache.Evict("ctr")
				if n, err := se2.Increment("ctr", 1, -1); err != nil || n != 106 {
					t.Fatalf("INCR after restart = %d, %v; want 106", n, err)
				}
				se2.cache.Evict("k")
				if r, err := se2.CompareAndSwap("k", v2, v3, -1); err != nil || r != CASSuccess {
					t.Fatalf("CAS after restart = %v, %v; want CASSuccess", r, err)
				}
				se2.cache.Evict("nx")
				if r, err := se2.SetIfNotExists("nx", []byte("other"), -1); err != nil || r != SetNXExists {
					t.Fatalf("SETNX on an existing key after restart = %v, %v", r, err)
				}
				// Atomic-written → Put → Get.
				if err := se2.Put("ctr2", []byte(fmt.Sprint(42)), -1); err != nil {
					t.Fatalf("Put over INCR key: %v", err)
				}
				mustGet(t, se2, "ctr2", []byte("42"), "Put after INCR")
				mustGet(t, se2, "k", v3, "CAS after restart")
				mustGet(t, se2, "ctr", []byte("106"), "INCR after restart")
			})
		}
	}
}
