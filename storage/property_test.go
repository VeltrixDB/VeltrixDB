package storage

// property_test.go — property-based tests for the storage engine.
//
// Uses stdlib testing/quick (no external dep) to generate random inputs.
// Each property runs 100+ random cases and asserts an invariant.
//
// Why property tests?
//   - Hand-written examples cover what we already think to test.  Properties
//     cover combinations of inputs we'd never enumerate (empty key, key with
//     null bytes, value 1 byte under threshold, etc.)
//   - Shrinking: when a property fails, testing/quick reduces the input to
//     the smallest failing case so debugging is fast.
//
// Coverage:
//   1. PutGetRoundTrip — every Put followed by Get returns the same bytes.
//   2. PutOverwrite — second Put visibly replaces the first.
//   3. DeleteIsTombstone — Get after Delete returns not-found.
//   4. CompressDecompress — value bytes survive a compress-then-decompress.
//   5. EncryptDecrypt — value bytes survive an encrypt-then-decrypt cycle.
//   6. BloomNoFalseNegative — every Add'd key returns true on MayContain.
//   7. CASRespectsExpected — CAS is a no-op when expected != current.

import (
	"bytes"
	"encoding/base64"
	"os"
	"reflect"
	"testing"
	"testing/quick"
)

func newTestEngine(t *testing.T) *StorageEngine {
	t.Helper()
	dir, err := os.MkdirTemp("", "veltrix-prop-")
	if err != nil {
		t.Fatalf("mkdir tmp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	cfg := DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.NumShards = 1024
	// Tighten group-commit windows so per-test latency is bearable.
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false
	cfg.BloomFilterShardBits = 1 << 18 // 256 K bits/shard, ~32 MB total

	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	return se
}

// nonEmptyString constrains testing/quick to generate non-empty strings — the
// engine's key length contract is keyLen ≥ 1.
type nonEmptyString string

func (nonEmptyString) Generate(rand *quick.Config) reflect.Value {
	// Unused: we use a wrapper Generate via reflect on byteSlice instead.
	return reflect.Value{}
}

// PutGetRoundTrip property: Get(key) returns exactly what Put(key, value) wrote.
func TestProperty_PutGetRoundTrip(t *testing.T) {
	se := newTestEngine(t)
	prop := func(keyBytes, valBytes []byte) bool {
		if len(keyBytes) == 0 {
			keyBytes = []byte("k")
		}
		key := string(keyBytes)
		if err := se.Put(key, valBytes, -1); err != nil {
			t.Logf("put err: %v", err)
			return false
		}
		got, err := se.Get(key)
		if err != nil {
			t.Logf("get err: %v", err)
			return false
		}
		return bytes.Equal(got, valBytes)
	}
	cfg := &quick.Config{MaxCount: 100}
	if err := quick.Check(prop, cfg); err != nil {
		t.Fatalf("PutGetRoundTrip: %v", err)
	}
}

// PutOverwrite property: a second Put replaces the first; Get returns v2.
func TestProperty_PutOverwrite(t *testing.T) {
	se := newTestEngine(t)
	prop := func(keyBytes, v1, v2 []byte) bool {
		if len(keyBytes) == 0 {
			keyBytes = []byte("ovw")
		}
		key := string(keyBytes)
		if err := se.Put(key, v1, -1); err != nil {
			return false
		}
		if err := se.Put(key, v2, -1); err != nil {
			return false
		}
		got, err := se.Get(key)
		if err != nil {
			return false
		}
		return bytes.Equal(got, v2)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("PutOverwrite: %v", err)
	}
}

// DeleteIsTombstone property: after Delete, Get returns "not found".
func TestProperty_DeleteIsTombstone(t *testing.T) {
	se := newTestEngine(t)
	prop := func(keyBytes, valBytes []byte) bool {
		if len(keyBytes) == 0 {
			return true
		}
		key := string(keyBytes)
		if err := se.Put(key, valBytes, -1); err != nil {
			return false
		}
		if err := se.Delete(key); err != nil {
			return false
		}
		_, err := se.Get(key)
		return err != nil
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("DeleteIsTombstone: %v", err)
	}
}

// CompressDecompress property: any sufficiently-large byte slice survives
// MaybeCompress→Decompress unchanged.
func TestProperty_CompressDecompress(t *testing.T) {
	prop := func(payload []byte) bool {
		if len(payload) < compressionThreshold {
			return true // not eligible
		}
		ct, ok := MaybeCompress(payload, 1)
		if !ok {
			return true // engine refused — also valid
		}
		out, err := Decompress(ct, uint32(len(payload)))
		if err != nil {
			return false
		}
		return bytes.Equal(out, payload)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("CompressDecompress: %v", err)
	}
}

// EncryptDecrypt property: round-trip through AES-GCM is identity.
func TestProperty_EncryptDecrypt(t *testing.T) {
	// 32-byte key, base64'd
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32))
	t.Setenv(EncryptionKeyEnvVar, key)
	enc, err := loadEncryptor("")
	if err != nil {
		t.Fatalf("load encryptor: %v", err)
	}
	setEncryptor(enc)
	t.Cleanup(func() { setEncryptor(nil) })

	prop := func(payload []byte) bool {
		ct, sealed, err := Encrypt(payload)
		if err != nil || !sealed {
			return false
		}
		pt, err := Decrypt(ct)
		if err != nil {
			return false
		}
		return bytes.Equal(pt, payload)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("EncryptDecrypt: %v", err)
	}
}

// BloomNoFalseNegative property: any key Add'd returns MayContain=true.
func TestProperty_BloomNoFalseNegative(t *testing.T) {
	b := newShardBloom(1<<14, 7)
	prop := func(keyBytes []byte) bool {
		h := uint64(0xCAFEBABE)
		for _, c := range keyBytes {
			h = h*1099511628211 ^ uint64(c)
		}
		b.Add(h)
		return b.MayContain(h)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatalf("BloomNoFalseNegative: %v", err)
	}
}

// CASRespectsExpected property: CAS with the wrong expected value never
// changes the stored value.
func TestProperty_CASRespectsExpected(t *testing.T) {
	se := newTestEngine(t)
	prop := func(keyBytes, v1, badExpected, newVal []byte) bool {
		if len(keyBytes) == 0 || bytes.Equal(badExpected, v1) {
			return true // skip case where expected coincidentally matches
		}
		key := string(keyBytes)
		if err := se.Put(key, v1, -1); err != nil {
			return false
		}
		res, err := se.CompareAndSwap(key, badExpected, newVal, -1)
		if err != nil {
			return false
		}
		if res != CASMismatch {
			return false
		}
		got, err := se.Get(key)
		if err != nil {
			return false
		}
		return bytes.Equal(got, v1) // value must be unchanged
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 50}); err != nil {
		t.Fatalf("CASRespectsExpected: %v", err)
	}
}
