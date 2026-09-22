package storage

import (
	"bytes"
	"fmt"
	"testing"
)

// These tests reproduce the two symptoms a pre-fix build left behind and prove
// the repair resolves both.
//
// The damage is injected directly into the index rather than by running old
// code, because the old code no longer exists. Each helper reproduces exactly
// what the buggy replay path produced — see repair.go for the derivation.

// damageCleanShutdown reproduces what a CLEAN restart used to leave behind:
// the checkpoint wrote entry.ValueSize into the valueLen field, so ValueSize
// came back correct, but the transform flags were lost and UncompressedSize
// was overwritten with the on-disk length.
//
// Symptom: reads SUCCEED and silently return the raw blob.
func damageCleanShutdown(t *testing.T, se *StorageEngine, key string) {
	t.Helper()
	shard, _ := se.index.shardFor(key)
	shard.mu.Lock()
	e, ok := shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		t.Fatalf("damageCleanShutdown: key %q not in index", key)
	}
	e.UncompressedSize = e.ValueSize // lost: the real plaintext length
	e.Flags &^= FlagCompressed | FlagEncrypted
	shard.mu.Unlock()
	se.cache.Evict(key) // force the next Get down to the VLog
}

// damageCrashRestart reproduces what a CRASH restart used to leave behind: the
// WAL recorded the plaintext length, so ValueSize came back as the plaintext
// length rather than the on-disk blob length, and the flags were lost.
//
// Symptom: reads FAIL with a CRC32C mismatch.
func damageCrashRestart(t *testing.T, se *StorageEngine, key string, plaintextLen int) {
	t.Helper()
	shard, _ := se.index.shardFor(key)
	shard.mu.Lock()
	e, ok := shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		t.Fatalf("damageCrashRestart: key %q not in index", key)
	}
	e.ValueSize = uint32(plaintextLen)
	e.UncompressedSize = uint32(plaintextLen)
	e.Flags &^= FlagCompressed | FlagEncrypted
	shard.mu.Unlock()
	se.cache.Evict(key)
}

func TestRepair_CleanShutdownSymptom_SilentWrongData(t *testing.T) {
	dir := t.TempDir()
	se := transformTestEngine(t, dir)
	t.Cleanup(func() { se.Close() })

	want := map[string][]byte{}
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("clean-%d", i)
		v := compressibleValue(fmt.Sprintf("c%d", i))
		if err := se.Put(k, v, -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[k] = v
	}
	for k := range want {
		damageCleanShutdown(t, se, k)
	}

	// Establish the symptom: reads succeed but hand back the wrong bytes.
	silentlyWrong := 0
	for k, v := range want {
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("expected a silent wrong-data read for %s, got error: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			silentlyWrong++
		}
	}
	if silentlyWrong != len(want) {
		t.Fatalf("expected all %d keys to return wrong data before repair, got %d",
			len(want), silentlyWrong)
	}

	// Scan must find exactly these and change nothing.
	scan, err := se.RepairTransformMetadata(true)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if scan.Repaired != len(want) {
		t.Errorf("scan reported %d repairable, want %d (%s)", scan.Repaired, len(want), scan)
	}
	if scan.HasUnresolved() {
		t.Errorf("scan reported unresolved entries: %s", scan)
	}
	for k := range want {
		se.cache.Evict(k)
		if got, err := se.Get(k); err == nil && bytes.Equal(got, want[k]) {
			t.Fatalf("dry-run scan repaired key %s — it must not mutate", k)
		}
	}

	// Repair, then every key must read back correctly.
	rep, err := se.RepairTransformMetadata(false)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if rep.Repaired != len(want) {
		t.Errorf("repaired %d, want %d (%s)", rep.Repaired, len(want), rep)
	}
	if rep.RepairedCompressed != len(want) {
		t.Errorf("RepairedCompressed=%d, want %d", rep.RepairedCompressed, len(want))
	}
	for k, v := range want {
		se.cache.Evict(k)
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("Get %s after repair: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s after repair: got %d bytes, want %d", k, len(got), len(v))
		}
	}
}

func TestRepair_CrashRestartSymptom_CRCFailure(t *testing.T) {
	dir := t.TempDir()
	se := transformTestEngine(t, dir)
	t.Cleanup(func() { se.Close() })

	want := map[string][]byte{}
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("crash-%d", i)
		v := compressibleValue(fmt.Sprintf("x%d", i))
		if err := se.Put(k, v, -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[k] = v
	}
	for k, v := range want {
		damageCrashRestart(t, se, k, len(v))
	}

	// Establish the symptom: reads fail outright.
	for k := range want {
		if _, err := se.Get(k); err == nil {
			t.Fatalf("expected a read error for %s before repair", k)
		}
	}

	rep, err := se.RepairTransformMetadata(false)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if rep.Repaired != len(want) {
		t.Errorf("repaired %d, want %d (%s)", rep.Repaired, len(want), rep)
	}
	for k, v := range want {
		se.cache.Evict(k)
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("Get %s after repair: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s after repair: got %d bytes, want %d", k, len(got), len(v))
		}
	}
}

func TestRepair_EncryptedRecords(t *testing.T) {
	enableTestEncryption(t)
	dir := t.TempDir()
	se := transformTestEngine(t, dir)
	t.Cleanup(func() { se.Close() })

	want := map[string][]byte{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("enc-%d", i)
		v := compressibleValue(fmt.Sprintf("e%d", i)) // compressed AND encrypted
		if err := se.Put(k, v, -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[k] = v
	}
	for k := range want {
		damageCleanShutdown(t, se, k)
	}

	rep, err := se.RepairTransformMetadata(false)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if rep.Repaired != len(want) {
		t.Fatalf("repaired %d, want %d (%s)", rep.Repaired, len(want), rep)
	}
	if rep.RepairedEncrypted != len(want) || rep.RepairedCompressed != len(want) {
		t.Errorf("expected all %d keys to be both compressed and encrypted, got "+
			"compressed=%d encrypted=%d", len(want), rep.RepairedCompressed, rep.RepairedEncrypted)
	}
	for k, v := range want {
		se.cache.Evict(k)
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("Get %s after repair: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s after repair: mismatch", k)
		}
	}
}

// Healthy data must be left strictly alone, and a second pass must be a no-op.
func TestRepair_IsIdempotentAndLeavesHealthyDataAlone(t *testing.T) {
	dir := t.TempDir()
	se := transformTestEngine(t, dir)
	t.Cleanup(func() { se.Close() })

	want := map[string][]byte{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("healthy-%d", i)
		v := compressibleValue(fmt.Sprintf("h%d", i))
		if err := se.Put(k, v, -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[k] = v
	}
	// Also cover values below the compression threshold, which are stored raw.
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("tiny-%d", i)
		v := []byte(fmt.Sprintf("small-value-%d", i))
		if err := se.Put(k, v, -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[k] = v
	}

	first, err := se.RepairTransformMetadata(false)
	if err != nil {
		t.Fatalf("repair pass 1: %v", err)
	}
	if first.Repaired != 0 {
		t.Errorf("pass 1 repaired %d healthy entries — must be 0 (%s)", first.Repaired, first)
	}
	if first.AlreadyHealthy != len(want) {
		t.Errorf("pass 1 AlreadyHealthy=%d, want %d (%s)", first.AlreadyHealthy, len(want), first)
	}

	// Damage, repair, then confirm a third pass finds nothing left to do.
	for k := range want {
		damageCleanShutdown(t, se, k)
	}
	if _, err := se.RepairTransformMetadata(false); err != nil {
		t.Fatalf("repair pass 2: %v", err)
	}
	third, err := se.RepairTransformMetadata(false)
	if err != nil {
		t.Fatalf("repair pass 3: %v", err)
	}
	if third.Repaired != 0 {
		t.Errorf("pass 3 repaired %d — repair is not idempotent (%s)", third.Repaired, third)
	}

	for k, v := range want {
		se.cache.Evict(k)
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s: mismatch after damage+repair", k)
		}
	}
}

// An entry whose plaintext CRC matches nothing must be reported, not guessed
// at. This is the property that makes the repair safe to run unattended.
func TestRepair_LeavesUnresolvableEntriesUntouched(t *testing.T) {
	dir := t.TempDir()
	se := transformTestEngine(t, dir)
	t.Cleanup(func() { se.Close() })

	const key = "unresolvable"
	val := compressibleValue("u")
	if err := se.Put(key, val, -1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Corrupt the oracle itself: no interpretation of the blob can hash to
	// this, so the repair has no basis on which to act.
	shard, _ := se.index.shardFor(key)
	shard.mu.Lock()
	before := *shard.entries[key]
	shard.entries[key].CRC32C = 0xDEADBEEF
	shard.entries[key].Flags &^= FlagCompressed | FlagEncrypted
	shard.mu.Unlock()
	se.cache.Evict(key)

	rep, err := se.RepairTransformMetadata(false)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if rep.Unresolved != 1 {
		t.Errorf("Unresolved=%d, want 1 (%s)", rep.Unresolved, rep)
	}
	if rep.Repaired != 0 {
		t.Errorf("repaired %d entries despite no CRC match — must be 0 (%s)", rep.Repaired, rep)
	}
	if !rep.HasUnresolved() {
		t.Error("HasUnresolved() = false, want true")
	}

	shard.mu.RLock()
	after := *shard.entries[key]
	shard.mu.RUnlock()
	if after.ValueSize != before.ValueSize || after.Flags != (before.Flags&^(FlagCompressed|FlagEncrypted)) {
		t.Errorf("unresolvable entry was mutated: before=%+v after=%+v", before, after)
	}
}

// resolveBlob is the core of the repair; check its four interpretations
// directly, including that it rejects a blob whose CRC matches nothing.
func TestResolveBlob_AllFourInterpretations(t *testing.T) {
	plain := compressibleValue("resolve")
	wantCRC := computeCRC32C(plain)

	t.Run("raw", func(t *testing.T) {
		c, ok := resolveBlob(plain, wantCRC)
		if !ok || c.flags != 0 {
			t.Fatalf("got ok=%v flags=0x%02x, want ok=true flags=0x00", ok, c.flags)
		}
	})

	t.Run("compressed", func(t *testing.T) {
		blob, did := MaybeCompress(plain, 1)
		if !did {
			t.Skip("value did not compress")
		}
		c, ok := resolveBlob(blob, wantCRC)
		if !ok || c.flags != FlagCompressed {
			t.Fatalf("got ok=%v flags=0x%02x, want ok=true flags=0x%02x", ok, c.flags, FlagCompressed)
		}
		if !bytes.Equal(c.plain, plain) {
			t.Error("recovered plaintext differs from the original")
		}
	})

	t.Run("encrypted", func(t *testing.T) {
		enableTestEncryption(t)
		blob, sealed, err := Encrypt(plain)
		if err != nil || !sealed {
			t.Fatalf("Encrypt: %v sealed=%v", err, sealed)
		}
		c, ok := resolveBlob(blob, wantCRC)
		if !ok || c.flags != FlagEncrypted {
			t.Fatalf("got ok=%v flags=0x%02x, want ok=true flags=0x%02x", ok, c.flags, FlagEncrypted)
		}
	})

	t.Run("compressed+encrypted", func(t *testing.T) {
		enableTestEncryption(t)
		comp, did := MaybeCompress(plain, 1)
		if !did {
			t.Skip("value did not compress")
		}
		blob, sealed, err := Encrypt(comp)
		if err != nil || !sealed {
			t.Fatalf("Encrypt: %v sealed=%v", err, sealed)
		}
		c, ok := resolveBlob(blob, wantCRC)
		if !ok || c.flags != FlagCompressed|FlagEncrypted {
			t.Fatalf("got ok=%v flags=0x%02x, want ok=true flags=0x%02x",
				ok, c.flags, FlagCompressed|FlagEncrypted)
		}
		if !bytes.Equal(c.plain, plain) {
			t.Error("recovered plaintext differs from the original")
		}
	})

	t.Run("no match is rejected", func(t *testing.T) {
		if _, ok := resolveBlob(plain, 0xDEADBEEF); ok {
			t.Error("resolveBlob accepted a blob that matches no CRC")
		}
	})
}

// The startup detector is the only thing standing between a pre-fix node and
// silently serving blobs, so it has to fire on damage and stay quiet without.
func TestCheckTransformMetadataHealth(t *testing.T) {
	dir := t.TempDir()
	se := transformTestEngine(t, dir)
	t.Cleanup(func() { se.Close() })

	keys := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("health-%d", i)
		if err := se.Put(k, compressibleValue(fmt.Sprintf("h%d", i)), -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		keys = append(keys, k)
	}

	checked, damaged := se.CheckTransformMetadataHealth()
	if checked == 0 {
		t.Fatal("checked 0 records — the sampler found nothing to look at")
	}
	if damaged != 0 {
		t.Fatalf("reported %d/%d damaged on healthy data — false positive",
			damaged, checked)
	}

	// Now inflict the exact damage a pre-fix build left behind.
	for _, k := range keys {
		damageCleanShutdown(t, se, k)
	}
	checked2, damaged2 := se.CheckTransformMetadataHealth()
	if damaged2 == 0 {
		t.Fatalf("reported 0/%d damaged after damaging every record — "+
			"the detector would let a corrupt node start silently", checked2)
	}
	t.Logf("detector: %d/%d flagged after damage", damaged2, checked2)

	// And it must clear once repaired.
	if _, err := se.RepairTransformMetadata(false); err != nil {
		t.Fatalf("repair: %v", err)
	}
	if _, damaged3 := se.CheckTransformMetadataHealth(); damaged3 != 0 {
		t.Errorf("still reports %d damaged after repair", damaged3)
	}
}
