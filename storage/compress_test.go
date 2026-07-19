package storage

// compress_test.go — unit tests for value compression helpers.
//
// Tests exercise MaybeCompress and Decompress directly (no engine required).

import (
	"bytes"
	"math/rand"
	"strings"
	"sync"
	"testing"
)

// TestCompress_SmallValueSkipped verifies that values below the compression
// threshold are not compressed.  The function must return (original, false).
func TestCompress_SmallValueSkipped(t *testing.T) {
	// Values strictly below compressionThreshold.
	for _, n := range []int{0, 1, 64, compressionThreshold - 1} {
		val := make([]byte, n)
		out, ok := MaybeCompress(val, 1)
		if ok {
			t.Errorf("size=%d: expected ok=false for sub-threshold value, got true", n)
		}
		if !bytes.Equal(out, val) {
			t.Errorf("size=%d: MaybeCompress returned different bytes when not compressing", n)
		}
	}
}

// TestCompress_LargeValueRoundTrip verifies that a 10 KB compressible value
// survives compress → decompress with identical bytes.
func TestCompress_LargeValueRoundTrip(t *testing.T) {
	// Highly compressible: repeated pattern.
	val := []byte(strings.Repeat("veltrixdb-compress-test-data ", 350)) // ~10 KB
	if len(val) < compressionThreshold {
		t.Fatalf("test value too short: %d", len(val))
	}

	ct, ok := MaybeCompress(val, 1)
	if !ok {
		t.Fatalf("MaybeCompress returned ok=false for 10KB compressible value")
	}

	out, err := Decompress(ct, uint32(len(val)))
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(out, val) {
		t.Errorf("round-trip failed: len(out)=%d len(val)=%d", len(out), len(val))
	}
}

// TestCompress_AlgorithmPrefix verifies that the compressed output starts with
// a known algorithm byte (compAlgoFlate = 0x01).
func TestCompress_AlgorithmPrefix(t *testing.T) {
	// The write-path algorithm is a process-global that any engine-creating
	// test can flip (NewStorageEngine installs cfg.Compression — "zstd" in
	// DefaultStorageConfig). Pin flate for this assertion and restore.
	if err := SetCompressionAlgo("flate"); err != nil {
		t.Fatalf("SetCompressionAlgo(flate): %v", err)
	}
	t.Cleanup(func() { _ = SetCompressionAlgo("zstd") })
	val := []byte(strings.Repeat("abcdefghijklmnop", 20)) // 320 bytes, compressible
	ct, ok := MaybeCompress(val, 1)
	if !ok {
		t.Fatalf("MaybeCompress returned ok=false")
	}
	if len(ct) == 0 {
		t.Fatal("compressed output is empty")
	}
	if ct[0] != compAlgoFlate {
		t.Errorf("algorithm prefix = 0x%02x, want 0x%02x (flate)", ct[0], compAlgoFlate)
	}
}

// TestCompress_IncompressibleData verifies that random bytes (high entropy)
// may not compress — MaybeCompress should return ok=false when the compressed
// output is not meaningfully smaller than the input.
func TestCompress_IncompressibleData(t *testing.T) {
	// Fill with pseudo-random bytes — high entropy, essentially incompressible.
	rng := rand.New(rand.NewSource(42))
	val := make([]byte, 1024)
	rng.Read(val)

	ct, ok := MaybeCompress(val, 1)
	if ok {
		// If it did compress (unusual for random bytes) verify round-trip.
		out, err := Decompress(ct, uint32(len(val)))
		if err != nil {
			t.Fatalf("Decompress of compressed random data: %v", err)
		}
		if !bytes.Equal(out, val) {
			t.Error("round-trip of compressed random data failed")
		}
	} else {
		// ok=false is the normal path for high-entropy data.
		// The returned slice must equal the original (no mutation).
		if !bytes.Equal(ct, val) {
			t.Error("MaybeCompress(ok=false) returned different bytes than input")
		}
	}
}

// TestDecompress_EmptyInput verifies that Decompress returns an error on an
// empty blob (no algorithm byte present).
func TestDecompress_EmptyInput(t *testing.T) {
	_, err := Decompress([]byte{}, 0)
	if err == nil {
		t.Error("expected error from Decompress on empty input, got nil")
	}
}

// TestDecompress_UnknownAlgo verifies that an unknown algorithm prefix byte
// causes Decompress to return a descriptive error.
func TestDecompress_UnknownAlgo(t *testing.T) {
	// 0xFF is not a registered algorithm.
	blob := []byte{0xFF, 0x01, 0x02, 0x03}
	_, err := Decompress(blob, 4)
	if err == nil {
		t.Error("expected error for unknown algorithm byte, got nil")
	}
}

// withCompressionAlgo switches the package-level write algorithm for the
// duration of a test and restores the flate default afterwards so tests do
// not leak state into each other.
func withCompressionAlgo(t *testing.T, algo string) {
	t.Helper()
	if err := SetCompressionAlgo(algo); err != nil {
		t.Fatalf("SetCompressionAlgo(%q): %v", algo, err)
	}
	t.Cleanup(func() {
		if err := SetCompressionAlgo("flate"); err != nil {
			t.Fatalf("SetCompressionAlgo(restore flate): %v", err)
		}
	})
}

// TestCompress_ZstdRoundTrip verifies that with zstd selected, a compressible
// value gets the compAlgoZstd prefix and survives compress → decompress.
func TestCompress_ZstdRoundTrip(t *testing.T) {
	withCompressionAlgo(t, "zstd")

	val := []byte(strings.Repeat("veltrixdb-zstd-round-trip-data ", 350)) // ~10 KB
	ct, ok := MaybeCompress(val, 1)
	if !ok {
		t.Fatalf("MaybeCompress returned ok=false for 10KB compressible value under zstd")
	}
	if ct[0] != compAlgoZstd {
		t.Fatalf("algorithm prefix = 0x%02x, want 0x%02x (zstd)", ct[0], compAlgoZstd)
	}
	if len(ct) >= len(val) {
		t.Errorf("zstd output not smaller: %d >= %d", len(ct), len(val))
	}
	out, err := Decompress(ct, uint32(len(val)))
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(out, val) {
		t.Errorf("zstd round-trip failed: len(out)=%d len(val)=%d", len(out), len(val))
	}
}

// TestCompress_MixedAlgorithmDecode verifies the on-disk compatibility story:
// a blob written while flate was active decodes after switching to zstd, and
// a blob written while zstd was active decodes after switching back to flate.
// Decompress dispatches on the 1-byte prefix, never on the active setting.
func TestCompress_MixedAlgorithmDecode(t *testing.T) {
	val := []byte(strings.Repeat("mixed-algorithm-store-data ", 40)) // ~1 KB

	// Write with flate active.
	withCompressionAlgo(t, "flate")
	flateBlob, ok := MaybeCompress(val, 1)
	if !ok || flateBlob[0] != compAlgoFlate {
		t.Fatalf("flate write failed: ok=%v prefix=0x%02x", ok, flateBlob[0])
	}

	// Write with zstd active.
	if err := SetCompressionAlgo("zstd"); err != nil {
		t.Fatalf("SetCompressionAlgo(zstd): %v", err)
	}
	zstdBlob, ok := MaybeCompress(val, 1)
	if !ok || zstdBlob[0] != compAlgoZstd {
		t.Fatalf("zstd write failed: ok=%v prefix=0x%02x", ok, zstdBlob[0])
	}

	// Old flate data must decode while zstd is the active write algorithm.
	out, err := Decompress(flateBlob, uint32(len(val)))
	if err != nil {
		t.Fatalf("Decompress flate blob under zstd config: %v", err)
	}
	if !bytes.Equal(out, val) {
		t.Error("flate blob round-trip failed under zstd config")
	}

	// New zstd data must decode after rolling the config back to flate.
	if err := SetCompressionAlgo("flate"); err != nil {
		t.Fatalf("SetCompressionAlgo(flate): %v", err)
	}
	out, err = Decompress(zstdBlob, uint32(len(val)))
	if err != nil {
		t.Fatalf("Decompress zstd blob under flate config: %v", err)
	}
	if !bytes.Equal(out, val) {
		t.Error("zstd blob round-trip failed under flate config")
	}
}

// TestCompress_ZstdSmallValueSkipped verifies the 256-byte threshold applies
// to zstd exactly as it does to flate.
func TestCompress_ZstdSmallValueSkipped(t *testing.T) {
	withCompressionAlgo(t, "zstd")
	for _, n := range []int{0, 1, 64, compressionThreshold - 1} {
		val := make([]byte, n)
		out, ok := MaybeCompress(val, 1)
		if ok {
			t.Errorf("size=%d: expected ok=false for sub-threshold value under zstd, got true", n)
		}
		if !bytes.Equal(out, val) {
			t.Errorf("size=%d: MaybeCompress returned different bytes when not compressing", n)
		}
	}
}

// TestCompress_ZstdIncompressibleData verifies the savings floor holds for
// zstd: high-entropy data either compresses with a valid round-trip or is
// returned unchanged with ok=false.
func TestCompress_ZstdIncompressibleData(t *testing.T) {
	withCompressionAlgo(t, "zstd")
	rng := rand.New(rand.NewSource(43))
	val := make([]byte, 1024)
	rng.Read(val)

	ct, ok := MaybeCompress(val, 1)
	if ok {
		out, err := Decompress(ct, uint32(len(val)))
		if err != nil {
			t.Fatalf("Decompress of compressed random data: %v", err)
		}
		if !bytes.Equal(out, val) {
			t.Error("round-trip of compressed random data failed")
		}
	} else if !bytes.Equal(ct, val) {
		t.Error("MaybeCompress(ok=false) returned different bytes than input")
	}
}

// TestSetCompressionAlgo_Selection verifies valid names are accepted and
// unknown names error (startup must fail, not silently fall back).
func TestSetCompressionAlgo_Selection(t *testing.T) {
	t.Cleanup(func() {
		if err := SetCompressionAlgo("flate"); err != nil {
			t.Fatalf("SetCompressionAlgo(restore flate): %v", err)
		}
	})

	for _, algo := range []string{"", "none", "flate", "zstd"} {
		if err := SetCompressionAlgo(algo); err != nil {
			t.Errorf("SetCompressionAlgo(%q): unexpected error %v", algo, err)
		}
	}
	for _, algo := range []string{"snappy", "lz4", "gzip", "ZSTD"} {
		if err := SetCompressionAlgo(algo); err == nil {
			t.Errorf("SetCompressionAlgo(%q): expected error, got nil", algo)
		}
	}

	// "none" disables the write path entirely, even above the threshold.
	if err := SetCompressionAlgo("none"); err != nil {
		t.Fatalf("SetCompressionAlgo(none): %v", err)
	}
	val := []byte(strings.Repeat("x", 4096))
	out, ok := MaybeCompress(val, 1)
	if ok {
		t.Error("MaybeCompress compressed with algo=none")
	}
	if !bytes.Equal(out, val) {
		t.Error("MaybeCompress(algo=none) returned different bytes than input")
	}
}

// TestCompress_ZstdConcurrent exercises the shared zstd encoder/decoder from
// many goroutines simultaneously. Run with -race: EncodeAll/DecodeAll on the
// shared instances must be concurrency-safe with no per-call allocation of
// codec state.
func TestCompress_ZstdConcurrent(t *testing.T) {
	withCompressionAlgo(t, "zstd")

	const goroutines = 16
	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < iterations; i++ {
				// Compressible payload with per-goroutine variation.
				n := compressionThreshold + rng.Intn(4096)
				val := bytes.Repeat([]byte{byte('a' + seed%26)}, n)
				ct, ok := MaybeCompress(val, 1+int(seed)%3)
				if !ok {
					continue
				}
				out, err := Decompress(ct, uint32(len(val)))
				if err != nil {
					t.Errorf("goroutine %d iter %d: Decompress: %v", seed, i, err)
					return
				}
				if !bytes.Equal(out, val) {
					t.Errorf("goroutine %d iter %d: round-trip mismatch", seed, i)
					return
				}
			}
		}(int64(g))
	}
	wg.Wait()
}

// TestCompress_ThresholdBoundary checks the boundary value exactly at
// compressionThreshold — it may or may not compress depending on savings,
// but must not panic.
func TestCompress_ThresholdBoundary(t *testing.T) {
	val := make([]byte, compressionThreshold)
	copy(val, bytes.Repeat([]byte("x"), compressionThreshold))
	// Must not panic.
	ct, ok := MaybeCompress(val, 1)
	if ok {
		// If compressed, round-trip must work.
		out, err := Decompress(ct, uint32(len(val)))
		if err != nil {
			t.Fatalf("Decompress at threshold: %v", err)
		}
		if !bytes.Equal(out, val) {
			t.Error("threshold round-trip failed")
		}
	}
}
