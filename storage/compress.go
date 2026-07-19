package storage

// compress.go — value compression for VLog records.
//
// Algorithm selection: the engine writes one of {none, flate, zstd}. Each
// compressed record carries a 1-byte algorithm prefix so decompression can
// dispatch on the prefix byte — a store that mixes flate-era and zstd-era
// records stays fully readable in both directions with no on-disk format
// change.
//
// zstd comes from github.com/klauspost/compress/zstd. Both the encoders and
// the decoder are shared package-level instances used exclusively through
// EncodeAll/DecodeAll, which the library documents as concurrency-safe — no
// per-call encoder allocation on the Put/Get hot paths.
//
// The active WRITE algorithm is a package-level setting installed once at
// startup via SetCompressionAlgo (called from engine config parsing). Unknown
// Compression config values error there instead of silently falling back.
// The READ path (Decompress) always understands every registered algorithm
// regardless of the active write algorithm.
//
// FlagCompressed (already declared in types.go as 0x02) means: this record's
// VALUE BYTES (the bytes following the VLog header) are stored as
//   [1B algorithm-id][compressed payload]
// The IndexEntry.UncompressedSize stays the original byte length so callers
// know how big to allocate the output buffer.

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

// Algorithm IDs (max 8, room for lz4 / snappy if needed):
const (
	compAlgoNone  byte = 0x00 // sentinel — no compression on the write path
	compAlgoFlate byte = 0x01
	compAlgoZstd  byte = 0x02
)

// activeCompAlgo is the algorithm MaybeCompress uses on the write path.
// Stored as uint32 for lock-free hot-path reads. Defaults to flate so
// existing callers (and tests) that never call SetCompressionAlgo keep the
// historical behavior.
var activeCompAlgo atomic.Uint32

func init() {
	activeCompAlgo.Store(uint32(compAlgoFlate))
}

// SetCompressionAlgo installs the write-path compression algorithm. Called
// once during engine config parsing (NewStorageEngine); safe to call again
// (e.g. from tests). Valid values: "none"/"" (disable), "flate", "zstd".
// Any other value returns an error so a misconfigured Compression setting
// fails at startup instead of silently writing a different algorithm.
func SetCompressionAlgo(algo string) error {
	switch algo {
	case "", "none":
		activeCompAlgo.Store(uint32(compAlgoNone))
	case "flate":
		activeCompAlgo.Store(uint32(compAlgoFlate))
	case "zstd":
		// Fail fast at startup if the shared zstd codec cannot initialize.
		if _, err := sharedZstdDecoder(); err != nil {
			return fmt.Errorf("compression: zstd init: %w", err)
		}
		activeCompAlgo.Store(uint32(compAlgoZstd))
	default:
		return fmt.Errorf("compression: unknown algorithm %q (want none, flate, or zstd)", algo)
	}
	return nil
}

// --- shared zstd codec state ------------------------------------------------
//
// One encoder per speed level (klauspost exposes 4), one decoder total. All
// are lazily created and then reused for the process lifetime; EncodeAll /
// DecodeAll on a shared instance is concurrency-safe per the library docs.

var (
	zstdEncMu   sync.Mutex
	zstdEncs    [zstd.SpeedBestCompression + 1]*zstd.Encoder
	zstdDecOnce sync.Once
	zstdDec     *zstd.Decoder
	zstdDecErr  error
)

// zstdEncoderForLevel maps the engine's CompressionLevel (zstd CLI scale,
// 1–22; ≤0 means fastest) onto one of the shared encoders.
func zstdEncoderForLevel(level int) (*zstd.Encoder, error) {
	if level <= 0 {
		level = 1
	}
	el := zstd.EncoderLevelFromZstd(level)
	zstdEncMu.Lock()
	defer zstdEncMu.Unlock()
	if enc := zstdEncs[el]; enc != nil {
		return enc, nil
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(el))
	if err != nil {
		return nil, err
	}
	zstdEncs[el] = enc
	return enc, nil
}

func sharedZstdDecoder() (*zstd.Decoder, error) {
	zstdDecOnce.Do(func() {
		zstdDec, zstdDecErr = zstd.NewReader(nil)
	})
	return zstdDec, zstdDecErr
}

// compressionThreshold is the minimum value size (bytes) we attempt to compress.
// Smaller values rarely benefit (overhead exceeds savings) and the engine
// hot-path stays uncompressed for them.
const compressionThreshold = 256

// MaybeCompress returns (compressed, true) when compression saved at least one
// sector AND the value is above the threshold; otherwise returns (orig, false).
// Caller is expected to set FlagCompressed on the resulting IndexEntry only when
// the bool return is true.
//
// The algorithm is the package-level setting (SetCompressionAlgo); flate is
// the default when nothing was configured.
//
// level: 0 = fastest; flate clamps to 1–9, zstd maps 1–22 onto the library's
// four speed levels.
func MaybeCompress(value []byte, level int) ([]byte, bool) {
	if len(value) < compressionThreshold {
		return value, false
	}
	switch byte(activeCompAlgo.Load()) {
	case compAlgoZstd:
		return compressZstd(value, level)
	case compAlgoFlate:
		return compressFlate(value, level)
	default: // compAlgoNone
		return value, false
	}
}

func compressFlate(value []byte, level int) ([]byte, bool) {
	if level <= 0 {
		level = 1
	}
	if level > 9 {
		level = 9
	}
	var buf bytes.Buffer
	buf.Grow(len(value) + 8)
	buf.WriteByte(compAlgoFlate)
	w, err := flate.NewWriter(&buf, level)
	if err != nil {
		return value, false
	}
	if _, err := w.Write(value); err != nil {
		_ = w.Close()
		return value, false
	}
	if err := w.Close(); err != nil {
		return value, false
	}
	// Reject if compression didn't even pay back its 1-byte algorithm prefix
	// plus a 4-byte savings floor — small ratios do not justify the decompress
	// cost on the read path.
	if buf.Len()+4 >= len(value) {
		return value, false
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, true
}

func compressZstd(value []byte, level int) ([]byte, bool) {
	enc, err := zstdEncoderForLevel(level)
	if err != nil {
		return value, false
	}
	// EncodeAll appends to the supplied slice — seed it with the 1-byte
	// algorithm prefix so the whole blob is built in a single allocation.
	out := make([]byte, 1, len(value)/2+64)
	out[0] = compAlgoZstd
	out = enc.EncodeAll(value, out)
	// Same savings floor as flate: the prefix byte plus 4 bytes must be paid
	// back or the decompress cost on the read path isn't worth it.
	if len(out)+4 >= len(value) {
		return value, false
	}
	return out, true
}

// Decompress restores the original value bytes for a record written with
// FlagCompressed. expectedSize comes from IndexEntry.UncompressedSize.
//
// Dispatches on the 1-byte algorithm prefix, independent of the active write
// algorithm — a store containing both flate-era and zstd-era records is fully
// readable.
//
// Returns an error if the algorithm byte is unknown (rolling-back a binary that
// supports a newer algorithm onto data that only understands the older subset).
func Decompress(blob []byte, expectedSize uint32) ([]byte, error) {
	if len(blob) < 1 {
		return nil, errors.New("decompress: empty input")
	}
	algo := blob[0]
	payload := blob[1:]
	switch algo {
	case compAlgoFlate:
		r := flate.NewReader(bytes.NewReader(payload))
		defer r.Close()
		out := make([]byte, 0, expectedSize)
		buf := bytes.NewBuffer(out)
		if _, err := io.Copy(buf, r); err != nil {
			return nil, fmt.Errorf("decompress flate: %w", err)
		}
		if uint32(buf.Len()) != expectedSize {
			return nil, fmt.Errorf("decompress flate: expected %d bytes, got %d",
				expectedSize, buf.Len())
		}
		return buf.Bytes(), nil
	case compAlgoZstd:
		dec, err := sharedZstdDecoder()
		if err != nil {
			return nil, fmt.Errorf("decompress zstd: %w", err)
		}
		out, err := dec.DecodeAll(payload, make([]byte, 0, expectedSize))
		if err != nil {
			return nil, fmt.Errorf("decompress zstd: %w", err)
		}
		if uint32(len(out)) != expectedSize {
			return nil, fmt.Errorf("decompress zstd: expected %d bytes, got %d",
				expectedSize, len(out))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("decompress: unknown algorithm 0x%02x", algo)
	}
}
