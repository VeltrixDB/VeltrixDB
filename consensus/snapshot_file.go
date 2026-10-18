package consensus

// snapshot_file.go — the streaming on-disk snapshot format (raft_snapshot.dat).
//
// Layout (all integers little-endian):
//
//	header  snapHeaderSize (4096) bytes, written LAST (the capture learns its
//	        index only when it ends), at offset 0:
//	          0  magic "VXSN"
//	          4  format version (1)
//	          8  payload kind (1 = record stream, 2 = one legacy blob)
//	         12  reserved
//	         16  lastIncludedIndex
//	         24  lastIncludedTerm
//	         32  configIndex
//	         40  snapshot ID (random; identifies the file in a transfer)
//	         48  bodyLen  (bytes after the header, trailer included)
//	         56  bodyCRC  (crc32c of the body)
//	         60  record count
//	         68  config length, then the gob-encoded Configuration
//	       4092  crc32c of bytes [0, 4092)
//	body    records [len u32][crc32c(payload) u32][payload], len < 0xFFFFFFFF
//	trailer [0xFFFFFFFF u32][record count u64][crc32c of those 12 bytes u32]
//
// A reader checks the header CRC, every record CRC, the trailer, the record
// count and the body CRC, so a torn, truncated or corrupted file is always
// detected.  The file is written to raft_snapshot_*.tmp, fsynced, renamed to
// raft_snapshot.dat and the directory fsynced (snapshot.go); the rename is
// the commit point.  The record payloads are opaque to this package (the
// state machine's encoding); a legacy-blob file holds exactly one record, a
// pre-streaming Snapshot() blob.
//
// raft_snapshot.gob (the old single-gob format) is migrated on startup into a
// legacy-blob file (migrateLegacySnapshot).

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	snapFileName          = "raft_snapshot.dat"
	legacySnapFileName    = "raft_snapshot.gob"
	snapTmpPattern        = "raft_snapshot_*.tmp"
	snapRecvFileName      = "raft_snapshot_recv.tmp"
	snapMagic             = "VXSN"
	snapFormatVersion     = uint32(1)
	snapHeaderSize        = 4096
	snapHeaderCRCOff      = snapHeaderSize - 4
	snapHeaderFixed       = 72
	snapTrailerMarker     = uint32(0xFFFFFFFF)
	snapTrailerLen        = 16
	maxSnapRecord         = 1 << 30
	payloadStream         = uint32(1)
	payloadLegacyBlob     = uint32(2)
	snapWriterBufferBytes = 1 << 20
)

// snapshotMeta describes a snapshot file (its header).
type snapshotMeta struct {
	Kind        uint32
	Index       uint64
	Term        uint64
	ConfigIndex uint64
	Config      Configuration
	ID          uint64
	BodyLen     uint64
	BodyCRC     uint32
	Count       uint64
}

// FileSize is the total size of the snapshot file.
func (m snapshotMeta) FileSize() uint64 { return snapHeaderSize + m.BodyLen }

// sameFile reports whether two headers describe the same snapshot file.
func (m snapshotMeta) sameFile(o snapshotMeta) bool {
	return m.Index == o.Index && m.Term == o.Term && m.ID == o.ID && m.BodyLen == o.BodyLen
}

var errSnapshotCorrupt = errors.New("raft snapshot corrupt")

func corrupt(format string, a ...interface{}) error {
	return fmt.Errorf("%w: %s", errSnapshotCorrupt, fmt.Sprintf(format, a...))
}

func newSnapshotID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(b[:])
}

func encodeSnapHeader(m snapshotMeta) ([]byte, error) {
	cfg, err := encodeConfiguration(m.Config)
	if err != nil {
		return nil, err
	}
	if snapHeaderFixed+len(cfg) > snapHeaderCRCOff {
		return nil, fmt.Errorf("configuration too large for the snapshot header (%d bytes)", len(cfg))
	}
	h := make([]byte, snapHeaderSize)
	copy(h, snapMagic)
	le := binary.LittleEndian
	le.PutUint32(h[4:], snapFormatVersion)
	le.PutUint32(h[8:], m.Kind)
	le.PutUint64(h[16:], m.Index)
	le.PutUint64(h[24:], m.Term)
	le.PutUint64(h[32:], m.ConfigIndex)
	le.PutUint64(h[40:], m.ID)
	le.PutUint64(h[48:], m.BodyLen)
	le.PutUint32(h[56:], m.BodyCRC)
	le.PutUint64(h[60:], m.Count)
	le.PutUint32(h[68:], uint32(len(cfg)))
	copy(h[snapHeaderFixed:], cfg)
	le.PutUint32(h[snapHeaderCRCOff:], crc32.Checksum(h[:snapHeaderCRCOff], castagnoli))
	return h, nil
}

func decodeSnapHeader(h []byte) (snapshotMeta, error) {
	var m snapshotMeta
	if len(h) < snapHeaderSize {
		return m, corrupt("short header (%d bytes)", len(h))
	}
	le := binary.LittleEndian
	if string(h[:4]) != snapMagic {
		return m, corrupt("bad magic %q", h[:4])
	}
	if crc32.Checksum(h[:snapHeaderCRCOff], castagnoli) != le.Uint32(h[snapHeaderCRCOff:]) {
		return m, corrupt("header checksum mismatch")
	}
	if v := le.Uint32(h[4:]); v != snapFormatVersion {
		return m, fmt.Errorf("unsupported raft snapshot format version %d", v)
	}
	m.Kind = le.Uint32(h[8:])
	if m.Kind != payloadStream && m.Kind != payloadLegacyBlob {
		return m, corrupt("unknown payload kind %d", m.Kind)
	}
	m.Index = le.Uint64(h[16:])
	m.Term = le.Uint64(h[24:])
	m.ConfigIndex = le.Uint64(h[32:])
	m.ID = le.Uint64(h[40:])
	m.BodyLen = le.Uint64(h[48:])
	m.BodyCRC = le.Uint32(h[56:])
	m.Count = le.Uint64(h[60:])
	cl := int(le.Uint32(h[68:]))
	if snapHeaderFixed+cl > snapHeaderCRCOff {
		return m, corrupt("bad config length %d", cl)
	}
	cfg, err := decodeConfiguration(h[snapHeaderFixed : snapHeaderFixed+cl])
	if err != nil {
		return m, corrupt("config: %v", err)
	}
	m.Config = cfg
	return m, nil
}

// ── writer ──────────────────────────────────────────────────────────────────

// snapshotWriter streams records into a temp file.  It implements
// SnapshotSink.  abort (optional) is polled on every record so a capture
// stops promptly on shutdown or when an install needs the node.
type snapshotWriter struct {
	f     *os.File
	tmp   string
	bw    *bufio.Writer
	crc   uint32
	n     uint64
	count uint64
	abort func() error
	hdr   [8]byte
}

func createSnapshotWriter(dir string, abort func() error) (*snapshotWriter, error) {
	f, err := os.CreateTemp(dir, snapTmpPattern)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(snapHeaderSize, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return &snapshotWriter{f: f, tmp: f.Name(), bw: bufio.NewWriterSize(f, snapWriterBufferBytes), abort: abort}, nil
}

func (w *snapshotWriter) writeBody(b []byte) error {
	w.crc = crc32.Update(w.crc, castagnoli, b)
	w.n += uint64(len(b))
	_, err := w.bw.Write(b)
	return err
}

// Write appends one record (SnapshotSink).
func (w *snapshotWriter) Write(rec []byte) error {
	if w.abort != nil {
		if err := w.abort(); err != nil {
			return err
		}
	}
	if len(rec) >= maxSnapRecord {
		return fmt.Errorf("snapshot record too large (%d bytes)", len(rec))
	}
	binary.LittleEndian.PutUint32(w.hdr[0:], uint32(len(rec)))
	binary.LittleEndian.PutUint32(w.hdr[4:], crc32.Checksum(rec, castagnoli))
	if err := w.writeBody(w.hdr[:]); err != nil {
		return err
	}
	if err := w.writeBody(rec); err != nil {
		return err
	}
	w.count++
	return nil
}

// finish writes the trailer and header, fsyncs and closes the temp file, and
// returns the completed meta.  The file is not yet installed (see
// installSnapshotFile).
func (w *snapshotWriter) finish(m snapshotMeta) (snapshotMeta, error) {
	var tr [snapTrailerLen]byte
	binary.LittleEndian.PutUint32(tr[0:], snapTrailerMarker)
	binary.LittleEndian.PutUint64(tr[4:], w.count)
	binary.LittleEndian.PutUint32(tr[12:], crc32.Checksum(tr[:12], castagnoli))
	if err := w.writeBody(tr[:]); err != nil {
		return m, err
	}
	if err := w.bw.Flush(); err != nil {
		return m, err
	}
	m.BodyLen, m.BodyCRC, m.Count = w.n, w.crc, w.count
	h, err := encodeSnapHeader(m)
	if err != nil {
		return m, err
	}
	if _, err := w.f.WriteAt(h, 0); err != nil {
		return m, err
	}
	if err := w.f.Sync(); err != nil {
		return m, err
	}
	err = w.f.Close()
	w.f = nil
	return m, err
}

// discard removes the temp file.
func (w *snapshotWriter) discard() {
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
	os.Remove(w.tmp)
}

// installSnapshotFile renames a complete, fsynced snapshot file over
// raft_snapshot.dat and fsyncs the directory — the commit point of a capture
// or an install.
func installSnapshotFile(dir, tmp string) error {
	if err := os.Rename(tmp, filepath.Join(dir, snapFileName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// writeLegacyBlobSnapshot writes a one-record legacy-blob snapshot file to a
// temp file in dir and returns its path and meta (not installed).
func writeLegacyBlobSnapshot(dir string, m snapshotMeta, blob []byte) (string, snapshotMeta, error) {
	w, err := createSnapshotWriter(dir, nil)
	if err != nil {
		return "", m, err
	}
	if err := w.Write(blob); err != nil {
		w.discard()
		return "", m, err
	}
	m.Kind = payloadLegacyBlob
	if m.ID == 0 {
		m.ID = newSnapshotID()
	}
	m, err = w.finish(m)
	if err != nil {
		w.discard()
		return "", m, err
	}
	return w.tmp, m, nil
}

// ── reader ──────────────────────────────────────────────────────────────────

// snapshotReader yields the records of a snapshot file in order and verifies
// every checksum; Next returns io.EOF only after the trailer, the record count
// and the body CRC all matched.  It implements SnapshotSource.
type snapshotReader struct {
	r     *bufio.Reader
	meta  snapshotMeta
	crc   uint32
	read  uint64
	count uint64
	done  bool
	abort func() error
}

func newSnapshotReader(f *os.File, m snapshotMeta, abort func() error) (*snapshotReader, error) {
	sr := io.NewSectionReader(f, snapHeaderSize, int64(m.BodyLen))
	return &snapshotReader{r: bufio.NewReaderSize(sr, snapWriterBufferBytes), meta: m, abort: abort}, nil
}

func (r *snapshotReader) readFull(b []byte) error {
	if _, err := io.ReadFull(r.r, b); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return corrupt("truncated body after %d bytes", r.read)
		}
		return err
	}
	r.crc = crc32.Update(r.crc, castagnoli, b)
	r.read += uint64(len(b))
	return nil
}

// Next returns the next record payload (a fresh slice).
func (r *snapshotReader) Next() ([]byte, error) {
	if r.done {
		return nil, io.EOF
	}
	if r.abort != nil {
		if err := r.abort(); err != nil {
			return nil, err
		}
	}
	var hdr [8]byte
	if err := r.readFull(hdr[:4]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:4])
	if n == snapTrailerMarker {
		var tr [12]byte
		if err := r.readFull(tr[:]); err != nil {
			return nil, err
		}
		full := append(hdr[:4:4], tr[:8]...)
		if crc32.Checksum(full, castagnoli) != binary.LittleEndian.Uint32(tr[8:]) {
			return nil, corrupt("trailer checksum mismatch")
		}
		if c := binary.LittleEndian.Uint64(tr[:8]); c != r.count || c != r.meta.Count {
			return nil, corrupt("record count %d, trailer %d, header %d", r.count, c, r.meta.Count)
		}
		if r.read != r.meta.BodyLen {
			return nil, corrupt("trailer at %d of %d body bytes", r.read, r.meta.BodyLen)
		}
		if r.crc != r.meta.BodyCRC {
			return nil, corrupt("body checksum mismatch")
		}
		r.done = true
		return nil, io.EOF
	}
	if n >= maxSnapRecord {
		return nil, corrupt("record length %d", n)
	}
	if err := r.readFull(hdr[4:8]); err != nil {
		return nil, err
	}
	rec := make([]byte, n)
	if err := r.readFull(rec); err != nil {
		return nil, err
	}
	if crc32.Checksum(rec, castagnoli) != binary.LittleEndian.Uint32(hdr[4:8]) {
		return nil, corrupt("record %d checksum mismatch", r.count)
	}
	r.count++
	return rec, nil
}

// readSnapshotMeta opens path and decodes its header.  ok=false when the
// file does not exist.  The caller closes f.
func openSnapshotFile(path string) (f *os.File, m snapshotMeta, ok bool, err error) {
	f, err = os.Open(path)
	if os.IsNotExist(err) {
		return nil, m, false, nil
	}
	if err != nil {
		return nil, m, false, err
	}
	h := make([]byte, snapHeaderSize)
	if _, err := io.ReadFull(f, h); err != nil {
		f.Close()
		return nil, m, false, corrupt("%s: short header: %v", path, err)
	}
	m, err = decodeSnapHeader(h)
	if err != nil {
		f.Close()
		return nil, m, false, fmt.Errorf("%s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, m, false, err
	}
	if uint64(fi.Size()) != m.FileSize() {
		f.Close()
		return nil, m, false, corrupt("%s: size %d, header says %d", path, fi.Size(), m.FileSize())
	}
	return f, m, true, nil
}

// verifySnapshotFile reads the whole file and checks every checksum.
func verifySnapshotFile(path string) (snapshotMeta, error) {
	f, m, ok, err := openSnapshotFile(path)
	if err != nil {
		return m, err
	}
	if !ok {
		return m, fmt.Errorf("%s: %w", path, os.ErrNotExist)
	}
	defer f.Close()
	r, err := newSnapshotReader(f, m, nil)
	if err != nil {
		return m, err
	}
	for {
		if _, err := r.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				return m, nil
			}
			return m, fmt.Errorf("%s: %w", path, err)
		}
	}
}

// ── migration from raft_snapshot.gob ────────────────────────────────────────

// legacySnapshotFile is the gob layout of raft_snapshot.gob (pre-streaming).
type legacySnapshotFile struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Config            Configuration
	ConfigIndex       uint64
	Data              []byte
}

// cleanSnapshotTemps removes temp files left by an interrupted capture or
// receive (never the commit point, so always safe to drop).
func cleanSnapshotTemps(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		n := e.Name()
		if n == snapRecvFileName || (strings.HasPrefix(n, "raft_snapshot_") && strings.HasSuffix(n, ".tmp")) {
			os.Remove(filepath.Join(dir, n))
		}
	}
}

// migrateLegacySnapshot converts raft_snapshot.gob into raft_snapshot.dat
// (legacy-blob payload).  raft_snapshot.dat is installed first (the commit
// point); the .gob is then renamed to .gob.migrated.  A crash in between
// leaves both, and the next start only retires the .gob.
func migrateLegacySnapshot(dir string) error {
	gobPath := filepath.Join(dir, legacySnapFileName)
	if _, err := os.Stat(gobPath); err != nil {
		return nil // nothing to migrate
	}
	if _, err := os.Stat(filepath.Join(dir, snapFileName)); err == nil {
		return retireLegacySnapshot(dir)
	}
	b, err := os.ReadFile(gobPath)
	if err != nil {
		return err
	}
	var old legacySnapshotFile
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&old); err != nil {
		return fmt.Errorf("decode %s: %w", gobPath, err)
	}
	tmp, _, err := writeLegacyBlobSnapshot(dir, snapshotMeta{
		Index: old.LastIncludedIndex, Term: old.LastIncludedTerm,
		Config: old.Config, ConfigIndex: old.ConfigIndex,
	}, old.Data)
	if err != nil {
		return fmt.Errorf("migrate %s: %w", gobPath, err)
	}
	if err := installSnapshotFile(dir, tmp); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("migrate %s: %w", gobPath, err)
	}
	log.Printf("[raft] migrated %s → %s (idx=%d term=%d, %d bytes)",
		gobPath, snapFileName, old.LastIncludedIndex, old.LastIncludedTerm, len(old.Data))
	return retireLegacySnapshot(dir)
}

func retireLegacySnapshot(dir string) error {
	gobPath := filepath.Join(dir, legacySnapFileName)
	if err := os.Rename(gobPath, gobPath+legacyMigratedSuffix); err != nil {
		return fmt.Errorf("retire %s: %w", gobPath, err)
	}
	return syncDir(dir)
}
