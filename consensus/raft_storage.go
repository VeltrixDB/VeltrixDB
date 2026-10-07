package consensus

// raft_storage.go — incremental Raft persistence.
//
// Raft must keep currentTerm, votedFor and the log across crashes.  They live
// in two files under the node's dataDir:
//
//	raft_log.dat   append-only log: an 8-byte header ("VXRL", version 1) then
//	               one record per entry —
//	                 [len uint32 LE][crc32c(payload) uint32 LE][payload]
//	                 payload = [term u64][index u64][type u8][command…]
//	raft_meta.dat  currentTerm + votedFor, replaced atomically
//	               (temp file → fsync → rename → dir fsync), and only when one
//	               of them changes.
//
// Cost per commit is O(new entries): a group-commit flush writes the new
// records with one write(2) and one fsync — it never rewrites the retained log
// (the old raft_state.gob rewrote and fsynced the whole log on every flush).
//
// Write path: the caller encodes new records into an in-memory queue while it
// holds rn.mu (enqueue), so queue order == log order; the node's single syncer
// goroutine (pipeline.go) drains the queue with ONE write(2) and then fsyncs,
// both outside rn.mu.  File order is therefore still log order, and no
// rn.mu holder ever waits for a write(2) — which matters on APFS, where a
// write(2) blocks while an F_FULLFSYNC of the same file is in progress.
//
// Truncation needs no separate record.  A Raft log is a function of index, so
// replay treats a record whose index is <= the last replayed index as "drop
// every entry from this index on, then append": the conflicting suffix a
// follower overwrites is superseded the moment the first new record is on
// disk.  A crash before that record is complete leaves the old suffix — the
// state from before the AppendEntries, which the follower never acknowledged.
//
// Torn tail: a record with a short read, a bad length or a CRC mismatch ends
// replay; the file is truncated to the last good record (and fsynced) before
// anything is appended, so later records never land behind garbage.
//
// Compaction (snapshot.go) and InstallSnapshot rewrite the file to the
// retained suffix atomically (temp → fsync → rename → dir fsync).  A crash
// before the rename leaves the longer file, whose snapshot-covered prefix is
// dropped on load.
//
// Migration: a dataDir that has the old whole-state raft_state.gob and no
// raft_meta.dat is converted on startup (log file first, meta file last — the
// meta file is the commit point), then raft_state.gob is renamed to
// raft_state.gob.migrated.  An older build cannot read the new files; see
// docs/replication.md#persistence for the rollback procedure.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	raftLogFileName      = "raft_log.dat"
	raftMetaFileName     = "raft_meta.dat"
	legacyStateFileName  = "raft_state.gob"
	legacyMigratedSuffix = ".migrated"

	raftLogMagic   = "VXRL"
	raftLogVersion = uint32(1)
	raftLogHdrLen  = 8 // magic + version

	raftMetaMagic   = "VXRM"
	raftMetaVersion = uint32(1)

	recHdrLen     = 8  // len + crc
	recFixedLen   = 17 // term + index + type
	maxRecPayload = 1 << 30
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errLogStoreClosed is returned by a logStore used after close.
var errLogStoreClosed = errors.New("raft log store closed")

// raftMeta is the persisted term/vote pair.
type raftMeta struct {
	CurrentTerm uint64
	VotedFor    string
}

// logStore owns raft_log.dat and raft_meta.dat for one RaftNode.
//
// Ordering: every change to the log (enqueue, rewrite) is issued while the
// caller holds rn.mu, so the record order on disk is the order of the
// in-memory log.  flush (write the queue + fsync) runs without rn.mu, from the
// syncer goroutine only; it covers every record enqueued before it began.
//
// Lock order: rn.mu → s.mu → s.qmu.  s.mu is held across the write(2) of a
// flush (never across its fsync); enqueue takes only s.qmu.
type logStore struct {
	dir string

	mu     sync.Mutex // guards f, size, gen, failed, closed, dirty, meta
	f      *os.File   // raft_log.dat, opened O_APPEND
	size   int64      // bytes of complete records (end of the good region)
	gen    uint64     // bumped when rewrite swaps f
	failed error      // sticky: a failed write/fsync leaves the file state unknown
	closed bool
	// dirty: records were written since the last fsync started.
	dirty   bool
	meta    raftMeta // last persisted meta
	hasMeta bool

	// qmu guards the write queue.  qfailed/qclosed mirror failed/closed so
	// enqueue can refuse without taking s.mu.
	qmu     sync.Mutex
	pend    []byte // encoded records not yet written, in log order
	spare   []byte // recycled buffer for pend
	qfailed error
	qclosed bool

	// syncFile fsyncs the log file (os.File.Sync; F_FULLFSYNC on darwin).
	// Tests replace it to inject slow or failing fsyncs (setSyncHook).
	syncFile func(*os.File) error

	// Metrics / test hooks.
	bytesWritten atomic.Uint64 // log + meta + rewrite bytes handed to write(2)
	syncCalls    atomic.Uint64 // log fsyncs (flush + rewrite)
	metaWrites   atomic.Uint64
	rewrites     atomic.Uint64
}

func (s *logStore) logPath() string    { return filepath.Join(s.dir, raftLogFileName) }
func (s *logStore) metaPath() string   { return filepath.Join(s.dir, raftMetaFileName) }
func (s *logStore) legacyPath() string { return filepath.Join(s.dir, legacyStateFileName) }

// loadedState is what openLogStore recovered from disk.
type loadedState struct {
	meta      raftMeta
	entries   []LogEntry
	tornBytes int64  // bytes dropped from the log tail
	gapFrom   uint64 // highest index discarded by a replay gap (0 = none)
	migrated  bool
}

// openLogStore loads (or migrates, or initialises) the persisted Raft state in
// dir and leaves raft_log.dat open for appends.
func openLogStore(dir string) (*logStore, loadedState, error) {
	s := &logStore{dir: dir, syncFile: (*os.File).Sync}
	var st loadedState

	meta, hasMeta, err := s.readMeta()
	if err != nil {
		return nil, st, err
	}
	_, legacyErr := os.Stat(s.legacyPath())
	hasLegacy := legacyErr == nil

	switch {
	case !hasMeta && hasLegacy:
		// Old format (or a migration that crashed before its commit point):
		// convert.  raft_meta.dat is written last, so until it exists the
		// legacy file stays authoritative and this branch simply runs again.
		ps, err := readLegacyState(s.legacyPath())
		if err != nil {
			return nil, st, fmt.Errorf("read legacy %s: %w", legacyStateFileName, err)
		}
		if err := s.rewriteFile(ps.Log); err != nil {
			return nil, st, fmt.Errorf("migrate log: %w", err)
		}
		m := raftMeta{CurrentTerm: ps.CurrentTerm, VotedFor: ps.VotedFor}
		if err := s.writeMeta(m); err != nil {
			return nil, st, fmt.Errorf("migrate meta: %w", err)
		}
		if err := s.retireLegacy(); err != nil {
			return nil, st, err
		}
		log.Printf("[raft] migrated %s → %s + %s (term=%d log_len=%d)",
			s.legacyPath(), raftLogFileName, raftMetaFileName, ps.CurrentTerm, len(ps.Log))
		meta, hasMeta = m, true
		st.migrated = true
	case hasMeta && hasLegacy:
		// Migration committed (meta written) but the rename did not happen.
		if err := s.retireLegacy(); err != nil {
			return nil, st, err
		}
	case !hasMeta:
		// Fresh node: create the meta file now so a log file without a meta
		// file can only mean an interrupted migration.
		if err := s.writeMeta(raftMeta{}); err != nil {
			return nil, st, fmt.Errorf("init meta: %w", err)
		}
		meta, hasMeta = raftMeta{}, true
	}
	s.meta, s.hasMeta = meta, hasMeta
	st.meta = meta

	entries, good, gapFrom, err := s.replay()
	if err != nil {
		return nil, st, err
	}
	st.entries, st.gapFrom = entries, gapFrom

	f, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, st, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, st, err
	}
	if fi.Size() > good {
		// Torn tail (crash mid-append): cut it off durably before appending.
		st.tornBytes = fi.Size() - good
		if err := f.Truncate(good); err != nil {
			f.Close()
			return nil, st, fmt.Errorf("truncate torn raft log tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, st, fmt.Errorf("sync truncated raft log: %w", err)
		}
		log.Printf("[raft] %s: dropped %d-byte torn tail after %d records",
			s.logPath(), st.tornBytes, len(entries))
	}
	s.f, s.size = f, good
	return s, st, nil
}

// replay reads raft_log.dat (creating an empty one if missing) and returns the
// recovered log, the offset just past the last good record, and the highest
// index discarded by an index gap (see loadState).
func (s *logStore) replay() (entries []LogEntry, good int64, gapFrom uint64, err error) {
	f, err := os.Open(s.logPath())
	if os.IsNotExist(err) {
		if err := s.rewriteFile(nil); err != nil {
			return nil, 0, 0, err
		}
		return nil, raftLogHdrLen, 0, nil
	}
	if err != nil {
		return nil, 0, 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, 0, err
	}
	fileSize := fi.Size()

	r := bufio.NewReaderSize(f, 1<<20)
	var hdr [raftLogHdrLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		// The header is written by rewriteFile (temp + rename) before the file
		// becomes visible, so a short header is not a torn append.
		return nil, 0, 0, fmt.Errorf("%s: short header: %w", s.logPath(), err)
	}
	if string(hdr[:4]) != raftLogMagic {
		return nil, 0, 0, fmt.Errorf("%s: bad magic %q", s.logPath(), hdr[:4])
	}
	if v := binary.LittleEndian.Uint32(hdr[4:]); v != raftLogVersion {
		return nil, 0, 0, fmt.Errorf("%s: unsupported version %d", s.logPath(), v)
	}
	good = raftLogHdrLen

	var rh [recHdrLen]byte
	for {
		if _, err := io.ReadFull(r, rh[:]); err != nil {
			break // EOF or torn record header
		}
		n := binary.LittleEndian.Uint32(rh[0:4])
		sum := binary.LittleEndian.Uint32(rh[4:8])
		if n < recFixedLen || n > maxRecPayload || good+recHdrLen+int64(n) > fileSize {
			break // garbage length (torn header) or record extends past EOF
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			break
		}
		if crc32.Checksum(payload, castagnoli) != sum {
			break
		}
		e := LogEntry{
			Term:  binary.LittleEndian.Uint64(payload[0:8]),
			Index: binary.LittleEndian.Uint64(payload[8:16]),
			Type:  EntryType(payload[16]),
		}
		if len(payload) > recFixedLen {
			e.Command = payload[recFixedLen:]
		}
		if len(entries) > 0 {
			first := entries[0].Index
			last := entries[len(entries)-1].Index
			switch {
			case e.Index <= first:
				// Supersedes the whole replayed log.
				entries = entries[:0]
			case e.Index <= last:
				// Conflict overwrite: drop [e.Index, last], then append.
				entries = entries[:e.Index-first]
			case e.Index > last+1:
				// Index gap: the log before it was discarded in memory (snapshot
				// install) but the rewrite did not land.  Only valid if the
				// snapshot covers the gap — loadState checks gapFrom.
				gapFrom = last
				entries = entries[:0]
			}
		}
		entries = append(entries, e)
		good += int64(recHdrLen) + int64(n)
	}
	return entries, good, gapFrom, nil
}

// encodeRecords appends the on-disk records for entries to dst.
func encodeRecords(dst []byte, entries []LogEntry) []byte {
	for i := range entries {
		e := &entries[i]
		n := recFixedLen + len(e.Command)
		start := len(dst)
		dst = append(dst, make([]byte, recHdrLen+recFixedLen)...)
		dst = append(dst, e.Command...)
		p := dst[start+recHdrLen:]
		binary.LittleEndian.PutUint64(p[0:8], e.Term)
		binary.LittleEndian.PutUint64(p[8:16], e.Index)
		p[16] = byte(e.Type)
		binary.LittleEndian.PutUint32(dst[start:start+4], uint32(n))
		binary.LittleEndian.PutUint32(dst[start+4:start+8], crc32.Checksum(p[:n], castagnoli))
	}
	return dst
}

// enqueue appends the records for entries to the write queue (no I/O).
// Caller holds rn.mu, so queue order == log order.  It fails once the store
// has failed (sticky) or is closed: the caller must then not count the
// entries as persisted.
func (s *logStore) enqueue(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.qmu.Lock()
	defer s.qmu.Unlock()
	if s.qclosed {
		return errLogStoreClosed
	}
	if s.qfailed != nil {
		return s.qfailed
	}
	s.pend = encodeRecords(s.pend, entries)
	return nil
}

// failedErr reports the sticky failure, if any.
func (s *logStore) failedErr() error {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	return s.qfailed
}

// setFailedLocked records a sticky failure and drops the queue.  s.mu held.
func (s *logStore) setFailedLocked(err error) {
	s.failed = err
	s.qmu.Lock()
	s.qfailed = err
	s.pend = nil
	s.qmu.Unlock()
}

// setSyncHook replaces the fsync function (tests: slow / failing fsync).
func (s *logStore) setSyncHook(fn func(*os.File) error) {
	s.mu.Lock()
	s.syncFile = fn
	s.mu.Unlock()
}

// flush writes every queued record with ONE write(2) and then fsyncs the
// file, so on success every record enqueued before flush began is durable.
// Called without rn.mu, from the syncer goroutine only (single writer).  A
// failed write or fsync is sticky: nothing written so far can be trusted to be
// durable, and the store refuses further writes until the node restarts.  If a
// concurrent rewrite swapped (and closed) the file while the fsync ran, the
// rewrite already fsynced a superset of what this call had to cover, so that
// counts as success.
func (s *logStore) flush() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errLogStoreClosed
	}
	if s.failed != nil {
		err := s.failed
		s.mu.Unlock()
		return err
	}
	s.qmu.Lock()
	buf := s.pend
	s.pend, s.spare = s.spare[:0], nil
	s.qmu.Unlock()
	if len(buf) > 0 {
		n, err := s.f.Write(buf)
		s.bytesWritten.Add(uint64(n))
		if err != nil {
			werr := fmt.Errorf("raft log write: %w", err)
			if terr := s.f.Truncate(s.size); terr != nil {
				werr = fmt.Errorf("raft log: write failed (%v) and rollback failed: %w", err, terr)
			}
			s.setFailedLocked(werr)
			s.mu.Unlock()
			return werr
		}
		s.size += int64(n)
		s.dirty = true
	}
	if cap(buf) <= 4<<20 { // do not pin a huge batch buffer
		s.qmu.Lock()
		if s.spare == nil {
			s.spare = buf[:0]
		}
		s.qmu.Unlock()
	}
	dirty := s.dirty
	s.dirty = false
	f, gen, syncFile := s.f, s.gen, s.syncFile
	s.mu.Unlock()
	if !dirty {
		return nil
	}

	s.syncCalls.Add(1)
	err := syncFile(f)
	if err == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen != gen {
		return nil // rewritten (and fsynced) while we were syncing
	}
	// After a failed fsync the kernel may have dropped dirty pages; nothing
	// written so far can be trusted to be durable.  Refuse further writes.
	s.setFailedLocked(fmt.Errorf("raft log fsync failed: %w", err))
	return s.failed
}

// rewrite atomically replaces raft_log.dat with exactly entries (compaction,
// InstallSnapshot) and reopens it for appends.  Caller holds rn.mu; entries is
// the whole in-memory log, so the write queue (a suffix of it) is dropped.
// On a failure before the swap the old file and the queue stay as they were.
func (s *logStore) rewrite(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errLogStoreClosed
	}
	if err := s.rewriteFile(entries); err != nil {
		return err
	}
	f, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		s.setFailedLocked(fmt.Errorf("reopen rewritten raft log: %w", err))
		return s.failed
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		s.setFailedLocked(fmt.Errorf("stat rewritten raft log: %w", err))
		return s.failed
	}
	old := s.f
	s.f, s.size = f, fi.Size()
	s.gen++
	s.dirty = false
	// The new file is complete and fsynced: a sticky failure on the old file
	// no longer matters, and the queued records are part of it.
	s.failed = nil
	s.qmu.Lock()
	s.qfailed = nil
	s.pend = s.pend[:0]
	s.qmu.Unlock()
	if old != nil {
		old.Close()
	}
	return nil
}

// rewriteFile writes header + entries to a temp file, fsyncs it, renames it
// over raft_log.dat and fsyncs the directory.
func (s *logStore) rewriteFile(entries []LogEntry) error {
	s.rewrites.Add(1)
	buf := make([]byte, raftLogHdrLen, raftLogHdrLen+64)
	copy(buf, raftLogMagic)
	binary.LittleEndian.PutUint32(buf[4:], raftLogVersion)
	buf = encodeRecords(buf, entries)
	if err := s.atomicWrite(s.logPath(), "raft_log_*.tmp", buf); err != nil {
		return fmt.Errorf("rewrite raft log: %w", err)
	}
	s.syncCalls.Add(1)
	return nil
}

// saveMeta persists (term, vote) atomically if it differs from what is on
// disk.  Called with rn.mu held, BEFORE the node acts on the new term/vote
// (replying to a RequestVote / AppendEntries, or soliciting votes).
func (s *logStore) saveMeta(m raftMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errLogStoreClosed
	}
	if s.hasMeta && s.meta == m {
		return nil
	}
	if err := s.writeMeta(m); err != nil {
		return err
	}
	s.meta, s.hasMeta = m, true
	return nil
}

func (s *logStore) writeMeta(m raftMeta) error {
	vote := []byte(m.VotedFor)
	if len(vote) > 0xFFFF {
		return fmt.Errorf("votedFor too long (%d bytes)", len(vote))
	}
	buf := make([]byte, 0, 4+4+8+2+len(vote)+4)
	buf = append(buf, raftMetaMagic...)
	buf = binary.LittleEndian.AppendUint32(buf, raftMetaVersion)
	buf = binary.LittleEndian.AppendUint64(buf, m.CurrentTerm)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(vote)))
	buf = append(buf, vote...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))
	if err := s.atomicWrite(s.metaPath(), "raft_meta_*.tmp", buf); err != nil {
		return fmt.Errorf("write raft meta: %w", err)
	}
	s.metaWrites.Add(1)
	return nil
}

// readMeta loads raft_meta.dat.  ok=false means it does not exist.  The file
// is only ever replaced by rename, so a damaged one is an error, not a torn
// write.
func (s *logStore) readMeta() (m raftMeta, ok bool, err error) {
	b, err := os.ReadFile(s.metaPath())
	if os.IsNotExist(err) {
		return raftMeta{}, false, nil
	}
	if err != nil {
		return raftMeta{}, false, err
	}
	bad := func(why string) (raftMeta, bool, error) {
		return raftMeta{}, false, fmt.Errorf("%s: %s", s.metaPath(), why)
	}
	if len(b) < 4+4+8+2+4 || string(b[:4]) != raftMetaMagic {
		return bad("bad header")
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return bad("checksum mismatch")
	}
	if v := binary.LittleEndian.Uint32(b[4:8]); v != raftMetaVersion {
		return bad(fmt.Sprintf("unsupported version %d", v))
	}
	m.CurrentTerm = binary.LittleEndian.Uint64(b[8:16])
	vl := int(binary.LittleEndian.Uint16(b[16:18]))
	if 18+vl != len(body) {
		return bad("bad votedFor length")
	}
	m.VotedFor = string(b[18 : 18+vl])
	return m, true, nil
}

// atomicWrite replaces path with data: temp file → write → fsync → rename →
// directory fsync.
func (s *logStore) atomicWrite(path, pattern string, data []byte) error {
	f, err := os.CreateTemp(s.dir, pattern)
	if err != nil {
		return err
	}
	tmp := f.Name()
	n, err := f.Write(data)
	s.bytesWritten.Add(uint64(n))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(s.dir)
}

// retireLegacy renames raft_state.gob out of the way after migration.
func (s *logStore) retireLegacy() error {
	if err := os.Rename(s.legacyPath(), s.legacyPath()+legacyMigratedSuffix); err != nil {
		return fmt.Errorf("retire legacy %s: %w", legacyStateFileName, err)
	}
	return syncDir(s.dir)
}

// close releases the log file.  Further calls fail with errLogStoreClosed.
func (s *logStore) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.qmu.Lock()
	s.qclosed = true
	s.pend = nil
	s.qmu.Unlock()
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

// readLegacyState decodes the pre-incremental whole-state gob file.
func readLegacyState(path string) (persistentState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return persistentState{}, err
	}
	var ps persistentState
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&ps); err != nil {
		return persistentState{}, err
	}
	return ps, nil
}

// syncDir fsyncs a directory so a rename in it is durable.  Filesystems that
// cannot fsync a directory (EINVAL / ENOTSUP) are tolerated: there the rename
// is as durable as the platform allows.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !isDirSyncUnsupported(err) {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

func isDirSyncUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EBADF)
}
