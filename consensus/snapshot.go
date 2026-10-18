package consensus

// snapshot.go — state-machine snapshots, log compaction and InstallSnapshot
// (Raft §7).  CLAUDE.md invariant 64 is the summary; this comment is the
// design.
//
// # Capture (takeSnapshot)
//
// When the retained log reaches Options.SnapshotThreshold entries, a
// background goroutine (not the applier) captures the state machine into a
// streaming file (snapshot_file.go), installs it as raft_snapshot.dat
// (temp → fsync → rename → dir fsync) and only then truncates the covered
// log prefix and rewrites raft_log.dat.  There is no size cap: records are
// streamed to disk, never held in memory as one blob.
//
// Consistency.  A StreamingStateMachine captures while Apply keeps running
// and calls the fence exactly once, at the end.  The fence takes smMu (so no
// Apply is in progress or can start), reads lastApplied = E and the term and
// configuration at E under rn.mu, runs the state machine's final step and
// releases smMu.  The snapshot is labelled E.  The state machine guarantees
// (cmd/server/raft_fsm.go has the argument for the storage engine) that the
// records it emitted, read in order with the last record of each key
// winning, equal its state with every entry ≤ E applied and none after —
// which is exactly what the fence observes, because Apply is quiesced inside
// it and lastApplied only moves under smMu.  Apply is therefore stalled only
// for the fence's final step (O(keys written during the last catch-up
// round)), not for the keyspace scan.  A legacy SnapshotStateMachine
// (Snapshot() []byte) is captured entirely inside the fence, as before.
//
// # Transfer (sendSnapshot / HandleInstallSnapshotChunk)
//
// The leader streams the file in chunks of Options.SnapshotChunkSize (2 MB)
// from an open descriptor: the descriptor pins that version of the file, so
// a newer capture renaming over raft_snapshot.dat mid-transfer does not
// change the bytes being sent; the follower identifies the transfer by
// (index, term, snapshot ID, size).  One chunk is in flight at a time and at
// most one transfer per peer (snapInFlight); the transfer runs on its own
// goroutine, so the peer's replicator keeps sending heartbeats.  The follower
// appends a chunk only at its current offset and always answers with the
// offset it wants next, so lost, duplicated and reordered chunks are
// harmless; a chunk of a different snapshot at offset 0 restarts the
// transfer, at any other offset it is answered "start at 0".  The last chunk
// carries Done; the follower fsyncs the temp file, verifies every checksum
// and installs it in the background, answering Installing until it is done
// (the leader polls) and Installed after.  A leader change aborts the old
// leader's transfer; the new leader starts its own (the follower resumes it
// only if it is the very same file).
//
// # Install (installSnapshot)
//
// Lock order snapMu → smMu → rn.mu.  Restoring never holds rn.mu, so the
// follower keeps answering heartbeats and votes; while it restores,
// rn.installing keeps its election timer quiet.  Steps: (1) the verified
// file is renamed to raft_snapshot.dat + dir fsync — the commit point;
// (2) under rn.mu the log keeps the suffix that agrees with the snapshot
// (Raft §7) or is discarded, lastIncludedIndex/Term and the configuration
// are set and raft_log.dat is rewritten; (3) with smMu held (no Apply) the
// state machine is restored from the file; (4) lastApplied = index.
//
// Crash safety: before (1) the node restarts with its old snapshot, log and
// state machine, untouched.  After (1) a restart restores the state machine
// from raft_snapshot.dat (loadSnapshot) before replaying the log tail — an
// AppliedIndexStateMachine marks the restore in progress (BeginRestore,
// durable) before (3), so its recorded applied index cannot skip that
// restore until it completed (applied.go) — so an interrupted restore is
// simply redone — RestoreSnapshot must
// make the state machine exactly equal to the snapshot whatever it held
// before.  The storage-backed state machine does that in place, through the
// engine that holds the data-dir lock (CLAUDE.md invariant 59).
//
// # Compatibility (v1.16 and earlier)
//
// Followers advertise capSnapshotChunks in every AppendEntries reply; a v1.16
// follower does not, and never receives a chunk (msgInstallSnapshotChunk is a
// message type it would refuse anyway).  To such a peer the leader sends the
// old single-shot InstallSnapshot when the state machine can encode the
// snapshot in the old format within the old 256 MB limit
// (LegacySnapshotEncoder), and otherwise refuses with a log line naming the
// peer.  A v1.16 leader's single-shot InstallSnapshot is accepted and
// installed through the same path (legacy-blob payload).  Rolling upgrade:
// followers first, the leader last.
//
// Crash recovery order (NewRaftNodeWithOptions): restore the snapshot first
// (state machine + lastIncludedIndex/Term + base config), then load the log
// and replay only the retained tail once commitIndex advances.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// SnapshotStateMachine is a StateMachine that can capture and restore its
// full state as one in-memory blob (the pre-streaming interface; still
// supported).  Snapshot is called with Apply quiesced.
type SnapshotStateMachine interface {
	StateMachine
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// SnapshotSink receives the records of a snapshot being captured.
type SnapshotSink interface {
	Write(record []byte) error
}

// SnapshotSource yields the records of a stored snapshot in order, then
// io.EOF (only after every checksum of the file matched).
type SnapshotSource interface {
	Next() ([]byte, error)
}

// SnapshotFence runs fn with Apply quiesced and returns the index of the last
// applied entry, which becomes the snapshot's lastIncludedIndex.  A capture
// calls it exactly once, after its last record that depends on concurrent
// applies has been decided (fn may compute records; writing them to the sink
// after the fence returns is fine).
type SnapshotFence func(fn func()) (uint64, error)

// StreamingStateMachine captures and restores snapshots as record streams
// without materialising them in memory (see the file comment).
type StreamingStateMachine interface {
	StateMachine
	// CaptureSnapshot writes the state to sink while Apply keeps running and
	// calls fence exactly once; the records, read in order with the last
	// record for a key winning, must equal the state at the fence's index.
	CaptureSnapshot(sink SnapshotSink, fence SnapshotFence) error
	// RestoreSnapshot makes the state exactly the snapshot's, whatever it was
	// before (an interrupted restore is redone on restart).  legacy: the
	// source holds one record, a blob from a pre-streaming Snapshot().
	RestoreSnapshot(src SnapshotSource, legacy bool) error
}

// LegacySnapshotEncoder converts a stream snapshot into the single-blob
// format a pre-streaming (v1.16) state machine's Restore reads, for a
// single-shot InstallSnapshot to an old follower.  It fails once the blob
// would exceed limit bytes.
type LegacySnapshotEncoder interface {
	EncodeLegacySnapshot(src SnapshotSource, w io.Writer, limit int) error
}

// legacyMaxSnapshotBytes is the largest single-shot InstallSnapshot a v1.16
// follower accepts (its maxSnapshotBytes).  A variable only for tests.
var legacyMaxSnapshotBytes = 256 << 20

// stageHook (tests only) is called at each crash-relevant stage of a capture
// or an install (snapshot_stream_test.go, SetSnapshotStageHookForTesting).
type stageHook func(rn *RaftNode, stage string)

var snapStageHook atomic.Pointer[stageHook]

func setStageHook(h stageHook) {
	if h == nil {
		snapStageHook.Store(nil)
		return
	}
	snapStageHook.Store(&h)
}

func (rn *RaftNode) stage(name string) {
	if h := snapStageHook.Load(); h != nil {
		(*h)(rn, name)
	}
}

const (
	// DefaultSnapshotChunkSize is the InstallSnapshot chunk size.
	DefaultSnapshotChunkSize = 2 << 20

	// capSnapshotChunks: this follower accepts msgInstallSnapshotChunk.
	capSnapshotChunks uint32 = 1 << 0

	snapshotPollInterval = 100 * time.Millisecond
	snapshotRetryMax     = time.Second
	// captureRetryMax caps the back-off after a failed capture.
	captureRetryMax = time.Minute
)

// InstallSnapshotArgs is the single-shot InstallSnapshot (v1.16 wire format):
// the whole snapshot in Data.  Sent only to followers that do not advertise
// capSnapshotChunks, and accepted from older leaders.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Config            Configuration // configuration as of LastIncludedIndex
	ConfigIndex       uint64
	Data              []byte // complete state-machine snapshot (old format)
}

// InstallSnapshotReply carries the follower's current term.
type InstallSnapshotReply struct {
	Term uint64
}

// InstallSnapshotChunkArgs carries one chunk of a snapshot file (Raft §7:
// offset, data, done).  The snapshot is identified by LastIncludedIndex,
// LastIncludedTerm, SnapshotID and Size.
type InstallSnapshotChunkArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	SnapshotID        uint64
	Size              uint64 // total file size
	Offset            uint64
	Data              []byte
	Done              bool // this chunk ends the file (or: status poll)
}

// InstallSnapshotChunkReply: NextOffset is the offset the follower wants
// next.  Installing: the file is complete and being installed (poll again);
// Installed: the follower's state includes LastIncludedIndex; Busy: another
// install is running (retry later); Failed: the install failed (restart).
type InstallSnapshotChunkReply struct {
	Term       uint64
	NextOffset uint64
	Installing bool
	Installed  bool
	Busy       bool
	Failed     bool
}

// ChunkTransport is the optional Transport extension for chunked snapshots.
type ChunkTransport interface {
	SendInstallSnapshotChunk(peer string, args InstallSnapshotChunkArgs) (InstallSnapshotChunkReply, error)
}

// ChunkedSnapshotHandler is implemented by *RaftNode (RPCServer dispatch).
type ChunkedSnapshotHandler interface {
	HandleInstallSnapshotChunk(args InstallSnapshotChunkArgs) InstallSnapshotChunkReply
}

var (
	errCaptureAborted   = errors.New("snapshot capture aborted")
	errSnapshotNotDue   = errors.New("nothing to snapshot")
	errRestoreAborted   = errors.New("snapshot restore aborted: node stopping")
	errNotSnapshottable = errors.New("state machine is not snapshot-capable")
)

func (rn *RaftNode) snapshotPath() string {
	return filepath.Join(rn.dataDir, snapFileName)
}

func (rn *RaftNode) snapshotCapable() bool {
	switch rn.sm.(type) {
	case StreamingStateMachine, SnapshotStateMachine:
		return true
	}
	return false
}

// localCaps is the capability set this node advertises in AppendEntries
// replies.
func (rn *RaftNode) localCaps() uint32 {
	if rn.legacyPeer {
		return 0
	}
	return capSnapshotChunks
}

func (rn *RaftNode) stoppedErr() error {
	if rn.stopped.Load() {
		return errRestoreAborted
	}
	return nil
}

// ── startup ─────────────────────────────────────────────────────────────────

// loadSnapshot loads the persisted snapshot during startup, BEFORE the log
// is loaded: lastIncludedIndex/Term, the base configuration and — unless the
// state machine recorded an applied index ≥ the snapshot's and no restore in
// progress (applied.go) — the state machine contents.  A skipped restore is
// left in rn.startupSnap and decided by finishStartupRestore once the log is
// loaded (the recorded term is checked against it).  No-op when no snapshot
// exists.  The whole file is verified before the state machine is touched.
// Called from NewRaftNodeWithOptions only (no goroutines running yet).
func (rn *RaftNode) loadSnapshot(a startupApplied) error {
	cleanSnapshotTemps(rn.dataDir)
	if err := migrateLegacySnapshot(rn.dataDir); err != nil {
		return err
	}
	if _, err := os.Stat(rn.snapshotPath()); os.IsNotExist(err) {
		return nil
	}
	m, err := verifySnapshotFile(rn.snapshotPath())
	if err != nil {
		return err
	}
	if !rn.snapshotCapable() {
		return fmt.Errorf("snapshot %s exists but %w", rn.snapshotPath(), errNotSnapshottable)
	}
	rn.lastIncludedIndex = m.Index
	rn.lastIncludedTerm = m.Term
	// A configuration that came from a log entry (ConfigIndex > 0) is the
	// persisted membership and wins over the bootstrap list.  ConfigIndex 0
	// is just some node's bootstrap list (--peers) captured in the header:
	// keep this node's own bootstrap, as before the first snapshot, so a
	// cluster that never changed membership online still follows --peers.
	if m.ConfigIndex > 0 {
		rn.baseConfig = m.Config.clone()
		rn.baseConfigIndex = m.ConfigIndex
	}
	if a.ok && !a.st.Restoring && a.st.Index >= m.Index {
		rn.startupSnap = &m // decided by finishStartupRestore
		return nil
	}
	return rn.startupRestore(m)
}

// finishStartupRestore decides a restore loadSnapshot deferred: skipped when
// the recorded applied state is trusted against the loaded log
// (trustApplied), else done.
func (rn *RaftNode) finishStartupRestore(a startupApplied, trusted bool) error {
	m := rn.startupSnap
	rn.startupSnap = nil
	if m == nil {
		return nil
	}
	if !trusted {
		return rn.startupRestore(*m)
	}
	rn.snapStats.startupRestoreSkips.Add(1)
	log.Printf("[raft] node=%s snapshot idx=%d term=%d NOT restored: the state machine already holds applied index %d",
		rn.id, m.Index, m.Term, a.st.Index)
	return nil
}

// startupRestore makes the state machine the snapshot's at startup, marking
// the restore in progress first and recording the snapshot index after.
func (rn *RaftNode) startupRestore(m snapshotMeta) error {
	if rn.appliedSM != nil {
		if err := rn.appliedSM.BeginRestore(); err != nil {
			return fmt.Errorf("mark restore in progress: %w", err)
		}
	}
	t0 := time.Now()
	if err := rn.restoreStateMachine(rn.snapshotPath(), m); err != nil {
		return fmt.Errorf("restore state machine: %w", err)
	}
	rn.snapStats.startupRestores.Add(1)
	log.Printf("[raft] node=%s restored snapshot idx=%d term=%d size=%d in %s",
		rn.id, m.Index, m.Term, m.FileSize(), time.Since(t0).Round(time.Millisecond))
	if rn.appliedSM != nil {
		if err := rn.appliedSM.SetAppliedIndex(m.Index, m.Term, true); err != nil {
			// Not fatal: the recorded index stays below the snapshot (or
			// Restoring), so the next start restores again.
			log.Printf("[raft] node=%s recording applied index %d after the restore failed: %v", rn.id, m.Index, err)
		}
	}
	return nil
}

// restoreStateMachine replaces the state machine's state with the snapshot
// file's.  The caller holds smMu (or runs before the node starts).
func (rn *RaftNode) restoreStateMachine(path string, m snapshotMeta) error {
	f, fm, ok, err := openSnapshotFile(path)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: %w", path, os.ErrNotExist)
	}
	defer f.Close()
	if !fm.sameFile(m) {
		return fmt.Errorf("%s changed underneath the restore", path)
	}
	src, err := newSnapshotReader(f, fm, rn.stoppedErr)
	if err != nil {
		return err
	}
	switch sm := rn.sm.(type) {
	case StreamingStateMachine:
		if err := sm.RestoreSnapshot(src, fm.Kind == payloadLegacyBlob); err != nil {
			return err
		}
	case SnapshotStateMachine:
		if fm.Kind != payloadLegacyBlob {
			return fmt.Errorf("record-stream snapshot needs a StreamingStateMachine")
		}
		blob, err := src.Next()
		if err != nil {
			return err
		}
		if err := sm.Restore(blob); err != nil {
			return err
		}
	default:
		return errNotSnapshottable
	}
	// Drain to the trailer: a source the state machine did not finish still
	// has its checksums verified (and a stream SM normally reads to EOF).
	for {
		if _, err := src.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// ── capture ─────────────────────────────────────────────────────────────────

// maybeSnapshot starts a background capture when the retained log has
// reached the threshold and there is applied state beyond the current
// snapshot.  Called from the applier goroutine after applying entries; the
// capture itself runs elsewhere, so Apply continues meanwhile.
func (rn *RaftNode) maybeSnapshot() {
	if rn.snapshotThreshold == 0 || !rn.snapshotCapable() {
		return
	}
	rn.mu.Lock()
	trigger := !rn.snapshotting &&
		uint64(len(rn.ps.Log)) >= rn.snapshotThreshold &&
		rn.lastApplied > rn.lastIncludedIndex &&
		!time.Now().Before(rn.captureRetryAt)
	if trigger {
		rn.snapshotting = true
	}
	rn.mu.Unlock()
	if trigger && !rn.spawn(rn.takeSnapshot) {
		rn.mu.Lock()
		rn.snapshotting = false
		rn.mu.Unlock()
	}
}

// captureAbortErr stops a capture on shutdown or when an install waits.
func (rn *RaftNode) captureAbortErr() error {
	if rn.stopped.Load() || rn.abortCapture.Load() {
		return errCaptureAborted
	}
	return nil
}

// takeSnapshot captures the state machine, installs the snapshot file, then
// truncates the covered log prefix.  Runs on its own goroutine (spawn).
func (rn *RaftNode) takeSnapshot() {
	ok := false
	defer func() {
		rn.mu.Lock()
		rn.snapshotting = false
		if ok {
			rn.captureFailures = 0
			rn.captureRetryAt = time.Time{}
		} else {
			rn.captureFailures++
			rn.snapStats.captureFailures.Add(1)
			back := time.Duration(1<<uint(min(rn.captureFailures, 16))) * 100 * time.Millisecond
			if back > captureRetryMax {
				back = captureRetryMax
			}
			rn.captureRetryAt = time.Now().Add(back)
		}
		rn.mu.Unlock()
	}()

	rn.snapMu.Lock()
	defer rn.snapMu.Unlock()
	if rn.stopped.Load() {
		return
	}

	t0 := time.Now()
	w, err := createSnapshotWriter(rn.dataDir, rn.captureAbortErr)
	if err != nil {
		log.Printf("[raft] node=%s snapshot capture failed: %v", rn.id, err)
		return
	}
	committed := false
	defer func() {
		if !committed {
			w.discard()
		}
	}()

	var meta snapshotMeta
	fenced := false
	var fenceHold time.Duration
	fence := func(fn func()) (uint64, error) {
		if fenced {
			return 0, errors.New("snapshot fence called twice")
		}
		fenced = true
		rn.smMu.Lock()
		start := time.Now()
		rn.mu.Lock()
		idx := rn.lastApplied
		if idx <= rn.lastIncludedIndex {
			rn.mu.Unlock()
			rn.smMu.Unlock()
			return 0, errSnapshotNotDue
		}
		term := rn.logTerm(idx)
		cfg, cfgIdx := rn.configAt(idx)
		rn.mu.Unlock()
		if term == 0 {
			rn.smMu.Unlock()
			return 0, fmt.Errorf("term of applied entry %d not resolvable", idx)
		}
		if fn != nil {
			fn()
		}
		fenceHold = time.Since(start)
		rn.smMu.Unlock()
		meta = snapshotMeta{Index: idx, Term: term, Config: cfg, ConfigIndex: cfgIdx, ID: newSnapshotID()}
		return idx, nil
	}

	switch sm := rn.sm.(type) {
	case StreamingStateMachine:
		meta.Kind = payloadStream
		err = sm.CaptureSnapshot(w, fence)
		meta.Kind = payloadStream
	case SnapshotStateMachine:
		var data []byte
		var serr error
		_, err = fence(func() { data, serr = sm.Snapshot() })
		if err == nil {
			err = serr
		}
		if err == nil {
			err = w.Write(data)
		}
		meta.Kind = payloadLegacyBlob
	default:
		err = errNotSnapshottable
	}
	if err == nil && !fenced {
		err = errors.New("state machine returned from CaptureSnapshot without calling the fence")
	}
	if err != nil {
		if !errors.Is(err, errSnapshotNotDue) && !errors.Is(err, errCaptureAborted) {
			log.Printf("[raft] node=%s snapshot capture failed: %v", rn.id, err)
		}
		if errors.Is(err, errSnapshotNotDue) {
			ok = true // not a failure: nothing new to capture
		}
		return
	}
	captureDur := time.Since(t0)
	if meta, err = w.finish(meta); err != nil {
		log.Printf("[raft] node=%s snapshot persist failed: %v", rn.id, err)
		return
	}
	rn.stage("capture-written")

	// Persist the snapshot BEFORE truncating the log: a crash in between
	// leaves overlap, which loadState resolves in the snapshot's favour.
	if err := installSnapshotFile(rn.dataDir, w.tmp); err != nil {
		log.Printf("[raft] node=%s snapshot persist failed: %v", rn.id, err)
		return
	}
	committed = true
	ok = true
	rn.stage("capture-installed")
	rn.snapStats.captures.Add(1)
	rn.snapStats.lastFenceHoldNs.Store(int64(fenceHold))
	rn.snapStats.lastCaptureNs.Store(int64(captureDur))
	rn.snapStats.lastSizeBytes.Store(meta.FileSize())

	// Truncate the covered prefix and rewrite the log file to the retained
	// suffix (one O(retained) rewrite per compaction; a failure leaves the
	// longer file, whose covered prefix loadState drops).
	rn.mu.Lock()
	idx, term := meta.Index, meta.Term
	if idx > rn.lastIncludedIndex {
		first := rn.logFirstIndex()
		if idx >= first {
			cut := idx - first + 1
			if cut >= uint64(len(rn.ps.Log)) {
				rn.ps.Log = nil
			} else {
				rn.ps.Log = append([]LogEntry(nil), rn.ps.Log[cut:]...)
			}
		}
		rn.lastIncludedIndex = idx
		rn.lastIncludedTerm = term
		rn.baseConfig = meta.Config.clone()
		rn.baseConfigIndex = meta.ConfigIndex
		_ = rn.rewriteLogLocked()
		log.Printf("[raft] node=%s snapshot taken  idx=%d term=%d size=%d records=%d capture=%s apply_fence=%s retained_log=%d",
			rn.id, idx, term, meta.FileSize(), meta.Count, captureDur.Round(time.Millisecond),
			fenceHold.Round(time.Microsecond), len(rn.ps.Log))
	}
	rn.mu.Unlock()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// snapshotStats are test / log metrics (atomic; read without locks).
type snapshotStats struct {
	captures        atomic.Uint64
	lastFenceHoldNs atomic.Int64 // Apply stall of the last capture
	lastCaptureNs   atomic.Int64
	lastSizeBytes   atomic.Uint64
	installs        atomic.Uint64
	chunksSent      atomic.Uint64
	chunksRecv      atomic.Uint64
	legacySent      atomic.Uint64
	// Startup: snapshot restores done / skipped because the state machine
	// already held the snapshot's state (applied.go).
	startupRestores     atomic.Uint64
	startupRestoreSkips atomic.Uint64
	startupResets       atomic.Uint64
	// Failure counters (SnapshotStatus).
	captureFailures  atomic.Uint64
	snapshotsSent    atomic.Uint64
	transferFailures atomic.Uint64
	installFailures  atomic.Uint64
	restoreFailures  atomic.Uint64
}

// ── leader: transfer ────────────────────────────────────────────────────────

// runSnapshotTransfer ships the snapshot to pr's peer on its own goroutine
// and restarts the peer's pipeline afterwards.  The replicator keeps sending
// heartbeats meanwhile (replicate).
func (rn *RaftNode) runSnapshotTransfer(pr *peerRepl) {
	installed, confirmed := rn.sendSnapshot(pr.peer, pr.term, pr.stop)
	rn.mu.Lock()
	pr.snapshotting = false
	// InstallSnapshot resets the pipeline: restart (probing) after it.
	pr.epoch++
	pr.probing = true
	if (!installed || !confirmed) && !pr.stopped {
		// Failed, or a single-shot whose reply cannot confirm the install
		// (v1.16 replies carry only a term): if the peer still needs a
		// snapshot, retry no sooner than this.
		pr.snapRetryAt = time.Now().Add(snapshotRetryMax)
	}
	rn.mu.Unlock()
	pr.wake()
}

// sendSnapshot ships the persisted snapshot to a follower whose nextIndex
// predates the log start, and reports whether the follower installed it
// (confirmed: the follower said so — a chunked transfer; a single-shot
// reply carries only a term).
// Called without rn.mu.  snapInFlight ensures at most one transfer per peer.
// cancel (the replicator's stop channel; nil = none) ends the transfer when
// the peer leaves the configuration or the replicator is replaced.
func (rn *RaftNode) sendSnapshot(peer string, term uint64, cancel <-chan struct{}) (installed, confirmed bool) {
	rn.mu.Lock()
	if rn.snapInFlight[peer] || rn.role != RoleLeader || rn.ps.CurrentTerm != term {
		rn.mu.Unlock()
		return false, false
	}
	caps, known := rn.peerCaps[peer]
	rn.snapInFlight[peer] = true
	rn.mu.Unlock()
	defer func() {
		rn.mu.Lock()
		delete(rn.snapInFlight, peer)
		rn.mu.Unlock()
	}()
	if !known {
		return false, false // no reply from the peer yet: capabilities unknown
	}

	// Pin this version of the file: renaming a newer snapshot over the path
	// does not change what this descriptor reads.
	f, meta, ok, err := openSnapshotFile(rn.snapshotPath())
	if err != nil || !ok {
		if err != nil {
			log.Printf("[raft] node=%s open snapshot for peer=%s failed: %v", rn.id, peer, err)
		}
		return false, false
	}
	defer f.Close()

	ct, chunked := rn.transport.(ChunkTransport)
	if chunked && caps&capSnapshotChunks != 0 && !rn.legacyPeer {
		installed = rn.sendSnapshotChunks(ct, peer, term, f, meta, cancel)
		confirmed = installed
	} else {
		installed = rn.sendSnapshotLegacy(peer, term, f, meta)
	}
	if !installed {
		rn.snapStats.transferFailures.Add(1)
		return false, false
	}
	rn.snapStats.snapshotsSent.Add(1)

	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.role != RoleLeader || rn.ps.CurrentTerm != term {
		return false, false // stale
	}
	if meta.Index > rn.matchIndex[peer] {
		rn.matchIndex[peer] = meta.Index
	}
	if rn.nextIndex[peer] <= meta.Index {
		rn.nextIndex[peer] = meta.Index + 1
	}
	// matchIndex ≤ commitIndex here (a snapshot holds only applied, i.e.
	// committed, entries), so an unconfirmed single-shot can never make the
	// leader commit anything.
	rn.maybeAdvanceCommit()
	return true, confirmed
}

// restartTransfer reports whether a chunked transfer that just failed to
// reach peer should be abandoned and restarted by the replicator instead of
// retried: the peer no longer advertises chunk support (it was restarted as
// an older version), or nothing was accepted yet and a newer snapshot has
// replaced the pinned one (a peer that was unreachable for a while would
// otherwise first get a stale snapshot, then need another).  With pre-vote
// a returning peer no longer forces a leader change, which used to restart
// such a transfer as a side effect.
func (rn *RaftNode) restartTransfer(peer string, meta snapshotMeta, off uint64) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if caps, ok := rn.peerCaps[peer]; ok && caps&capSnapshotChunks == 0 {
		return true
	}
	return off == 0 && rn.lastIncludedIndex > meta.Index
}

// stillLeading reports whether this node still leads in term (and is not
// stopping); a transfer stops as soon as it does not.  A reply carrying a
// higher term makes the node step down.
func (rn *RaftNode) stillLeading(term, replyTerm uint64) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if replyTerm > rn.ps.CurrentTerm {
		rn.becomeFollower(replyTerm)
		return false
	}
	return !rn.stopped.Load() && rn.role == RoleLeader && rn.ps.CurrentTerm == term
}

func (rn *RaftNode) sleepOrStop(d time.Duration) bool {
	return rn.sleepOrCancel(d, nil)
}

// sleepOrCancel sleeps d; false when the node stops or cancel closes first.
func (rn *RaftNode) sleepOrCancel(d time.Duration, cancel <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-rn.done:
		return false
	case <-cancel:
		return false
	case <-t.C:
		return true
	}
}

func cancelled(cancel <-chan struct{}) bool {
	select {
	case <-cancel:
		return true
	default:
		return false
	}
}

// sendSnapshotChunks streams f in chunks, one in flight, following the
// follower's NextOffset, then polls until the follower has installed it.
func (rn *RaftNode) sendSnapshotChunks(ct ChunkTransport, peer string, term uint64, f *os.File, meta snapshotMeta, cancel <-chan struct{}) bool {
	size := meta.FileSize()
	chunk := rn.snapChunkSize
	if chunk <= 0 {
		chunk = DefaultSnapshotChunkSize
	}
	buf := make([]byte, chunk)
	off := uint64(0)
	backoff := 20 * time.Millisecond
	t0 := time.Now()
	for {
		if cancelled(cancel) || !rn.stillLeading(term, 0) {
			return false
		}
		args := InstallSnapshotChunkArgs{
			Term: term, LeaderID: rn.id,
			LastIncludedIndex: meta.Index, LastIncludedTerm: meta.Term,
			SnapshotID: meta.ID, Size: size, Offset: off,
		}
		if off < size {
			n := uint64(chunk)
			if off+n > size {
				n = size - off
			}
			if _, err := f.ReadAt(buf[:n], int64(off)); err != nil {
				log.Printf("[raft] node=%s read snapshot for peer=%s failed: %v", rn.id, peer, err)
				return false
			}
			args.Data = buf[:n]
			args.Done = off+n == size
		} else {
			args.Done = true // status poll
		}
		reply, err := ct.SendInstallSnapshotChunk(peer, args)
		if err != nil {
			if rn.restartTransfer(peer, meta, off) {
				return false // the replicator retries with the current file / caps
			}
			if !rn.sleepOrCancel(backoff, cancel) {
				return false
			}
			if backoff *= 2; backoff > snapshotRetryMax {
				backoff = snapshotRetryMax
			}
			continue // resend at off; the follower's reply re-aligns us
		}
		if !rn.stillLeading(term, reply.Term) {
			return false
		}
		if args.Data != nil {
			rn.snapStats.chunksSent.Add(1)
			if reply.NextOffset > args.Offset {
				backoff = 20 * time.Millisecond
			}
		}
		switch {
		case reply.Installed:
			log.Printf("[raft] node=%s snapshot idx=%d (%d bytes) installed on peer=%s in %s",
				rn.id, meta.Index, size, peer, time.Since(t0).Round(time.Millisecond))
			return true
		case reply.Failed:
			log.Printf("[raft] node=%s peer=%s failed to install snapshot idx=%d", rn.id, peer, meta.Index)
			return false
		case reply.Installing, reply.Busy:
			off = size
			if !rn.sleepOrCancel(snapshotPollInterval, cancel) {
				return false
			}
		default:
			next := reply.NextOffset
			if next > size {
				next = 0
			}
			if next <= args.Offset && len(args.Data) > 0 {
				// No progress (the follower restarted the transfer, or
				// could not write): back off instead of spinning.
				if !rn.sleepOrCancel(backoff, cancel) {
					return false
				}
				if backoff *= 2; backoff > snapshotRetryMax {
					backoff = snapshotRetryMax
				}
			}
			off = next
		}
	}
}

// sendSnapshotLegacy sends the whole snapshot in one v1.16 InstallSnapshot.
func (rn *RaftNode) sendSnapshotLegacy(peer string, term uint64, f *os.File, meta snapshotMeta) bool {
	rn.mu.Lock()
	refused := rn.legacyRefused[peer] == meta.ID
	rn.mu.Unlock()
	if refused {
		return false
	}
	src, err := newSnapshotReader(f, meta, nil)
	if err != nil {
		return false
	}
	var data []byte
	if meta.Kind == payloadLegacyBlob {
		if meta.FileSize() <= uint64(legacyMaxSnapshotBytes)+snapHeaderSize+64 {
			data, err = src.Next()
		} else {
			err = fmt.Errorf("snapshot is %d bytes, over the %d-byte single-shot limit", meta.FileSize(), legacyMaxSnapshotBytes)
		}
	} else if enc, ok := rn.sm.(LegacySnapshotEncoder); ok {
		var buf bytes.Buffer
		err = enc.EncodeLegacySnapshot(src, &buf, legacyMaxSnapshotBytes)
		data = buf.Bytes()
	} else {
		err = errors.New("state machine cannot encode the pre-streaming snapshot format")
	}
	if err == nil && len(data) > legacyMaxSnapshotBytes {
		err = fmt.Errorf("snapshot is %d bytes, over the %d-byte single-shot limit", len(data), legacyMaxSnapshotBytes)
	}
	if err != nil {
		rn.mu.Lock()
		rn.legacyRefused[peer] = meta.ID
		rn.mu.Unlock()
		log.Printf("[raft] node=%s NOT sending snapshot idx=%d to peer=%s: the peer does not support chunked snapshots (pre-streaming build, e.g. v1.16) and the single-shot fallback is impossible: %v — upgrade peer=%s to catch it up",
			rn.id, meta.Index, peer, err, peer)
		return false
	}
	args := InstallSnapshotArgs{
		Term: term, LeaderID: rn.id,
		LastIncludedIndex: meta.Index, LastIncludedTerm: meta.Term,
		Config: meta.Config.clone(), ConfigIndex: meta.ConfigIndex,
		Data: data,
	}
	reply, err := rn.transport.SendInstallSnapshot(peer, args)
	if err != nil {
		return false
	}
	if !rn.stillLeading(term, reply.Term) {
		return false
	}
	rn.snapStats.legacySent.Add(1)
	log.Printf("[raft] node=%s sent single-shot snapshot idx=%d (%d bytes) to pre-streaming peer=%s",
		rn.id, meta.Index, len(data), peer)
	return true
}

// ── follower: receive ───────────────────────────────────────────────────────

// snapRecv is the follower's in-progress chunked transfer (guarded by
// rn.recvMu; state is atomic so the install goroutine can publish it).
type snapRecv struct {
	index, term, id, size uint64
	f                     *os.File
	written               uint64
	state                 atomic.Int32 // recvReceiving / recvInstalling / recvInstalled / recvFailed
}

const (
	recvReceiving int32 = iota
	recvInstalling
	recvInstalled
	recvFailed
)

func (st *snapRecv) matches(a InstallSnapshotChunkArgs) bool {
	return st.index == a.LastIncludedIndex && st.term == a.LastIncludedTerm &&
		st.id == a.SnapshotID && st.size == a.Size
}

func (rn *RaftNode) recvPath() string { return filepath.Join(rn.dataDir, snapRecvFileName) }

// dropRecvLocked discards the receive state (rn.recvMu held).
func (rn *RaftNode) dropRecvLocked() {
	if st := rn.recv; st != nil {
		if st.f != nil {
			st.f.Close()
			st.f = nil
		}
		if st.state.Load() == recvReceiving {
			os.Remove(rn.recvPath())
		}
	}
	rn.recv = nil
}

// acceptLeaderLocked is the common term handling of a snapshot RPC from a
// leader (rn.mu held).  ok=false: the RPC is from a stale term (or the new
// term could not be persisted) and must be answered with reply term only.
func (rn *RaftNode) acceptLeaderLocked(term uint64, leader string) bool {
	if rn.stopped.Load() || term < rn.ps.CurrentTerm {
		return false
	}
	if term > rn.ps.CurrentTerm {
		rn.becomeFollower(term)
		if rn.persistMetaLocked() != nil {
			return false
		}
	}
	rn.role = RoleFollower
	rn.currentRole.Store(int32(RoleFollower))
	rn.LeaderID.Store(leader)
	rn.resetElectionTimer()
	rn.noteLeaderContactLocked()
	return true
}

// HandleInstallSnapshotChunk processes one chunk (see the file comment).  It
// never holds rn.mu across I/O and never waits for an install.
func (rn *RaftNode) HandleInstallSnapshotChunk(args InstallSnapshotChunkArgs) InstallSnapshotChunkReply {
	rn.recvMu.Lock()
	defer rn.recvMu.Unlock()

	rn.mu.Lock()
	reply := InstallSnapshotChunkReply{Term: rn.ps.CurrentTerm}
	if rn.legacyPeer || !rn.acceptLeaderLocked(args.Term, args.LeaderID) {
		rn.mu.Unlock()
		return reply
	}
	reply.Term = rn.ps.CurrentTerm
	covered := args.LastIncludedIndex <= rn.lastIncludedIndex || args.LastIncludedIndex <= rn.lastApplied
	rn.mu.Unlock()

	st := rn.recv
	if st != nil && st.matches(args) {
		switch st.state.Load() {
		case recvInstalling:
			reply.Installing = true
			return reply
		case recvInstalled:
			reply.Installed = true
			return reply
		case recvFailed:
			rn.dropRecvLocked()
			reply.Failed = true
			return reply
		}
	} else {
		if covered {
			// Our state already includes this index (it is committed, so
			// it matches the leader's history through it).
			reply.Installed = true
			return reply
		}
		if st != nil && st.state.Load() == recvInstalling {
			reply.Busy = true // another snapshot is being installed
			return reply
		}
		if args.Offset != 0 {
			reply.NextOffset = 0 // unknown transfer: start from the beginning
			return reply
		}
		rn.dropRecvLocked()
		f, err := os.OpenFile(rn.recvPath(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("[raft] node=%s create snapshot receive file: %v", rn.id, err)
			return reply
		}
		st = &snapRecv{index: args.LastIncludedIndex, term: args.LastIncludedTerm,
			id: args.SnapshotID, size: args.Size, f: f}
		rn.recv = st
		log.Printf("[raft] node=%s receiving snapshot idx=%d term=%d size=%d from %s",
			rn.id, args.LastIncludedIndex, args.LastIncludedTerm, args.Size, args.LeaderID)
	}

	if args.Offset != st.written || st.written+uint64(len(args.Data)) > st.size {
		reply.NextOffset = st.written // duplicate, reordered or gap: say where we are
		return reply
	}
	if len(args.Data) > 0 {
		if _, err := st.f.Write(args.Data); err != nil {
			log.Printf("[raft] node=%s write snapshot chunk: %v", rn.id, err)
			rn.dropRecvLocked()
			return reply // NextOffset 0
		}
		st.written += uint64(len(args.Data))
		rn.snapStats.chunksRecv.Add(1)
	}
	reply.NextOffset = st.written
	if !args.Done || st.written != st.size {
		return reply
	}

	// Complete.  The fsync and the full checksum pass can take seconds on a
	// large file, so they run on the install goroutine — this handler runs
	// on the stream's read loop, and heartbeats queue behind it.
	f := st.f
	st.f = nil
	st.state.Store(recvInstalling)
	tmp := filepath.Join(rn.dataDir, fmt.Sprintf("raft_snapshot_recv_%x.tmp", st.id))
	want := snapshotMeta{Index: st.index, Term: st.term, ID: st.id, BodyLen: st.size - snapHeaderSize}
	from := args.LeaderID
	rn.installing.Add(1) // quiet elections from now until the install ends
	if !rn.spawn(func() {
		defer rn.installing.Add(-1)
		err := f.Sync()
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		var meta snapshotMeta
		if err == nil {
			meta, err = verifySnapshotFile(rn.recvPath())
		}
		if err == nil && !meta.sameFile(want) {
			err = corrupt("received header idx=%d term=%d id=%x size=%d does not match the transfer",
				meta.Index, meta.Term, meta.ID, meta.FileSize())
		}
		if err == nil {
			err = os.Rename(rn.recvPath(), tmp)
		}
		if err != nil {
			log.Printf("[raft] node=%s received snapshot idx=%d rejected (%v) — the leader restarts the transfer", rn.id, want.Index, err)
			rn.snapStats.installFailures.Add(1)
			os.Remove(rn.recvPath())
			st.state.Store(recvFailed)
			return
		}
		if err := rn.installSnapshot(tmp, meta, from); err != nil {
			if !errors.Is(err, errRestoreAborted) {
				rn.snapStats.installFailures.Add(1)
			}
			st.state.Store(recvFailed)
			return
		}
		st.state.Store(recvInstalled)
	}) {
		rn.installing.Add(-1)
		f.Close()
		st.state.Store(recvFailed)
		return reply
	}
	reply.Installing = true
	return reply
}

// HandleInstallSnapshot processes a single-shot (v1.16 wire format)
// InstallSnapshot: the blob is written as a legacy-blob snapshot file and
// installed through the same path as a chunked transfer.  The reply is sent
// once the install finished.
func (rn *RaftNode) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	rn.mu.Lock()
	reply := InstallSnapshotReply{Term: rn.ps.CurrentTerm}
	if !rn.acceptLeaderLocked(args.Term, args.LeaderID) {
		rn.mu.Unlock()
		return reply
	}
	reply.Term = rn.ps.CurrentTerm
	stale := args.LastIncludedIndex <= rn.lastIncludedIndex || args.LastIncludedIndex <= rn.lastApplied
	rn.mu.Unlock()
	if stale || !rn.snapshotCapable() {
		if !stale {
			log.Printf("[raft] node=%s cannot install snapshot: %v", rn.id, errNotSnapshottable)
		}
		return reply
	}
	tmp, meta, err := writeLegacyBlobSnapshot(rn.dataDir, snapshotMeta{
		Index: args.LastIncludedIndex, Term: args.LastIncludedTerm,
		Config: args.Config.clone(), ConfigIndex: args.ConfigIndex,
	}, args.Data)
	if err != nil {
		log.Printf("[raft] node=%s persist installed snapshot failed: %v", rn.id, err)
		rn.snapStats.installFailures.Add(1)
		return reply
	}
	if err := rn.installSnapshot(tmp, meta, args.LeaderID); err != nil {
		log.Printf("[raft] node=%s install snapshot from %s failed: %v", rn.id, args.LeaderID, err)
		if !errors.Is(err, errRestoreAborted) {
			rn.snapStats.installFailures.Add(1)
		}
	}
	rn.mu.Lock()
	reply.Term = rn.ps.CurrentTerm
	rn.mu.Unlock()
	return reply
}

// installSnapshot installs a complete, verified snapshot file (tmp) on this
// node; see "Install" in the file comment.  tmp is consumed.
func (rn *RaftNode) installSnapshot(tmp string, meta snapshotMeta, from string) error {
	defer os.Remove(tmp) // no-op once renamed
	rn.abortCapture.Store(true)
	rn.snapMu.Lock()
	defer rn.snapMu.Unlock()
	rn.abortCapture.Store(false)
	rn.installing.Add(1)
	defer func() {
		rn.installing.Add(-1)
		rn.mu.Lock()
		rn.resetElectionTimer()
		rn.mu.Unlock()
	}()

	rn.smMu.Lock()
	defer rn.smMu.Unlock()
	if rn.stopped.Load() {
		return errRestoreAborted
	}

	rn.mu.Lock()
	if meta.Index <= rn.lastIncludedIndex || meta.Index <= rn.lastApplied {
		rn.mu.Unlock()
		return nil // stale: our state already includes it
	}
	rn.mu.Unlock()

	// (1) Commit point.
	rn.stage("install-verified")
	if err := installSnapshotFile(rn.dataDir, tmp); err != nil {
		return fmt.Errorf("persist snapshot: %w", err)
	}
	rn.stage("install-renamed")

	// (2) Log and configuration.
	rn.mu.Lock()
	// Retain any log suffix that extends past the snapshot and agrees with it
	// (Raft §7); otherwise discard the entire log.
	if meta.Index < rn.lastLogIndex() && rn.logTerm(meta.Index) == meta.Term {
		rn.ps.Log = append([]LogEntry(nil), rn.entriesFrom(meta.Index+1)...)
	} else {
		rn.ps.Log = nil
	}
	rn.lastIncludedIndex = meta.Index
	rn.lastIncludedTerm = meta.Term
	if rn.commitIndex < meta.Index {
		rn.commitIndex = meta.Index
	}
	rn.baseConfig = meta.Config.clone()
	rn.baseConfigIndex = meta.ConfigIndex
	rn.refreshConfigFromLog()
	_ = rn.rewriteLogLocked()
	decided := rn.takeDecidedWaitersLocked()
	rn.mu.Unlock()
	respondAll(decided)
	rn.stage("install-log-rewritten")

	// (3) State machine, with Apply excluded (smMu) but rn.mu free.  An
	// AppliedIndexStateMachine first records, durably, that a restore is in
	// progress: a restart then restores again instead of trusting its
	// recorded applied index (applied.go).
	t0 := time.Now()
	var err error
	for attempt := 0; ; attempt++ {
		if rn.appliedSM != nil {
			err = rn.appliedSM.BeginRestore()
		}
		if err == nil {
			err = rn.restoreStateMachine(rn.snapshotPath(), meta)
		}
		if err == nil {
			break
		}
		if errors.Is(err, errRestoreAborted) || rn.stopped.Load() {
			return err // redone from raft_snapshot.dat on restart
		}
		rn.snapStats.restoreFailures.Add(1)
		log.Printf("[raft] node=%s restore of installed snapshot idx=%d failed (attempt %d), retrying — the state machine is NOT usable until it succeeds: %v",
			rn.id, meta.Index, attempt+1, err)
		if !rn.sleepOrStop(time.Duration(min(attempt+1, 10)) * time.Second) {
			return errRestoreAborted
		}
	}

	if rn.appliedSM != nil {
		if err := rn.appliedSM.SetAppliedIndex(meta.Index, meta.Term, true); err != nil {
			// Not fatal: the next start sees Restoring and restores again.
			log.Printf("[raft] node=%s recording applied index %d after the install failed: %v", rn.id, meta.Index, err)
		}
	}
	rn.stage("install-restored")

	// (4) Applied.
	rn.mu.Lock()
	if rn.lastApplied < meta.Index {
		rn.lastApplied = meta.Index
	}
	retained := len(rn.ps.Log)
	rn.mu.Unlock()
	rn.notifyApplier()
	rn.snapStats.installs.Add(1)
	log.Printf("[raft] node=%s installed snapshot from %s  idx=%d term=%d size=%d restore=%s retained_log=%d",
		rn.id, from, meta.Index, meta.Term, meta.FileSize(), time.Since(t0).Round(time.Millisecond), retained)
	return nil
}

// SnapshotStatus is a point-in-time view of a node's snapshot state.
type SnapshotStatus struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	LastApplied       uint64
	CommitIndex       uint64
	RetainedLog       int
	Captures          uint64        // snapshots taken by this node
	Installs          uint64        // snapshots installed from a leader
	LastApplyFence    time.Duration // Apply stall of the last capture
	LastCapture       time.Duration // duration of the last capture
	LastSnapshotBytes uint64        // file size of the last capture
	ChunksSent        uint64
	ChunksReceived    uint64
	LegacySent        uint64 // single-shot InstallSnapshots sent to v1.16 peers
	// StartupRestores / StartupRestoreSkips: at this node's start, the
	// snapshot was restored into the state machine / not, because the state
	// machine's recorded applied index already covered it (applied.go).
	StartupRestores     uint64
	StartupRestoreSkips uint64
	// StartupResets: the state machine was reset to empty at start and the
	// whole log applied (no snapshot, no trusted applied index).
	StartupResets uint64
	// CaptureFailures: captures that ended without a new snapshot (error,
	// or aborted by an install / shutdown); retried with backoff.
	CaptureFailures uint64
	// SnapshotsSent: whole-snapshot transfers to a follower that completed
	// (chunked: the follower confirmed the install; single-shot: the v1.16
	// peer accepted it — also counted in LegacySent).
	SnapshotsSent uint64
	// TransferFailures: transfers to a follower that ended without that —
	// a send, read or install failure on the follower, a single-shot that
	// is impossible (over 256 MB), or an abort (leader change, peer removed).
	TransferFailures uint64
	// InstallFailures: snapshots received from a leader that this node did
	// not install (failed verification, persist or install; shutdown
	// aborts not counted).  RestoreFailures: failed state-machine restore
	// attempts of an install (retried until it succeeds; the state machine
	// is not usable meanwhile).
	InstallFailures uint64
	RestoreFailures uint64
}

// SnapshotStatus returns the node's snapshot counters and log position.
func (rn *RaftNode) SnapshotStatus() SnapshotStatus {
	rn.mu.Lock()
	st := SnapshotStatus{
		LastIncludedIndex: rn.lastIncludedIndex,
		LastIncludedTerm:  rn.lastIncludedTerm,
		LastApplied:       rn.lastApplied,
		CommitIndex:       rn.commitIndex,
		RetainedLog:       len(rn.ps.Log),
	}
	rn.mu.Unlock()
	s := &rn.snapStats
	st.Captures = s.captures.Load()
	st.Installs = s.installs.Load()
	st.LastApplyFence = time.Duration(s.lastFenceHoldNs.Load())
	st.LastCapture = time.Duration(s.lastCaptureNs.Load())
	st.LastSnapshotBytes = s.lastSizeBytes.Load()
	st.ChunksSent = s.chunksSent.Load()
	st.ChunksReceived = s.chunksRecv.Load()
	st.LegacySent = s.legacySent.Load()
	st.StartupRestores = s.startupRestores.Load()
	st.StartupRestoreSkips = s.startupRestoreSkips.Load()
	st.StartupResets = s.startupResets.Load()
	st.CaptureFailures = s.captureFailures.Load()
	st.SnapshotsSent = s.snapshotsSent.Load()
	st.TransferFailures = s.transferFailures.Load()
	st.InstallFailures = s.installFailures.Load()
	st.RestoreFailures = s.restoreFailures.Load()
	return st
}

// SetSnapshotStageHookForTesting installs fn to be called at every
// crash-relevant stage of a snapshot capture ("capture-written",
// "capture-installed") or install ("install-verified", "install-renamed",
// "install-log-rewritten", "install-restored") with the node's ID.  For
// crash-injection tests only (the server wires it to
// VELTRIX_TEST_CRASH_AT_SNAPSHOT_STAGE); nil removes it.
func SetSnapshotStageHookForTesting(fn func(nodeID, stage string)) {
	if fn == nil {
		setStageHook(nil)
		return
	}
	setStageHook(func(rn *RaftNode, stage string) { fn(rn.id, stage) })
}
