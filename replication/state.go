package replication

// state.go — durable per-replica recovery state (ReplicationConfig.StatePath).
//
// The retention buffer, per-op acks and needsResync live in memory.  Without
// this file a primary restart forgot everything a down replica had not
// acknowledged: the replica came back, was treated as caught up, and every
// write and delete made before the restart was missing on it forever.
//
// What is persisted is one number per replica: its acknowledgement watermark
// W (Unix ns, this node's clock) — the replica has acked every write this node
// originated with a timestamp below W.  It is the same value the lag monitor
// already computes for tombstone GC (updateLag), before the GC safety margin.
//
// On startup (AddReplica) a replica with a persisted W starts FAILED with
// needsResync from W, so the recovery worker serves it from the change feed
// (CatchUpFunc, from W - catchUpSkew) before it may return to SYNC — the same
// path that serves ops evicted from the retention buffer (invariant 61).
//
// Safety of an amortised (stale) W.  The file is rewritten at most once per
// lag-monitor tick (1 s), and only when a watermark changed; the write path
// never touches it.  A persisted W was true when it was written and W only
// claims something about writes BEFORE it, so any later progress lost in a
// crash merely makes the restart catch-up start earlier and re-send keys the
// replica already has.  Re-sending is idempotent: the change feed carries the
// primary's CURRENT value or tombstone of each key, never an older version,
// and the replica receives no live batch while it is FAILED / SYNC_PENDING —
// every write made after the feed read the key is a retained op, replayed
// after the catch-up in SeqNum order.  Replicas apply in arrival order (no
// timestamp comparison), so this ordering is what keeps a re-sent value from
// overwriting a newer one.
//
// The file is written to a temp file, fsynced, renamed over StatePath and the
// directory fsynced, so a crash leaves either the old or the new state.

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

const stateFormatVersion = 1

// persistedState is the on-disk JSON document.
type persistedState struct {
	Version  int                         `json:"version"`
	NodeID   string                      `json:"node_id"`
	SavedAt  int64                       `json:"saved_at_ns"`
	Replicas map[string]persistedReplica `json:"replicas"`
}

type persistedReplica struct {
	// WatermarkNs: the replica acknowledged every write this node originated
	// with an earlier timestamp (Unix ns).
	WatermarkNs int64 `json:"watermark_ns"`
}

// loadState reads StatePath.  A missing file is not an error (first start,
// or an upgrade from a build without it).  A file that cannot be decoded is
// reported as corrupt: the caller then resyncs every replica from the start
// of the change feed rather than trust nothing.
func loadState(path string) (st *persistedState, corrupt bool, err error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	var s persistedState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, true, fmt.Errorf("decode %s: %w", path, err)
	}
	if s.Version != stateFormatVersion {
		return nil, true, fmt.Errorf("%s: unsupported format version %d", path, s.Version)
	}
	if s.Replicas == nil {
		s.Replicas = map[string]persistedReplica{}
	}
	return &s, false, nil
}

// saveState writes st to path atomically and durably.
func saveState(path string, st *persistedState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// initState loads StatePath once, at construction.  Caller holds no lock.
func (re *ReplicationEngine) initState() {
	if re.config == nil || re.config.StatePath == "" {
		return
	}
	st, corrupt, err := loadState(re.config.StatePath)
	switch {
	case corrupt:
		// Unknown progress: every replica is caught up from the beginning of
		// the change feed (resyncFromNs = 1, the earliest representable).
		log.Printf("[repl] ERROR replication state %s unreadable (%v) — every replica will be resynced from the start of the change feed",
			re.config.StatePath, err)
		re.stateCorrupt = true
	case st == nil:
		log.Printf("[repl] no replication state at %s — replicas are taken as caught up as of now (first start, or upgrade from a build without it)",
			re.config.StatePath)
	default:
		if st.NodeID != "" && st.NodeID != re.localNodeID {
			log.Printf("[repl] WARNING replication state %s was written by node %q, this is %q — using it anyway",
				re.config.StatePath, st.NodeID, re.localNodeID)
		}
		re.loaded = st.Replicas
		log.Printf("[repl] loaded recovery state for %d replica(s) from %s", len(st.Replicas), re.config.StatePath)
	}
}

// persistStateIfChanged writes marks (computed under re.mu, with gen from
// re.markGen taken at the same time) when they differ from the last state
// written.  A snapshot older than the last one written is dropped, so a slow
// writer can never replace newer state (e.g. drop a replica AddReplica just
// recorded).  Serialised by re.persistMu; never called with re.mu held.
func (re *ReplicationEngine) persistStateIfChanged(marks map[string]int64, gen uint64) {
	if re.config == nil || re.config.StatePath == "" {
		return
	}
	re.persistMu.Lock()
	defer re.persistMu.Unlock()
	if gen < re.lastPersistedGen || sameMarks(marks, re.lastPersisted) {
		return
	}
	st := &persistedState{
		Version:  stateFormatVersion,
		NodeID:   re.localNodeID,
		SavedAt:  time.Now().UnixNano(),
		Replicas: make(map[string]persistedReplica, len(marks)),
	}
	for id, w := range marks {
		st.Replicas[id] = persistedReplica{WatermarkNs: w}
	}
	if err := saveState(re.config.StatePath, st); err != nil {
		re.metrics.StatePersistErrors.Add(1)
		log.Printf("[repl] ERROR persist replication state %s: %v", re.config.StatePath, err)
		return
	}
	re.metrics.StatePersists.Add(1)
	re.lastPersisted = marks
	re.lastPersistedGen = gen
}

func sameMarks(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}
