// Package adminapi exposes a small HTTP API for operational tasks: trigger
// compaction, drop a key range, dump engine stats, force a checkpoint, query
// per-namespace quotas, run schema migrations, and stream CDC events.
//
// Mount via:
//
//	mux := http.NewServeMux()
//	adminapi.Register(mux, engine, "/admin")
//
// The same binary already exposes /metrics, /healthz, /readyz on a separate
// HTTP listener; admin API is mounted on the same listener at /admin/* by
// cmd/server/main.go so a single port carries all operator traffic.
//
// Auth: cmd/server wraps everything registered here in Guard (guard.go):
// loopback-only by default, bearer-token (--admin-token) for remote access.
// The admin endpoints can read and destroy data — still front the port with
// a network policy / firewall on untrusted networks; the token is a second
// layer, not a substitute for network isolation.
package adminapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// Engine is the subset of *storage.StorageEngine the admin API needs. Defined
// as an interface so tests can inject a mock without spinning up a full engine.
type Engine interface {
	GetIndexSize() int
	GetCacheStats() storage.CacheStats
	GetVLogStats() []storage.VLogStats
	GetMetrics() *storage.StorageMetrics
	GetWALTotals() (uint64, uint64)
	Checkpoint() error
	ListNamespaces() []storage.NSInfo
	QuotaStats() []storage.QuotaSnapshot
	SetNamespaceLimit(string, storage.QuotaLimit)
	MigrateAll() (int, int)
	CDCStats() (uint64, uint64, int)
	Subscribe(int, string) (<-chan storage.CDCEvent, func())
	ChangesSince(int64, int) storage.ChangesSinceResult
}

// BackupEngine is the subset of *storage.BackupEngine needed by the admin API.
// Pass nil to Register to disable the /admin/backup endpoint.
type BackupEngine interface {
	FullBackup(destDir string) (*storage.BackupManifest, error)
	IncrementalBackup(destDir string, base *storage.BackupManifest) (*storage.BackupManifest, error)
}

// Register mounts handlers under prefix on mux. Idempotent: safe to call once.
// be may be nil — when non-nil the /admin/backup endpoint is enabled.
func Register(mux *http.ServeMux, e Engine, prefix string, be BackupEngine) {
	if prefix == "" {
		prefix = "/admin"
	}
	prefix = strings.TrimRight(prefix, "/")

	mux.HandleFunc(prefix+"/stats", func(w http.ResponseWriter, r *http.Request) {
		handleStats(w, r, e)
	})
	mux.HandleFunc(prefix+"/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		handleCheckpoint(w, r, e)
	})
	mux.HandleFunc(prefix+"/quotas", func(w http.ResponseWriter, r *http.Request) {
		handleQuotas(w, r, e)
	})
	mux.HandleFunc(prefix+"/migrate", func(w http.ResponseWriter, r *http.Request) {
		handleMigrate(w, r, e)
	})
	mux.HandleFunc(prefix+"/cdc", func(w http.ResponseWriter, r *http.Request) {
		handleCDC(w, r, e)
	})
	mux.HandleFunc(prefix+"/changes", func(w http.ResponseWriter, r *http.Request) {
		handleChanges(w, r, e)
	})
	mux.HandleFunc(prefix+"/version", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"current_schema_version": int(storage.CurrentSchemaVersion),
			"encryption_enabled":     storage.EncryptionEnabled(),
		})
	})
	if be != nil {
		mux.HandleFunc(prefix+"/backup", func(w http.ResponseWriter, r *http.Request) {
			handleBackup(w, r, be)
		})
	}
	mux.HandleFunc(prefix+"/ui", handleUI)
	mux.HandleFunc(prefix+"/", handleUI)
}

// handleChanges serves the durable catch-up feed (storage.ChangesSince):
// GET /admin/changes?since=<µs>&limit=<n>  →  JSONL events, then one trailer
// line {"cursor":<µs>,"more":<bool>}.  repl-ship uses this on restart to
// replay writes it missed while it was down, before rejoining the live
// /admin/cdc stream.
func handleChanges(w http.ResponseWriter, r *http.Request, e Engine) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", 405)
		return
	}
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100_000 {
		limit = 10_000
	}
	res := e.ChangesSince(since, limit)

	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, ev := range res.Events {
		if err := enc.Encode(ev); err != nil {
			return
		}
	}
	_ = enc.Encode(map[string]any{"cursor": res.Cursor, "more": res.More})
}

func handleStats(w http.ResponseWriter, r *http.Request, e Engine) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", 405)
		return
	}
	cs := e.GetCacheStats()
	m := e.GetMetrics()
	walBytes, walEntries := e.GetWALTotals()
	cdcTotal, cdcDropped, cdcSubs := e.CDCStats()
	vlogs := e.GetVLogStats()
	ns := e.ListNamespaces()

	writeJSON(w, 200, map[string]any{
		"index_keys":           e.GetIndexSize(),
		"writes_total":         m.Writes.Load(),
		"reads_total":          m.Reads.Load(),
		"deletes_total":        m.Deletes.Load(),
		"atomic_ops_total":     m.AtomicOps.Load(),
		"audit_dropped_total":  m.AuditDropped.Load(),
		"cache":                cs,
		"wal_bytes":            walBytes,
		"wal_entries":          walEntries,
		"cdc_broadcast_total":  cdcTotal,
		"cdc_dropped_total":    cdcDropped,
		"cdc_subscribers":      cdcSubs,
		"vlogs":                vlogs,
		"namespaces":           ns,
	})
}

func handleCheckpoint(w http.ResponseWriter, r *http.Request, e Engine) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	start := time.Now()
	if err := e.Checkpoint(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, map[string]any{
		"status":      "ok",
		"duration_ms": time.Since(start).Milliseconds(),
	})
}

func handleQuotas(w http.ResponseWriter, r *http.Request, e Engine) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, e.QuotaStats())
	case http.MethodPost:
		ns := r.FormValue("ns")
		writesPerSec, _ := strconv.Atoi(r.FormValue("writes_per_sec"))
		burst, _ := strconv.Atoi(r.FormValue("burst"))
		maxKeys, _ := strconv.ParseInt(r.FormValue("max_keys"), 10, 64)
		e.SetNamespaceLimit(ns, storage.QuotaLimit{
			WritesPerSec: writesPerSec,
			BurstWrites:  burst,
			MaxKeys:      maxKeys,
		})
		writeJSON(w, 200, map[string]any{"status": "ok", "ns": ns})
	default:
		http.Error(w, "GET or POST", 405)
	}
}

func handleMigrate(w http.ResponseWriter, r *http.Request, e Engine) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	migrated, errs := e.MigrateAll()
	writeJSON(w, 200, map[string]any{
		"migrated":      migrated,
		"errors":        errs,
		"target_schema": int(storage.CurrentSchemaVersion),
	})
}

// handleCDC streams CDC events as JSON Lines until the client disconnects or
// the optional ?duration_seconds= elapses.  Filters on ?prefix=  (default: all).
func handleCDC(w http.ResponseWriter, r *http.Request, e Engine) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", 405)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	durSec, _ := strconv.Atoi(r.URL.Query().Get("duration_seconds"))
	bufferSize, _ := strconv.Atoi(r.URL.Query().Get("buffer"))
	if bufferSize <= 0 {
		bufferSize = 1024
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(200)

	ch, cancel := e.Subscribe(bufferSize, prefix)
	defer cancel()

	var deadline <-chan time.Time
	if durSec > 0 {
		t := time.NewTimer(time.Duration(durSec) * time.Second)
		defer t.Stop()
		deadline = t.C
	}
	enc := json.NewEncoder(w)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return // subscription evicted (slow consumer)
			}
			if err := enc.Encode(ev); err != nil {
				return
			}
			flusher.Flush()
		case <-deadline:
			return
		case <-r.Context().Done():
			return
		}
	}
}

// handleBackup handles POST /admin/backup.
//
// Request body (JSON):
//
//	{ "type": "full"|"incremental", "dest_dir": "/path/to/backup",
//	  "base_dir": "/path/to/previous/backup" }   // base_dir required for incremental
//
// Returns the BackupManifest JSON on success.
func handleBackup(w http.ResponseWriter, r *http.Request, be BackupEngine) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}

	var req struct {
		Type    string `json:"type"`     // "full" or "incremental"
		DestDir string `json:"dest_dir"` // local destination directory
		BaseDir string `json:"base_dir"` // required for incremental
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), 400)
		return
	}
	if req.DestDir == "" {
		http.Error(w, "dest_dir is required", 400)
		return
	}

	start := time.Now()
	switch req.Type {
	case "full", "":
		manifest, err := be.FullBackup(req.DestDir)
		if err != nil {
			http.Error(w, fmt.Sprintf("backup failed: %v", err), 500)
			return
		}
		writeJSON(w, 200, map[string]any{
			"status":      "ok",
			"type":        "full",
			"backup_id":   manifest.BackupID,
			"dest_dir":    req.DestDir,
			"num_disks":   manifest.NumDisks,
			"duration_ms": time.Since(start).Milliseconds(),
			"manifest":    manifest,
		})

	case "incremental":
		if req.BaseDir == "" {
			http.Error(w, "base_dir is required for incremental backup", 400)
			return
		}
		baseManifest, err := storage.ReadManifest(req.BaseDir)
		if err != nil {
			http.Error(w, fmt.Sprintf("read base manifest from %s: %v", req.BaseDir, err), 400)
			return
		}
		manifest, err := be.IncrementalBackup(req.DestDir, baseManifest)
		if err != nil {
			http.Error(w, fmt.Sprintf("incremental backup failed: %v", err), 500)
			return
		}
		writeJSON(w, 200, map[string]any{
			"status":      "ok",
			"type":        "incremental",
			"backup_id":   manifest.BackupID,
			"dest_dir":    req.DestDir,
			"base_dir":    req.BaseDir,
			"num_disks":   manifest.NumDisks,
			"duration_ms": time.Since(start).Milliseconds(),
			"manifest":    manifest,
		})

	default:
		http.Error(w, fmt.Sprintf("unknown backup type %q — use full or incremental", req.Type), 400)
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(body); err != nil {
		fmt.Fprintf(w, "encode error: %v\n", err)
	}
}
