package storage

// audit.go — append-only audit log for compliance / forensics.
//
// Records every authenticated mutating operation as a single line of JSONL
// (newline-delimited JSON). Designed for compliance frameworks (SOC 2, HIPAA,
// PCI-DSS) that require tamper-evident operation records separate from the
// data plane.
//
// Threat model:
//   - The audit log is NOT a security boundary against an attacker who has
//     compromised the host (they can rewrite the file).  It is the standard
//     defense-in-depth artifact: shipped to a separate aggregator (Loki,
//     Splunk, Elastic) for tamper-evident retention.
//   - Records do NOT contain value payloads (they would defeat encryption-
//     at-rest and balloon the log).  Only operation, key, identity, time,
//     and outcome are logged.
//
// Format (one JSON object per line, fields in stable order):
//   {"ts":"2026-05-10T11:35:55Z","op":"PUT","actor":"user-bob",
//    "key":"orders/4521","ns":"default","status":"ok"}
//
// Async write path: callers enqueue records via Audit.Log() which writes to a
// buffered channel.  A background goroutine drains the channel and appends to
// the file under a mutex; periodic fsync ensures the log is durable within
// the configured window.  When the channel is full, records are DROPPED and
// the audit_log_dropped_total counter is incremented — reflecting that audit
// must never block the data plane.

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// AuditRecord is one operation entry. Keep field set small and stable.
type AuditRecord struct {
	TS     string `json:"ts"`               // RFC 3339 nano UTC
	Op     string `json:"op"`               // PUT, DEL, CAS, INCR, …
	Actor  string `json:"actor,omitempty"`  // authenticated user id; empty when auth disabled
	Key    string `json:"key"`              // user-visible key (with namespace prefix when applicable)
	Ns     string `json:"ns,omitempty"`     // namespace, when distinct
	Status string `json:"status,omitempty"` // ok, err, mismatch, exists
	Err    string `json:"err,omitempty"`    // error message when Status=err
}

// AuditLog is the engine-wide audit logger. Created once via NewAuditLog;
// methods are safe for concurrent use.
type AuditLog struct {
	enabled bool
	path    string
	ch      chan AuditRecord
	dropped *atomic.Uint64

	mu       sync.Mutex
	f        *os.File
	enc      *json.Encoder
	syncEvery time.Duration
	stop     chan struct{}
	done     chan struct{}
}

// NewAuditLog opens (or creates) path and starts the background writer.
// channelDepth controls how many records can be buffered before drops kick in.
// syncEvery is how often the file is fsync'd; default 1 s when ≤ 0.
//
// Returns a no-op AuditLog when path is empty.
func NewAuditLog(path string, channelDepth int, syncEvery time.Duration, dropped *atomic.Uint64) (*AuditLog, error) {
	a := &AuditLog{dropped: dropped}
	if path == "" {
		return a, nil
	}
	if channelDepth <= 0 {
		channelDepth = 8192
	}
	if syncEvery <= 0 {
		syncEvery = 1 * time.Second
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("audit: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, fmt.Errorf("audit: open %s: %w", path, err)
	}
	a.enabled = true
	a.path = path
	a.ch = make(chan AuditRecord, channelDepth)
	a.f = f
	a.enc = json.NewEncoder(f)
	a.syncEvery = syncEvery
	a.stop = make(chan struct{})
	a.done = make(chan struct{})
	go a.run()
	log.Printf("[audit] enabled  path=%s  channel=%d  sync=%s",
		path, channelDepth, syncEvery)
	return a, nil
}

// Log enqueues a record for async write. Never blocks; on a full channel the
// record is dropped and the dropped counter is incremented.
func (a *AuditLog) Log(rec AuditRecord) {
	if a == nil || !a.enabled {
		return
	}
	if rec.TS == "" {
		rec.TS = time.Now().UTC().Format(time.RFC3339Nano)
	}
	select {
	case a.ch <- rec:
	default:
		if a.dropped != nil {
			a.dropped.Add(1)
		}
	}
}

// Close drains pending records and closes the file. Safe to call multiple times.
func (a *AuditLog) Close() error {
	if a == nil || !a.enabled {
		return nil
	}
	a.enabled = false
	close(a.stop)
	<-a.done
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f != nil {
		_ = a.f.Sync()
		err := a.f.Close()
		a.f = nil
		return err
	}
	return nil
}

func (a *AuditLog) run() {
	defer close(a.done)
	tick := time.NewTicker(a.syncEvery)
	defer tick.Stop()
	for {
		select {
		case rec := <-a.ch:
			a.write(rec)
		case <-tick.C:
			a.mu.Lock()
			if a.f != nil {
				_ = a.f.Sync()
			}
			a.mu.Unlock()
		case <-a.stop:
			// Drain remaining buffered records then return.
			for {
				select {
				case rec := <-a.ch:
					a.write(rec)
				default:
					return
				}
			}
		}
	}
}

func (a *AuditLog) write(rec AuditRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil {
		return
	}
	if err := a.enc.Encode(rec); err != nil {
		log.Printf("[audit] encode failed: %v", err)
	}
}
