package storage

// quotas.go — per-namespace token-bucket rate limits and key-count quotas.
//
// Two enforcement points:
//   1. Rate limit: tokens-per-second across PUT, DEL, CAS, INCR, SETNX. A burst
//      bucket allows brief spikes; sustained traffic is capped at the rate.
//      Reads (GET) are not rate-limited — read amplification is the cache's
//      responsibility, not the quota system's.
//   2. Storage quota: max live key count per namespace. Crossing the quota
//      causes new writes to that namespace to fail with ErrQuotaExceeded;
//      reads, deletes, and overwrites of existing keys still pass (quota is
//      key-count, not byte-count, intentionally — we want delete-to-recover
//      to work even when over quota).
//
// Limits are stored in an atomic-pointer map so updates are lock-free reads.
// SetLimit replaces the limit for a namespace; tokens carry over across reset.
//
// Per-tenant accounting via namespace prefix: customers map to namespaces in
// the application layer (e.g. "tenant_42"). Without namespaces, all writes go
// to the empty-string namespace and share one bucket — which is fine for
// single-tenant deployments.

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrQuotaExceeded is returned by quota-checked Put/Delete/CAS/INCR/SETNX.
var ErrQuotaExceeded = errors.New("quota exceeded")

// ErrRateLimited is returned when a namespace is over its rate limit.
var ErrRateLimited = errors.New("rate limit exceeded")

// QuotaLimit specifies the limits for one namespace.
type QuotaLimit struct {
	WritesPerSec int   // 0 = unlimited
	BurstWrites  int   // 0 = use WritesPerSec as burst
	MaxKeys      int64 // 0 = unlimited
}

// QuotaManager tracks live key counts per namespace and per-namespace token
// buckets. All state is in-memory; persisted across restarts only for key
// counts (rebuilt by the WAL replay walking the index).
type QuotaManager struct {
	mu     sync.RWMutex
	limits map[string]*QuotaLimit  // ns → limit
	state  map[string]*quotaState  // ns → live state
}

type quotaState struct {
	keyCount atomic.Int64

	bktMu     sync.Mutex
	tokens    float64
	lastFill  time.Time
}

// NewQuotaManager creates an empty manager.
func NewQuotaManager() *QuotaManager {
	return &QuotaManager{
		limits: map[string]*QuotaLimit{},
		state:  map[string]*quotaState{},
	}
}

// SetLimit installs or updates the limit for ns. Pass an empty namespace name
// for the default (un-namespaced) bucket.
func (q *QuotaManager) SetLimit(ns string, lim QuotaLimit) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cp := lim
	if cp.BurstWrites == 0 && cp.WritesPerSec > 0 {
		cp.BurstWrites = cp.WritesPerSec
	}
	q.limits[ns] = &cp
	if _, ok := q.state[ns]; !ok {
		q.state[ns] = &quotaState{tokens: float64(cp.BurstWrites), lastFill: time.Now()}
	}
}

// GetLimit returns the configured limit for ns, or zero-value when none set.
func (q *QuotaManager) GetLimit(ns string) QuotaLimit {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if l, ok := q.limits[ns]; ok {
		return *l
	}
	return QuotaLimit{}
}

// CheckWrite returns nil when a write to ns is allowed, ErrRateLimited or
// ErrQuotaExceeded otherwise.  isNewKey distinguishes new keys (which count
// against MaxKeys) from overwrites (which do not).  Caller is expected to
// invoke this BEFORE Put.
func (q *QuotaManager) CheckWrite(ns string, isNewKey bool) error {
	q.mu.RLock()
	lim := q.limits[ns]
	st := q.state[ns]
	q.mu.RUnlock()

	if lim == nil || st == nil {
		return nil // no limit configured
	}

	if lim.MaxKeys > 0 && isNewKey {
		if st.keyCount.Load() >= lim.MaxKeys {
			return ErrQuotaExceeded
		}
	}

	if lim.WritesPerSec > 0 {
		st.bktMu.Lock()
		now := time.Now()
		elapsed := now.Sub(st.lastFill).Seconds()
		st.tokens += elapsed * float64(lim.WritesPerSec)
		st.lastFill = now
		burst := float64(lim.BurstWrites)
		if burst <= 0 {
			burst = float64(lim.WritesPerSec)
		}
		if st.tokens > burst {
			st.tokens = burst
		}
		if st.tokens < 1 {
			st.bktMu.Unlock()
			return ErrRateLimited
		}
		st.tokens--
		st.bktMu.Unlock()
	}
	return nil
}

// IncKeyCount adds delta to the live key count for ns. Engine code calls this
// from Put (after a successful write of a new key) and Delete (with delta=-1).
// No-op when no limit is set.
func (q *QuotaManager) IncKeyCount(ns string, delta int64) {
	q.mu.RLock()
	st := q.state[ns]
	q.mu.RUnlock()
	if st == nil {
		return
	}
	st.keyCount.Add(delta)
}

// Snapshot returns a per-namespace summary for admin / metrics. The returned
// map is a copy — safe for concurrent reads.
type QuotaSnapshot struct {
	Namespace    string
	WritesPerSec int
	BurstWrites  int
	MaxKeys      int64
	KeyCount     int64
	TokensLeft   float64
}

func (q *QuotaManager) Snapshot() []QuotaSnapshot {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]QuotaSnapshot, 0, len(q.limits))
	for ns, lim := range q.limits {
		st := q.state[ns]
		var tk float64
		var kc int64
		if st != nil {
			st.bktMu.Lock()
			tk = st.tokens
			st.bktMu.Unlock()
			kc = st.keyCount.Load()
		}
		out = append(out, QuotaSnapshot{
			Namespace:    ns,
			WritesPerSec: lim.WritesPerSec,
			BurstWrites:  lim.BurstWrites,
			MaxKeys:      lim.MaxKeys,
			KeyCount:     kc,
			TokensLeft:   tk,
		})
	}
	return out
}

// SetNamespaceLimit is the engine-level helper.
func (se *StorageEngine) SetNamespaceLimit(ns string, lim QuotaLimit) {
	se.quotas.SetLimit(ns, lim)
}

// QuotaStats exposes the snapshot to admin callers.
func (se *StorageEngine) QuotaStats() []QuotaSnapshot {
	return se.quotas.Snapshot()
}
