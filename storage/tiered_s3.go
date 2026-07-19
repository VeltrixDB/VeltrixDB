package storage

// tiered_s3.go — Cloud cold-tier helpers.
//
// Production S3 and GCS implementations already live in backup_cloud.go:
//   NewS3ColdTier(cfg CloudBackupConfig) *S3ColdTier
//   NewGCSColdTier(cfg CloudBackupConfig) *GCSColdTier
//
// This file adds:
//   1. NopColdTier  — in-memory ColdTier for unit tests and dev environments.
//   2. NewS3ColdTierSimple — convenience constructor using only the fields
//      needed for tiered storage (bucket, region, prefix), rather than the
//      full CloudBackupConfig used by the backup subsystem.

import (
	"fmt"
	"sync"
)

// ── NopColdTier ───────────────────────────────────────────────────────────────

// NopColdTier is an in-memory ColdTier for unit tests.
// All operations are goroutine-safe and succeed without I/O.
type NopColdTier struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewNopColdTier() *NopColdTier {
	return &NopColdTier{data: make(map[string][]byte)}
}

func (n *NopColdTier) Put(handle string, value []byte) error {
	cp := make([]byte, len(value))
	copy(cp, value)
	n.mu.Lock()
	n.data[handle] = cp
	n.mu.Unlock()
	return nil
}

func (n *NopColdTier) Get(handle string) ([]byte, error) {
	n.mu.RLock()
	v, ok := n.data[handle]
	n.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("nop-cold-tier: handle not found: %s", handle)
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (n *NopColdTier) Delete(handle string) error {
	n.mu.Lock()
	delete(n.data, handle)
	n.mu.Unlock()
	return nil
}

func (n *NopColdTier) Name() string { return "nop" }

// Len returns the number of handles stored (for test assertions).
func (n *NopColdTier) Len() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.data)
}

// ── NewS3ColdTierSimple ───────────────────────────────────────────────────────

// NewS3ColdTierSimple creates an S3ColdTier using only the bucket, region, and
// prefix required by the tiering subsystem.  The full CloudBackupConfig
// (credentials, retry policy, etc.) is populated from the standard AWS
// credential chain and sane defaults.
func NewS3ColdTierSimple(bucket, region, prefix string) *S3ColdTier {
	return NewS3ColdTier(CloudBackupConfig{
		Provider: ProviderS3,
		Bucket:   bucket,
		Region:   region,
		Prefix:   prefix,
	})
}

// ── NewGCSColdTierSimple ──────────────────────────────────────────────────────

// NewGCSColdTierSimple creates a GCSColdTier from bucket and prefix.
func NewGCSColdTierSimple(bucket, prefix string) *GCSColdTier {
	return NewGCSColdTier(CloudBackupConfig{
		Provider: ProviderGCS,
		Bucket:   bucket,
		Prefix:   prefix,
	})
}

// ── compile-time interface checks ────────────────────────────────────────────

var _ ColdTier = (*NopColdTier)(nil)
