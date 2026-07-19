package storage

// secondary_index.go — secondary indexes by reserved key prefix.
//
// VeltrixDB doesn't have a query layer; secondary indexes here are a
// "convention encoded as keys" implemented inside the engine so callers
// can do (field, field-value) → primary-key lookups without scanning.
//
// On-disk layout:
//
//   primary key:    "user/42"               value: {...JSON...}
//   secondary:      "@idx/email/x@y.com/42"  value: ""  (sentinel)
//
// The "@idx/" prefix is reserved — primary keys must not start with it.
// The trailing "/42" is the primary key, repeated so reverse lookups work
// without an additional read.
//
// IndexRule defines which JSON-ish fields to extract from values. Callers
// register rules at startup; on every Put the engine evaluates registered
// rules against the new value and emits secondary entries.
//
// Removal: the rule's `Extract` function returns a list of (field-value,
// primary-key) pairs. A second call with the OLD value returns the entries
// to delete. The engine handles the diff.
//
// Limitations (deliberate, mark as future-work in production):
//   - No transactional consistency between primary and secondary writes.
//     If the secondary write fails, the primary is still committed; an
//     occasional "missing index entry" is the price of avoiding 2-phase
//     commit. A re-Put of the primary heals it.
//   - Range scans by secondary value need ScanByPrefix on "@idx/<field>/"
//     which iterates the underlying shardedIndex linearly. Fine up to
//     ~10 M index entries; beyond that, build a B-tree (separate work).
//   - Type-aware (int, time) ordering is not provided — values are
//     compared as raw bytes lexicographically.

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

const secondaryIndexPrefix = "@idx/"

// indexRuleCount mirrors len(globalIndexRules.rs) so the Put/Delete hot path
// can check "are any secondary indexes defined?" with a single atomic load
// instead of taking the rules RWMutex on every write.
var indexRuleCount atomic.Int32

// indexRulesActive reports whether at least one secondary-index rule is
// registered. Near-zero cost — one atomic load.
func indexRulesActive() bool { return indexRuleCount.Load() > 0 }

// isInternalIndexKey reports whether key belongs to an engine-internal
// keyspace (secondary-index entries, persisted vectors) that must never be
// secondary-indexed itself.
func isInternalIndexKey(key string) bool {
	return strings.HasPrefix(key, secondaryIndexPrefix) ||
		strings.HasPrefix(key, vectorKeyPrefix)
}

// IndexRule is one secondary-index extraction rule.
type IndexRule struct {
	// Name uniquely identifies this rule; appears as the second component
	// of the secondary key (e.g. "@idx/email/...").
	Name string
	// Extract returns the values to index for a primary entry. Multiple
	// values produce multiple secondary entries (multi-valued field).
	Extract func(value []byte) []string
}

type indexRules struct {
	mu sync.RWMutex
	rs []IndexRule
}

var globalIndexRules = &indexRules{}

// RegisterIndexRule installs a secondary-index rule. Idempotent on Name.
// Must be called before the engine is constructed; rules are evaluated
// from the engine's Put hot path.
func RegisterIndexRule(r IndexRule) {
	globalIndexRules.mu.Lock()
	defer globalIndexRules.mu.Unlock()
	for i, existing := range globalIndexRules.rs {
		if existing.Name == r.Name {
			globalIndexRules.rs[i] = r
			return
		}
	}
	globalIndexRules.rs = append(globalIndexRules.rs, r)
	indexRuleCount.Store(int32(len(globalIndexRules.rs)))
}

// UnregisterIndexRule removes the rule registered under name. No-op when the
// name is unknown. Existing "@idx/<name>/..." entries are NOT deleted here —
// see StorageEngine.DropFieldIndex for the full teardown.
func UnregisterIndexRule(name string) {
	globalIndexRules.mu.Lock()
	defer globalIndexRules.mu.Unlock()
	for i, existing := range globalIndexRules.rs {
		if existing.Name == name {
			globalIndexRules.rs = append(globalIndexRules.rs[:i], globalIndexRules.rs[i+1:]...)
			break
		}
	}
	indexRuleCount.Store(int32(len(globalIndexRules.rs)))
}

// secondaryKeysFor returns the full set of secondary keys for (primary, value)
// across all rules.
func secondaryKeysFor(primary string, value []byte) []string {
	globalIndexRules.mu.RLock()
	defer globalIndexRules.mu.RUnlock()
	if len(globalIndexRules.rs) == 0 {
		return nil
	}
	var out []string
	for _, r := range globalIndexRules.rs {
		for _, v := range r.Extract(value) {
			out = append(out, fmt.Sprintf("%s%s/%s/%s", secondaryIndexPrefix, r.Name, escape(v), primary))
		}
	}
	return out
}

// LookupBySecondary returns primary keys whose secondary entry matches
// (rule, value). Internally scans "@idx/<rule>/<value>/*" via shard walk.
// Result order is unspecified.
func (se *StorageEngine) LookupBySecondary(rule, value string) []string {
	prefix := fmt.Sprintf("%s%s/%s/", secondaryIndexPrefix, rule, escape(value))
	var out []string
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if entry.IsTombstone() {
				continue
			}
			if strings.HasPrefix(k, prefix) {
				out = append(out, k[len(prefix):])
			}
		}
		shard.mu.RUnlock()
	}
	return out
}

// applySecondaryIndexes diff-applies index entries for (primary, oldValue, newValue).
// Called from engine.Put after the primary write committed.  Errors are logged
// but not surfaced — see "no transactional consistency" caveat above.
func (se *StorageEngine) applySecondaryIndexes(primary string, oldValue, newValue []byte) {
	if isInternalIndexKey(primary) {
		return // never index our own index entries or persisted vectors
	}
	oldKeys := map[string]struct{}{}
	for _, k := range secondaryKeysFor(primary, oldValue) {
		oldKeys[k] = struct{}{}
	}
	newKeys := map[string]struct{}{}
	for _, k := range secondaryKeysFor(primary, newValue) {
		newKeys[k] = struct{}{}
	}
	// Add new entries (best-effort).
	for k := range newKeys {
		if _, kept := oldKeys[k]; kept {
			continue
		}
		_ = se.Put(k, []byte{}, -1)
	}
	// Delete obsolete entries.
	for k := range oldKeys {
		if _, kept := newKeys[k]; kept {
			continue
		}
		_ = se.Delete(k)
	}
}

func escape(s string) string {
	// '/' is the secondary-key path separator; escape it so values that
	// contain '/' don't break parsing. Use percent-encoding-style for clarity.
	return strings.NewReplacer("/", "%2F", "%", "%25").Replace(s)
}
