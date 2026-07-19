package storage

// schema.go — schema-version migrations applied lazily on read.
//
// Every IndexEntry carries a 1-byte SchemaVersion. New entries are written at
// the engine's CurrentSchemaVersion. When an old binary reads an entry whose
// SchemaVersion is below the current, MigrateOnRead is invoked with the raw
// value bytes so the application layer can transform them in flight (e.g.
// rename a JSON field, change a struct serialization, add a default).
//
// Why migrate-on-read instead of a stop-the-world batch migrate?
//   - Zero downtime: live traffic is never blocked.
//   - Cold keys never get migrated; you don't pay for keys nobody reads.
//   - Each replica migrates independently — no coordination required.
//
// The cost: every read goes through one extra version check. The migrator
// table is small (one entry per version delta) so the dispatch is a fast
// switch on the version byte.
//
// Registering migrations:
//
//   storage.RegisterMigration(2, func(value []byte) ([]byte, error) {
//       // upgrade from v1 to v2
//       return upgradeV1toV2(value), nil
//   })
//
// Migrations chain — if a v1 record is read on a binary with v3 registered,
// MigrateOnRead applies v1→v2 then v2→v3 sequentially. Operators MUST register
// migrations in monotonically-increasing order with no gaps.
//
// On-disk impact: migrate-on-read does NOT rewrite the record back to disk by
// default — the migrated bytes are returned to the caller, but the next read
// will run the migration again. To flush migrations to disk, run a full
// engine.MigrateAll() admin call which iterates every key and rewrites them
// at CurrentSchemaVersion.

import (
	"fmt"
	"sync"
)

// CurrentSchemaVersion is the version number written into every new IndexEntry
// from this binary. Bump when introducing a new on-disk value format. Always
// register a migration from N to N+1 BEFORE bumping this constant.
const CurrentSchemaVersion uint8 = 1

// MigrationFunc upgrades a value blob from version V-1 to version V.
type MigrationFunc func(value []byte) ([]byte, error)

var (
	migrationsMu sync.RWMutex
	migrations   = map[uint8]MigrationFunc{}
)

// RegisterMigration registers fn as the upgrade from (toVersion-1) to toVersion.
// Idempotent: re-registering the same version with the same fn is fine; with a
// different fn panics (programmer error).
func RegisterMigration(toVersion uint8, fn MigrationFunc) {
	if toVersion == 0 {
		panic("RegisterMigration: version 0 is reserved")
	}
	if fn == nil {
		panic("RegisterMigration: fn is nil")
	}
	migrationsMu.Lock()
	defer migrationsMu.Unlock()
	if existing, ok := migrations[toVersion]; ok && fmt.Sprintf("%p", existing) != fmt.Sprintf("%p", fn) {
		panic(fmt.Sprintf("RegisterMigration: version %d already registered with a different function", toVersion))
	}
	migrations[toVersion] = fn
}

// MigrateOnRead applies all pending migrations from fromVersion+1 up to
// CurrentSchemaVersion. Returns the migrated bytes and the version they now
// represent (always CurrentSchemaVersion when no error). If a migration is
// missing for an intermediate version, returns an error and the original
// bytes — callers should treat this as data corruption / forward-incompat.
func MigrateOnRead(fromVersion uint8, value []byte) ([]byte, uint8, error) {
	if fromVersion >= CurrentSchemaVersion {
		return value, fromVersion, nil
	}
	migrationsMu.RLock()
	defer migrationsMu.RUnlock()
	cur := value
	for v := fromVersion + 1; v <= CurrentSchemaVersion; v++ {
		fn, ok := migrations[v]
		if !ok {
			return value, fromVersion, fmt.Errorf(
				"schema: missing migration to version %d (record was at %d)", v, fromVersion)
		}
		out, err := fn(cur)
		if err != nil {
			return value, fromVersion, fmt.Errorf("schema: migration v%d failed: %w", v, err)
		}
		cur = out
	}
	return cur, CurrentSchemaVersion, nil
}

// MigrateAll walks every live key and rewrites it through Put so the stored
// IndexEntry.SchemaVersion is the current one. Use sparingly — this triggers
// a full re-Put which is heavy on WAL+VLog. Designed for offline upgrades or
// when the operator wants to drop support for an old version.
//
// Returns (migratedCount, errorCount).
func (se *StorageEngine) MigrateAll() (int, int) {
	migrated, errs := 0, 0
	for _, key := range se.ScanKeys() {
		entry, _, ok := se.index.get(key)
		if !ok || entry.IsTombstone() || entry.SchemaVersion >= CurrentSchemaVersion {
			continue
		}
		val, err := se.Get(key) // already runs MigrateOnRead transparently
		if err != nil {
			errs++
			continue
		}
		ttl := se.GetTTLForKey(key)
		if err := se.Put(key, val, ttl); err != nil {
			errs++
			continue
		}
		migrated++
	}
	return migrated, errs
}
