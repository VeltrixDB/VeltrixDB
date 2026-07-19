package storage

// field_index.go — named, persistent field-extraction secondary indexes.
//
// secondary_index.go provides the mechanism (IndexRule + "@idx/..." key
// convention); this file provides the operator-facing policy layer used by
// the IDXCREATE / IDXDROP / IDXQUERY wire commands:
//
//   - CreateFieldIndex(name, field): registers an IndexRule whose extractor
//     pulls <field> out of each value (JSON object or "k=v" pair syntax),
//     backfills index entries for existing live keys, and persists the
//     definition to <dataDir>/index_defs.json.
//   - Definitions are re-loaded (rules re-registered) by NewStorageEngine at
//     startup. The "@idx/..." entries themselves are ordinary durable keys
//     (WAL + VLog), so no index rebuild is needed after a restart — only the
//     rule registration so future writes keep maintaining the index.
//   - DropFieldIndex(name): unregisters the rule, deletes every
//     "@idx/<name>/..." entry, and removes the persisted definition.
//
// Field extraction understands two value shapes:
//
//   1. JSON objects:      {"email":"x@y.com","age":30}   field "age" → "30"
//   2. Delimited pairs:   name=Alice,age=30              field "age" → "30"
//
// Values that match neither shape simply produce no index entry.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// fieldIndexDefsFile is the JSON meta file (in the first data dir) that
// persists index definitions across restarts.
const fieldIndexDefsFile = "index_defs.json"

// FieldIndexDef is one persisted secondary-index definition.
type FieldIndexDef struct {
	Name  string `json:"name"`
	Field string `json:"field"`
}

func (se *StorageEngine) fieldIndexDefsPath() string {
	return filepath.Join(se.GetDataDirs()[0], fieldIndexDefsFile)
}

// extractFieldValue pulls the named field out of value. It first tries to
// parse value as a JSON object; failing that it scans for "<field>=<v>"
// pairs delimited by ',', ';', or space (the same shape the secondary-index
// tests use). Returns ("", false) when the field is absent.
func extractFieldValue(value []byte, field string) (string, bool) {
	if len(value) == 0 || field == "" {
		return "", false
	}
	// JSON object shape.
	trimmed := strings.TrimSpace(string(value))
	if strings.HasPrefix(trimmed, "{") {
		var obj map[string]any
		if err := json.Unmarshal([]byte(trimmed), &obj); err == nil {
			v, ok := obj[field]
			if !ok {
				return "", false
			}
			switch t := v.(type) {
			case string:
				return t, true
			case float64:
				return strconv.FormatFloat(t, 'f', -1, 64), true
			case bool:
				return strconv.FormatBool(t), true
			default:
				return "", false // nested objects/arrays/null are not indexable
			}
		}
	}
	// "k=v" pair shape: field=value delimited by ',', ';', or space.
	s := string(value)
	for start := 0; start < len(s); {
		idx := strings.Index(s[start:], field+"=")
		if idx < 0 {
			return "", false
		}
		pos := start + idx
		// The match must begin at a pair boundary, not mid-word ("age=" must
		// not match "stage=").
		if pos > 0 {
			prev := s[pos-1]
			if prev != ',' && prev != ';' && prev != ' ' {
				start = pos + len(field) + 1
				continue
			}
		}
		v := s[pos+len(field)+1:]
		if end := strings.IndexAny(v, ",; "); end >= 0 {
			v = v[:end]
		}
		return v, v != ""
	}
	return "", false
}

// makeFieldExtractor adapts extractFieldValue to the IndexRule.Extract shape.
func makeFieldExtractor(field string) func(value []byte) []string {
	return func(value []byte) []string {
		v, ok := extractFieldValue(value, field)
		if !ok {
			return nil
		}
		return []string{v}
	}
}

// CreateFieldIndex defines a named secondary index on field, persists the
// definition, and synchronously backfills index entries for all existing
// live keys. Idempotent when called again with the same (name, field);
// returns an error when name is already bound to a different field.
func (se *StorageEngine) CreateFieldIndex(name, field string) error {
	if name == "" || field == "" {
		return errors.New("index name and field must be non-empty")
	}
	if strings.ContainsAny(name, "/ \x00") || strings.ContainsAny(field, " \x00") {
		return fmt.Errorf("invalid index name %q or field %q", name, field)
	}

	se.fieldIdxMu.Lock()
	for _, d := range se.fieldIdxDefs {
		if d.Name == name {
			se.fieldIdxMu.Unlock()
			if d.Field == field {
				return nil // idempotent re-create
			}
			return fmt.Errorf("index %q already exists on field %q", name, d.Field)
		}
	}
	se.fieldIdxDefs = append(se.fieldIdxDefs, FieldIndexDef{Name: name, Field: field})
	defs := append([]FieldIndexDef(nil), se.fieldIdxDefs...)
	se.fieldIdxMu.Unlock()

	RegisterIndexRule(IndexRule{Name: name, Extract: makeFieldExtractor(field)})
	if err := se.saveFieldIndexDefs(defs); err != nil {
		return fmt.Errorf("persist index defs: %w", err)
	}
	se.backfillFieldIndex()
	return nil
}

// DropFieldIndex removes the named index: rule, persisted definition, and
// every "@idx/<name>/..." entry. No-op error when the index does not exist.
func (se *StorageEngine) DropFieldIndex(name string) error {
	se.fieldIdxMu.Lock()
	found := false
	for i, d := range se.fieldIdxDefs {
		if d.Name == name {
			se.fieldIdxDefs = append(se.fieldIdxDefs[:i], se.fieldIdxDefs[i+1:]...)
			found = true
			break
		}
	}
	defs := append([]FieldIndexDef(nil), se.fieldIdxDefs...)
	se.fieldIdxMu.Unlock()
	if !found {
		return fmt.Errorf("index %q not found", name)
	}

	UnregisterIndexRule(name)
	if err := se.saveFieldIndexDefs(defs); err != nil {
		return fmt.Errorf("persist index defs: %w", err)
	}

	// Delete all persisted entries for this index.
	prefix := secondaryIndexPrefix + name + "/"
	for _, k := range se.scanKeysWithPrefix(prefix) {
		_ = se.Delete(k)
	}
	return nil
}

// ListFieldIndexes returns a snapshot of the persisted index definitions.
func (se *StorageEngine) ListFieldIndexes() []FieldIndexDef {
	se.fieldIdxMu.Lock()
	defer se.fieldIdxMu.Unlock()
	return append([]FieldIndexDef(nil), se.fieldIdxDefs...)
}

// findFieldIndexFor returns the name of an index defined on field, if any.
// Used by QueryNS to accelerate equality predicates.
func (se *StorageEngine) findFieldIndexFor(field string) (string, bool) {
	se.fieldIdxMu.Lock()
	defer se.fieldIdxMu.Unlock()
	for _, d := range se.fieldIdxDefs {
		if d.Field == field {
			return d.Name, true
		}
	}
	return "", false
}

// saveFieldIndexDefs writes defs to the meta file via temp-file + rename so a
// crash mid-write cannot corrupt the previous definitions.
func (se *StorageEngine) saveFieldIndexDefs(defs []FieldIndexDef) error {
	data, err := json.MarshalIndent(defs, "", "  ")
	if err != nil {
		return err
	}
	path := se.fieldIndexDefsPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadFieldIndexDefs reads the meta file (if present) and re-registers one
// IndexRule per definition. Called once from NewStorageEngine.
func (se *StorageEngine) loadFieldIndexDefs() error {
	data, err := os.ReadFile(se.fieldIndexDefsPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var defs []FieldIndexDef
	if err := json.Unmarshal(data, &defs); err != nil {
		return fmt.Errorf("parse %s: %w", fieldIndexDefsFile, err)
	}
	se.fieldIdxMu.Lock()
	se.fieldIdxDefs = defs
	se.fieldIdxMu.Unlock()
	for _, d := range defs {
		RegisterIndexRule(IndexRule{Name: d.Name, Extract: makeFieldExtractor(d.Field)})
	}
	return nil
}

// backfillFieldIndex re-applies all registered index rules to every existing
// live user key. Runs synchronously on IDXCREATE — one Get + diff-apply per
// key, so cost is O(live keys). Entries already present are idempotently
// overwritten.
func (se *StorageEngine) backfillFieldIndex() {
	for _, k := range se.ScanKeys() {
		if isInternalIndexKey(k) {
			continue
		}
		val, err := se.Get(k)
		if err != nil {
			continue
		}
		se.applySecondaryIndexes(k, nil, val)
	}
}

// scanKeysWithPrefix returns all live internal keys with the given prefix.
func (se *StorageEngine) scanKeysWithPrefix(prefix string) []string {
	nowUs := time.Now().UnixMicro()
	var keys []string
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if !entry.IsTombstone() && !entry.IsExpired(nowUs) && strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		shard.mu.RUnlock()
	}
	return keys
}
