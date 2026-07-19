package storage

import (
	"fmt"
	"strings"
	"testing"
)

// extractAge is a simple index-rule extractor for tests: reads the "age" field
// from a value formatted as "age=<N>" and returns []string{"<N>"}.
func extractAge(value []byte) []string {
	s := string(value)
	prefix := "age="
	idx := strings.Index(s, prefix)
	if idx < 0 {
		return nil
	}
	age := s[idx+len(prefix):]
	// Trim any trailing characters.
	for i, c := range age {
		if c == ' ' || c == ',' || c == ';' {
			age = age[:i]
			break
		}
	}
	if age == "" {
		return nil
	}
	return []string{age}
}

// TestSecondaryIndex_Register: RegisterIndexRule("age", extractAge); Put key with
// age field; LookupBySecondary("age", "30") returns the key.
func TestSecondaryIndex_Register(t *testing.T) {
	t.Parallel()

	// Use a unique rule name to avoid collisions with parallel tests.
	ruleName := "age-reg-test"
	RegisterIndexRule(IndexRule{Name: ruleName, Extract: extractAge})

	se := newTestEngine(t)
	t.Cleanup(func() {
		// Remove the rule so it doesn't bleed into other tests.
		globalIndexRules.mu.Lock()
		for i, r := range globalIndexRules.rs {
			if r.Name == ruleName {
				globalIndexRules.rs = append(globalIndexRules.rs[:i], globalIndexRules.rs[i+1:]...)
				break
			}
		}
		globalIndexRules.mu.Unlock()
	})

	key := "user/42"
	value := []byte("name=Alice,age=30")
	if err := se.Put(key, value, -1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Apply secondary indexes manually (engine.Put calls this internally).
	se.applySecondaryIndexes(key, nil, value)

	results := se.LookupBySecondary(ruleName, "30")
	found := false
	for _, r := range results {
		if r == key {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected key %q in LookupBySecondary results, got: %v", key, results)
	}
}

// TestSecondaryIndex_MultipleValues: Multiple keys with same secondary value;
// all returned by lookup.
func TestSecondaryIndex_MultipleValues(t *testing.T) {
	t.Parallel()

	ruleName := "age-multi-test"
	RegisterIndexRule(IndexRule{Name: ruleName, Extract: extractAge})

	se := newTestEngine(t)
	t.Cleanup(func() {
		globalIndexRules.mu.Lock()
		for i, r := range globalIndexRules.rs {
			if r.Name == ruleName {
				globalIndexRules.rs = append(globalIndexRules.rs[:i], globalIndexRules.rs[i+1:]...)
				break
			}
		}
		globalIndexRules.mu.Unlock()
	})

	keys := []string{"user/1", "user/2", "user/3"}
	for _, k := range keys {
		v := []byte("age=25")
		if err := se.Put(k, v, -1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		se.applySecondaryIndexes(k, nil, v)
	}

	results := se.LookupBySecondary(ruleName, "25")
	if len(results) < len(keys) {
		t.Fatalf("expected at least %d results for age=25, got %d: %v", len(keys), len(results), results)
	}

	resultSet := make(map[string]bool, len(results))
	for _, r := range results {
		resultSet[r] = true
	}
	for _, k := range keys {
		if !resultSet[k] {
			t.Errorf("missing key %q in secondary index results", k)
		}
	}
}

// TestSecondaryIndex_Update: Change secondary field; old value no longer indexed;
// new value indexed.
func TestSecondaryIndex_Update(t *testing.T) {
	t.Parallel()

	ruleName := "age-update-test"
	RegisterIndexRule(IndexRule{Name: ruleName, Extract: extractAge})

	se := newTestEngine(t)
	t.Cleanup(func() {
		globalIndexRules.mu.Lock()
		for i, r := range globalIndexRules.rs {
			if r.Name == ruleName {
				globalIndexRules.rs = append(globalIndexRules.rs[:i], globalIndexRules.rs[i+1:]...)
				break
			}
		}
		globalIndexRules.mu.Unlock()
	})

	key := "user/update"
	oldValue := []byte("age=20")
	newValue := []byte("age=21")

	if err := se.Put(key, oldValue, -1); err != nil {
		t.Fatalf("Put initial: %v", err)
	}
	se.applySecondaryIndexes(key, nil, oldValue)

	// Update the key with a new age.
	if err := se.Put(key, newValue, -1); err != nil {
		t.Fatalf("Put update: %v", err)
	}
	se.applySecondaryIndexes(key, oldValue, newValue)

	// Old age should no longer be indexed.
	oldResults := se.LookupBySecondary(ruleName, "20")
	for _, r := range oldResults {
		if r == key {
			t.Errorf("key %q should not appear in age=20 index after update", key)
		}
	}

	// New age should be indexed.
	newResults := se.LookupBySecondary(ruleName, "21")
	found := false
	for _, r := range newResults {
		if r == key {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected key %q in age=21 index after update, got: %v", key, newResults)
	}
}

// TestSecondaryIndex_Delete: Delete primary key; removed from secondary index.
func TestSecondaryIndex_Delete(t *testing.T) {
	t.Parallel()

	ruleName := "age-delete-test"
	RegisterIndexRule(IndexRule{Name: ruleName, Extract: extractAge})

	se := newTestEngine(t)
	t.Cleanup(func() {
		globalIndexRules.mu.Lock()
		for i, r := range globalIndexRules.rs {
			if r.Name == ruleName {
				globalIndexRules.rs = append(globalIndexRules.rs[:i], globalIndexRules.rs[i+1:]...)
				break
			}
		}
		globalIndexRules.mu.Unlock()
	})

	key := "user/delete-me"
	value := []byte("age=35")
	if err := se.Put(key, value, -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	se.applySecondaryIndexes(key, nil, value)

	// Verify it is indexed.
	before := se.LookupBySecondary(ruleName, "35")
	found := false
	for _, r := range before {
		if r == key {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("key not in secondary index before delete")
	}

	// Delete primary: diff-apply with nil newValue removes all secondary entries.
	if err := se.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	se.applySecondaryIndexes(key, value, nil)

	after := se.LookupBySecondary(ruleName, "35")
	for _, r := range after {
		if r == key {
			t.Errorf("key %q still in secondary index after delete", key)
		}
	}
}

// TestSecondaryIndex_PrefixEncoding: Verify that secondary index keys use the
// "@idx/<rule>/<value>/<primary>" format by scanning the raw shards.
func TestSecondaryIndex_PrefixEncoding(t *testing.T) {
	t.Parallel()

	ruleName := "age-encoding-test"
	RegisterIndexRule(IndexRule{Name: ruleName, Extract: extractAge})

	se := newTestEngine(t)
	t.Cleanup(func() {
		globalIndexRules.mu.Lock()
		for i, r := range globalIndexRules.rs {
			if r.Name == ruleName {
				globalIndexRules.rs = append(globalIndexRules.rs[:i], globalIndexRules.rs[i+1:]...)
				break
			}
		}
		globalIndexRules.mu.Unlock()
	})

	primary := "user/encoding-check"
	value := []byte("age=99")
	if err := se.Put(primary, value, -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	se.applySecondaryIndexes(primary, nil, value)

	// Expected key format: "@idx/age-encoding-test/99/user/encoding-check"
	expectedPrefix := fmt.Sprintf("%s%s/%s/", secondaryIndexPrefix, ruleName, "99")

	found := false
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k := range shard.entries {
			if strings.HasPrefix(k, expectedPrefix) {
				found = true
			}
		}
		shard.mu.RUnlock()
	}
	if !found {
		t.Fatalf("expected secondary index key with prefix %q in shards", expectedPrefix)
	}
}
