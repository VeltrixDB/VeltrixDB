package storage

// query.go — minimal field-predicate query over namespace data.
//
// Wire shape (see cmd/server):
//
//	QUERY <namespace> WHERE <field> <op> <value> [LIMIT n]
//
// Each key in the namespace is treated as one record; its value is parsed
// with extractFieldValue (JSON object or "k=v" pair syntax — field_index.go).
// The predicate is evaluated through the predicates.go scan machinery.
//
// Execution strategy:
//   - op "=" AND a secondary index exists on <field> (CreateFieldIndex):
//     LookupBySecondary O(index entries) → filter to the namespace →
//     verify each hit against the live value (the index is best-effort,
//     see secondary_index.go caveats).
//   - otherwise: full namespace scan with the predicate applied per record.
//
// Supported ops: = != > < >= <= contains.  Ordering ops compare numerically
// when both sides parse as float64, else lexicographically as raw bytes.
// = and != are exact string comparisons (matching index-lookup semantics).

import (
	"fmt"
	"strconv"
	"strings"
)

// queryOps is the closed set of operators QUERY accepts.
var queryOps = map[string]bool{
	"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
	"contains": true,
}

// IsQueryOp reports whether op is a supported QUERY operator.
func IsQueryOp(op string) bool { return queryOps[op] }

// compareField evaluates <fieldVal> <op> <operand>.
func compareField(fieldVal, op, operand string) bool {
	switch op {
	case "=":
		return fieldVal == operand
	case "!=":
		return fieldVal != operand
	case "contains":
		return strings.Contains(fieldVal, operand)
	}
	// Ordering ops: numeric when both sides parse, else lexicographic.
	fa, errA := strconv.ParseFloat(fieldVal, 64)
	fb, errB := strconv.ParseFloat(operand, 64)
	var cmp int
	if errA == nil && errB == nil {
		switch {
		case fa < fb:
			cmp = -1
		case fa > fb:
			cmp = 1
		}
	} else {
		cmp = strings.Compare(fieldVal, operand)
	}
	switch op {
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	}
	return false
}

// BuildFieldPredicate returns a PredicateFunc evaluating
// "extract(<field>) <op> <operand>" against each record value.
// Records where the field is absent never match.
func BuildFieldPredicate(field, op, operand string) (PredicateFunc, error) {
	if field == "" {
		return nil, fmt.Errorf("query: empty field")
	}
	if !IsQueryOp(op) {
		return nil, fmt.Errorf("query: unsupported op %q (want = != > < >= <= contains)", op)
	}
	return func(_ string, value []byte) bool {
		fv, ok := extractFieldValue(value, field)
		if !ok {
			return false
		}
		return compareField(fv, op, operand)
	}, nil
}

// QueryNS runs "SELECT * FROM ns WHERE field op operand LIMIT limit".
// Returns matching records with the namespace prefix stripped from keys.
// Result order is unspecified. limit ≤ 0 → unlimited.
func (se *StorageEngine) QueryNS(ns, field, op, operand string, limit int) ([]NSEntry, error) {
	pred, err := BuildFieldPredicate(field, op, operand)
	if err != nil {
		return nil, err
	}
	prefix := ns + nsSep

	// Index-accelerated path: equality on an indexed field.
	if op == "=" {
		if idxName, ok := se.findFieldIndexFor(field); ok {
			var out []NSEntry
			for _, k := range se.LookupBySecondary(idxName, operand) {
				if !strings.HasPrefix(k, prefix) {
					continue // hit from another namespace or the plain keyspace
				}
				val, err := se.Get(k)
				if err != nil || !pred(k, val) {
					continue // stale index entry — verify against live value
				}
				out = append(out, NSEntry{Key: k[len(prefix):], Value: val})
				if limit > 0 && len(out) >= limit {
					break
				}
			}
			return out, nil
		}
	}

	// Fallback: predicate scan over the namespace.
	kvs := se.predicateScanCollect(pred, prefix, limit)
	out := make([]NSEntry, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, NSEntry{Key: kv.Key[len(prefix):], Value: kv.Value})
	}
	return out, nil
}
