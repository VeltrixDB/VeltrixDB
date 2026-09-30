package main

// ext_ops.go — binary wire handlers for the extended opcodes 0x20–0x29:
// ordered range scans, one-shot optimistic transactions, secondary indexes,
// the vector index, and the minimal field-predicate query.
//
// All handlers follow the existing framing conventions of main.go: the
// dispatcher has already consumed the standard 7-byte header
// [1B cmd][2B keyLen LE][4B valLen LE]; handlers read any op-specific extra
// header bytes plus the body, and reply either with the standard
// [1B status][4B payloadLen LE][payload] frame or with a counted list
// [1B status][4B count LE] + entries.
//
// A returned error means the connection is broken and must be closed;
// protocol-level failures are reported to the client and return nil.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/VeltrixDB/veltrixdb/security"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// defaultVectorNS is the single vector namespace exposed over the wire by
// VSET / VSEARCH. Its dimensionality is fixed by the first VSET (or by the
// persisted vectors reloaded at startup).
const defaultVectorNS = "default"

// maxExtStrLen bounds every variable-length string field in extended frames.
const maxExtStrLen = 1 << 20

// SEARCH (0x1C) sub-operations — see the handler for field layouts.
const (
	searchSubVCreate = 1
	searchSubTSet    = 2
	searchSubTDel    = 3
	searchSubTSearch = 4
	searchSubHSearch = 5
)

// maxSearchPayload bounds one SEARCH frame (a 1 MB document plus framing).
const maxSearchPayload = 2 << 20

// searchFields reads a SEARCH payload's length-prefixed fields in order;
// the first failure sticks in err and later reads return zero values.
type searchFields struct {
	fields [][]byte
	next   int
	err    error
}

func parseSearchFields(p []byte) (*searchFields, error) {
	f := &searchFields{}
	for len(p) > 0 {
		if len(p) < 4 {
			return nil, fmt.Errorf("truncated SEARCH field header")
		}
		n := int(binary.LittleEndian.Uint32(p))
		p = p[4:]
		if n > len(p) {
			return nil, fmt.Errorf("SEARCH field length %d exceeds payload", n)
		}
		f.fields = append(f.fields, p[:n])
		p = p[n:]
	}
	return f, nil
}

func (f *searchFields) raw() []byte {
	if f.err != nil {
		return nil
	}
	if f.next >= len(f.fields) {
		f.err = fmt.Errorf("SEARCH frame is missing field %d", f.next+1)
		return nil
	}
	b := f.fields[f.next]
	f.next++
	return b
}

func (f *searchFields) str() string { return string(f.raw()) }

func (f *searchFields) u32() uint32 {
	b := f.raw()
	if f.err == nil && len(b) != 4 {
		f.err = fmt.Errorf("SEARCH field %d: want 4 bytes, got %d", f.next, len(b))
	}
	if f.err != nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (f *searchFields) f32() float32 { return math.Float32frombits(f.u32()) }

func (f *searchFields) vec() []float32 {
	b := f.raw()
	if f.err == nil && len(b)%4 != 0 {
		f.err = fmt.Errorf("SEARCH vector field is %d bytes, not a multiple of 4", len(b))
	}
	if f.err != nil || len(b) == 0 {
		return nil
	}
	if len(b)/4 > 4096 {
		f.err = fmt.Errorf("vector dim out of range: %d", len(b)/4)
		return nil
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v
}

// filter reads the trailing (field, op, value) triple; field "" = none.
func (f *searchFields) filter() *storage.VectorFilter {
	field, op, value := f.str(), f.str(), f.str()
	if f.err != nil || field == "" {
		return nil
	}
	return &storage.VectorFilter{Field: field, Op: op, Value: value}
}

// handleExtOp dispatches one extended opcode. keyLen/valLen are the two
// standard header fields, reinterpreted per opcode (see the constants in
// main.go for each layout).
func handleExtOp(cmd byte, keyLen, valLen int, br *bufio.Reader, bw *bufio.Writer, engine *storage.StorageEngine, ca *security.ConnAuth, coord *coordinator) error {
	// sendResp writes one standard response frame and flushes.
	sendResp := func(status byte, payload []byte) error {
		hdr := [5]byte{status}
		binary.LittleEndian.PutUint32(hdr[1:], uint32(len(payload)))
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		if len(payload) > 0 {
			if _, err := bw.Write(payload); err != nil {
				return err
			}
		}
		return bw.Flush()
	}
	sendErr := func(msg string) error { return sendResp(binStatusErr, []byte(msg)) }

	// checkPerm enforces RBAC exactly like the 0x01–0x1B handlers: report
	// the denial to the client, then drop the connection.
	checkPerm := func(p security.Permission) (error, bool) {
		if err := ca.Check(p); err != nil {
			_ = sendResp(binStatusErr, []byte(err.Error()))
			return fmt.Errorf("permission denied"), false
		}
		return nil, true
	}

	readStr := func(n int) (string, error) {
		if n < 0 || n > maxExtStrLen {
			return "", fmt.Errorf("string field too large: %d", n)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(br, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	}

	// writeKVList emits [1B OK][4B count] + count × [2B keyLen][4B valLen][key][value].
	writeKVList := func(kvs []storage.KV) error {
		var hdr [5]byte
		hdr[0] = binStatusOK
		binary.LittleEndian.PutUint32(hdr[1:], uint32(len(kvs)))
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		var ent [6]byte
		for _, kv := range kvs {
			binary.LittleEndian.PutUint16(ent[0:2], uint16(len(kv.Key)))
			binary.LittleEndian.PutUint32(ent[2:6], uint32(len(kv.Value)))
			if _, err := bw.Write(ent[:]); err != nil {
				return err
			}
			if _, err := bw.WriteString(kv.Key); err != nil {
				return err
			}
			if _, err := bw.Write(kv.Value); err != nil {
				return err
			}
		}
		return nil
	}

	// readVec reads dim × 4B float32 LE.
	readVec := func(dim int) ([]float32, error) {
		raw := make([]byte, 4*dim)
		if _, err := io.ReadFull(br, raw); err != nil {
			return nil, err
		}
		vec := make([]float32, dim)
		for i := range vec {
			vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
		}
		return vec, nil
	}

	// writeVectorMatches emits [1B OK][4B count] + count × [2B idLen][4B score float32 LE][id].
	writeVectorMatches := func(matches []storage.VectorMatch) error {
		var hdr [5]byte
		hdr[0] = binStatusOK
		binary.LittleEndian.PutUint32(hdr[1:], uint32(len(matches)))
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		var ent [6]byte
		for _, m := range matches {
			binary.LittleEndian.PutUint16(ent[0:2], uint16(len(m.ID)))
			binary.LittleEndian.PutUint32(ent[2:6], math.Float32bits(m.Score))
			if _, err := bw.Write(ent[:]); err != nil {
				return err
			}
			if _, err := bw.WriteString(m.ID); err != nil {
				return err
			}
		}
		return bw.Flush()
	}

	switch cmd {

	// ── RANGE (0x20) ────────────────────────────────────────────────────────
	// Header: keyLen=startLen, valLen=limit (signed). Extra: [2B endLen][1B flags].
	// flags bit0 = reverse. Body: start + end.
	// Response: [1B OK][4B count] + count × [2B keyLen][4B valLen][key][value].
	case binCmdRange:
		var extra [3]byte
		if _, err := io.ReadFull(br, extra[:]); err != nil {
			return err
		}
		endLen := int(binary.LittleEndian.Uint16(extra[0:2]))
		reverse := extra[2]&0x01 != 0
		start, err := readStr(keyLen)
		if err != nil {
			return err
		}
		end, err := readStr(endLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermRead); !ok {
			return err
		}
		kvs, err := engine.RangeScan(start, end, int(int32(valLen)), reverse)
		if err != nil {
			return sendErr(err.Error())
		}
		if err := writeKVList(kvs); err != nil {
			return err
		}
		return bw.Flush()

	// ── SCANCUR (0x21) ──────────────────────────────────────────────────────
	// Header: keyLen=cursorLen, valLen=limit. Body: cursor.
	// Response: [1B OK][4B count] + entries + [2B nextCursorLen][nextCursor].
	case binCmdScanCur:
		cursor, err := readStr(keyLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermRead); !ok {
			return err
		}
		kvs, next, err := engine.ScanCursor(cursor, int(int32(valLen)))
		if err != nil {
			return sendErr(err.Error())
		}
		if err := writeKVList(kvs); err != nil {
			return err
		}
		var nl [2]byte
		binary.LittleEndian.PutUint16(nl[:], uint16(len(next)))
		if _, err := bw.Write(nl[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(next); err != nil {
			return err
		}
		return bw.Flush()

	// ── TXN (0x22) — one-shot optimistic transaction ────────────────────────
	// Header: keyLen unused, valLen=opCount. Per op:
	//   [1B opType][2B keyLen][4B valLen][4B ttl LE signed][8B expectedVersion LE][key][value]
	//   opType: 0=SET, 1=SETIF (guarded by expectedVersion), 2=DEL.
	// Response: [1B status][4B len][msg] with status 0x00=committed,
	// 0x05=conflict (retry with fresh versions), 0x01=error.
	//
	// Honesty note: this is read-committed optimistic CAS. Version guards are
	// validated, then the write set commits atomically per disk via MultiPut.
	// It is atomic within the batch but NOT serializable.
	case binCmdTxn:
		opCount := valLen
		if opCount <= 0 || opCount > maxBatchCount {
			return sendErr(fmt.Sprintf("txn op count out of range: %d", opCount))
		}
		if err, ok := checkPerm(security.PermWrite); !ok {
			return err
		}
		txnOps := make([]fsmTxnOp, 0, opCount)
		var opHdr [19]byte
		for i := 0; i < opCount; i++ {
			if _, err := io.ReadFull(br, opHdr[:]); err != nil {
				return err
			}
			opType := opHdr[0]
			kLen := int(binary.LittleEndian.Uint16(opHdr[1:3]))
			vLen := int(binary.LittleEndian.Uint32(opHdr[3:7]))
			ttl := int32(binary.LittleEndian.Uint32(opHdr[7:11]))
			expected := binary.LittleEndian.Uint64(opHdr[11:19])
			key, err := readStr(kLen)
			if err != nil {
				return err
			}
			if vLen < 0 || vLen > 64<<20 {
				return sendErr("txn value too large")
			}
			val := make([]byte, vLen)
			if _, err := io.ReadFull(br, val); err != nil {
				return err
			}
			switch opType {
			case 0:
				txnOps = append(txnOps, fsmTxnOp{Op: "SET", Key: key, Value: val, TTL: ttl})
			case 1:
				txnOps = append(txnOps, fsmTxnOp{Op: "SETIF", Key: key, Value: val, TTL: ttl, ExpectedVersion: expected})
			case 2:
				txnOps = append(txnOps, fsmTxnOp{Op: "DEL", Key: key})
			default:
				return sendErr(fmt.Sprintf("txn op %d: unknown type 0x%02x", i, opType))
			}
		}
		switch err := coord.Txn(txnOps); {
		case err == nil:
			return sendResp(binStatusOK, nil)
		case err == storage.ErrTxnConflict:
			return sendResp(binStatusConflict, nil)
		default:
			return sendErr(err.Error())
		}

	// ── IDXCREATE (0x23) ────────────────────────────────────────────────────
	// Header: keyLen=nameLen, valLen=fieldLen. Body: name + field.
	case binCmdIdxCreate:
		name, err := readStr(keyLen)
		if err != nil {
			return err
		}
		field, err := readStr(valLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermWrite); !ok {
			return err
		}
		if err := coord.IdxCreate(name, field); err != nil {
			return sendErr(err.Error())
		}
		return sendResp(binStatusOK, nil)

	// ── IDXDROP (0x24) ──────────────────────────────────────────────────────
	// Header: keyLen=nameLen, valLen=0. Body: name.
	case binCmdIdxDrop:
		name, err := readStr(keyLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermWrite); !ok {
			return err
		}
		if err := coord.IdxDrop(name); err != nil {
			return sendErr(err.Error())
		}
		return sendResp(binStatusOK, nil)

	// ── IDXQUERY (0x25) ─────────────────────────────────────────────────────
	// Header: keyLen=nameLen, valLen=limit. Extra: [2B valueLen].
	// Body: name + value. Response: [1B OK][4B count] + count × [2B keyLen][key].
	case binCmdIdxQuery:
		var vl [2]byte
		if _, err := io.ReadFull(br, vl[:]); err != nil {
			return err
		}
		valueLen := int(binary.LittleEndian.Uint16(vl[:]))
		name, err := readStr(keyLen)
		if err != nil {
			return err
		}
		value, err := readStr(valueLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermRead); !ok {
			return err
		}
		keys, err := coord.IdxQuery(name, value, int(int32(valLen)))
		if err != nil {
			return sendErr(err.Error())
		}
		var hdr [5]byte
		hdr[0] = binStatusOK
		binary.LittleEndian.PutUint32(hdr[1:], uint32(len(keys)))
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		var kl [2]byte
		for _, k := range keys {
			binary.LittleEndian.PutUint16(kl[:], uint16(len(k)))
			if _, err := bw.Write(kl[:]); err != nil {
				return err
			}
			if _, err := bw.WriteString(k); err != nil {
				return err
			}
		}
		return bw.Flush()

	// ── VSET (0x26) ─────────────────────────────────────────────────────────
	// Header: keyLen=keyLen, valLen=dim. Body: key + dim × 4B float32 LE.
	case binCmdVSet:
		dim := valLen
		if dim <= 0 || dim > 4096 {
			return sendErr(fmt.Sprintf("vector dim out of range: %d", dim))
		}
		key, err := readStr(keyLen)
		if err != nil {
			return err
		}
		raw := make([]byte, 4*dim)
		if _, err := io.ReadFull(br, raw); err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermWrite); !ok {
			return err
		}
		vec := make([]float32, dim)
		for i := range vec {
			vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
		}
		if err := coord.VSet(defaultVectorNS, key, vec); err != nil {
			return sendErr(err.Error())
		}
		return sendResp(binStatusOK, nil)

	// ── VSEARCH (0x27) ──────────────────────────────────────────────────────
	// Header: keyLen=k, valLen=dim. Body: dim × 4B float32 LE.
	// Response: [1B OK][4B count] + count × [2B idLen][4B score float32 LE][id].
	case binCmdVSearch:
		dim := valLen
		if dim <= 0 || dim > 4096 {
			return sendErr(fmt.Sprintf("vector dim out of range: %d", dim))
		}
		raw := make([]byte, 4*dim)
		if _, err := io.ReadFull(br, raw); err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermRead); !ok {
			return err
		}
		query := make([]float32, dim)
		for i := range query {
			query[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
		}
		matches, err := coord.VSearch(defaultVectorNS, query, keyLen, storage.VectorSearchOptions{})
		if err != nil {
			return sendErr(err.Error())
		}
		return writeVectorMatches(matches)

	// ── VSETNS (0x08) ───────────────────────────────────────────────────────
	// Header: keyLen=idLen, valLen=dim. Extra: [2B nsLen].
	// Body: ns + id + dim × 4B float32 LE.
	case binCmdVSetNS:
		dim := valLen
		if dim <= 0 || dim > 4096 {
			return sendErr(fmt.Sprintf("vector dim out of range: %d", dim))
		}
		var extra [2]byte
		if _, err := io.ReadFull(br, extra[:]); err != nil {
			return err
		}
		ns, err := readStr(int(binary.LittleEndian.Uint16(extra[:])))
		if err != nil {
			return err
		}
		id, err := readStr(keyLen)
		if err != nil {
			return err
		}
		vec, err := readVec(dim)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermWrite); !ok {
			return err
		}
		if err := coord.VSet(ns, id, vec); err != nil {
			return sendErr(err.Error())
		}
		return sendResp(binStatusOK, nil)

	// ── VSEARCHX (0x1E) ─────────────────────────────────────────────────────
	// Header: keyLen=k, valLen=dim.
	// Extra: [2B nsLen][2B ef][2B fieldLen][2B opLen][2B valueLen].
	// Body: ns + field + op + value + dim × 4B float32 LE.
	// fieldLen=0 → no filter; ef=0 → default beam width.
	// Response: as VSEARCH.
	case binCmdVSearchX:
		dim := valLen
		if dim <= 0 || dim > 4096 {
			return sendErr(fmt.Sprintf("vector dim out of range: %d", dim))
		}
		var extra [10]byte
		if _, err := io.ReadFull(br, extra[:]); err != nil {
			return err
		}
		ns, err := readStr(int(binary.LittleEndian.Uint16(extra[0:2])))
		if err != nil {
			return err
		}
		ef := int(binary.LittleEndian.Uint16(extra[2:4]))
		field, err := readStr(int(binary.LittleEndian.Uint16(extra[4:6])))
		if err != nil {
			return err
		}
		op, err := readStr(int(binary.LittleEndian.Uint16(extra[6:8])))
		if err != nil {
			return err
		}
		value, err := readStr(int(binary.LittleEndian.Uint16(extra[8:10])))
		if err != nil {
			return err
		}
		query, err := readVec(dim)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermRead); !ok {
			return err
		}
		opts := storage.VectorSearchOptions{Ef: ef}
		if field != "" {
			opts.Filter = &storage.VectorFilter{Field: field, Op: op, Value: value}
		}
		matches, err := coord.VSearch(ns, query, keyLen, opts)
		if err != nil {
			return sendErr(err.Error())
		}
		return writeVectorMatches(matches)

	// ── SEARCH (0x1C) ───────────────────────────────────────────────────────
	// Header: keyLen=subop, valLen=payloadLen. Payload: fields, each
	// [4B len LE][bytes]; u32 fields are 4 bytes LE, f32 fields IEEE-754 bits.
	//   1 VCREATE: ns, dim u32, quant ("" | none | int8)                → OK
	//   2 TSET:    ns, id, text                                        → OK
	//   3 TDEL:    ns, id                                              → OK
	//   4 TSEARCH: ns, k u32, query, filterField, filterOp, filterValue → matches
	//   5 HSEARCH: ns, k u32, ef u32, alpha f32 (NaN = default 0.5),
	//              candidates u32, vec (dim × 4B, may be empty), query,
	//              filterField, filterOp, filterValue                   → matches
	// filterField "" = no filter. Matches are encoded as in VSEARCH.
	case binCmdSearch:
		if valLen < 0 || valLen > maxSearchPayload {
			return sendErr(fmt.Sprintf("search payload too large: %d", valLen))
		}
		payload := make([]byte, valLen)
		if _, err := io.ReadFull(br, payload); err != nil {
			return err
		}
		f, err := parseSearchFields(payload)
		if err != nil {
			return sendErr(err.Error())
		}
		write := keyLen == searchSubVCreate || keyLen == searchSubTSet || keyLen == searchSubTDel
		perm := security.PermRead
		if write {
			perm = security.PermWrite
		}
		if err, ok := checkPerm(perm); !ok {
			return err
		}
		switch keyLen {
		case searchSubVCreate:
			ns, dim, quant := f.str(), f.u32(), f.str()
			if f.err != nil {
				return sendErr(f.err.Error())
			}
			err = coord.VCreate(ns, int(dim), quant)
		case searchSubTSet:
			ns, id, text := f.str(), f.str(), f.str()
			if f.err != nil {
				return sendErr(f.err.Error())
			}
			err = coord.TSet(ns, id, text)
		case searchSubTDel:
			ns, id := f.str(), f.str()
			if f.err != nil {
				return sendErr(f.err.Error())
			}
			err = coord.TDel(ns, id)
		case searchSubTSearch:
			ns, k, query := f.str(), f.u32(), f.str()
			filter := f.filter()
			if f.err != nil {
				return sendErr(f.err.Error())
			}
			matches, serr := coord.TSearch(ns, query, int(k), filter)
			if serr != nil {
				return sendErr(serr.Error())
			}
			return writeVectorMatches(matches)
		case searchSubHSearch:
			ns, k, ef := f.str(), f.u32(), f.u32()
			alpha := f.f32()
			cand := f.u32()
			vec := f.vec()
			query := f.str()
			filter := f.filter()
			if f.err != nil {
				return sendErr(f.err.Error())
			}
			opts := storage.HybridOptions{Ef: int(ef), Candidates: int(cand), Filter: filter}
			if !math.IsNaN(float64(alpha)) {
				a := float64(alpha)
				opts.Alpha = &a
			}
			matches, serr := coord.HSearch(ns, vec, query, int(k), opts)
			if serr != nil {
				return sendErr(serr.Error())
			}
			return writeVectorMatches(matches)
		default:
			return sendErr(fmt.Sprintf("unknown SEARCH subop %d", keyLen))
		}
		if err != nil {
			return sendErr(err.Error())
		}
		return sendResp(binStatusOK, nil)

	// ── VDEL (0x1F) ─────────────────────────────────────────────────────────
	// Header: keyLen=idLen, valLen=nsLen. Body: ns + id.
	case binCmdVDel:
		ns, err := readStr(valLen)
		if err != nil {
			return err
		}
		id, err := readStr(keyLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermWrite); !ok {
			return err
		}
		if err := coord.VDel(ns, id); err != nil {
			return sendErr(err.Error())
		}
		return sendResp(binStatusOK, nil)

	// ── QUERY (0x28) ────────────────────────────────────────────────────────
	// Header: keyLen=nsLen, valLen=limit. Extra: [2B fieldLen][2B opLen][2B valueLen].
	// Body: ns + field + op + value. Response: KV list (see RANGE).
	case binCmdQuery:
		var extra [6]byte
		if _, err := io.ReadFull(br, extra[:]); err != nil {
			return err
		}
		fieldLen := int(binary.LittleEndian.Uint16(extra[0:2]))
		opLen := int(binary.LittleEndian.Uint16(extra[2:4]))
		valueLen := int(binary.LittleEndian.Uint16(extra[4:6]))
		ns, err := readStr(keyLen)
		if err != nil {
			return err
		}
		field, err := readStr(fieldLen)
		if err != nil {
			return err
		}
		op, err := readStr(opLen)
		if err != nil {
			return err
		}
		value, err := readStr(valueLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermRead); !ok {
			return err
		}
		entries, err := coord.Query(ns, field, op, value, int(int32(valLen)))
		if err != nil {
			return sendErr(err.Error())
		}
		kvs := make([]storage.KV, len(entries))
		for i, e := range entries {
			kvs[i] = storage.KV{Key: e.Key, Value: e.Value}
		}
		if err := writeKVList(kvs); err != nil {
			return err
		}
		return bw.Flush()

	// ── GETVER (0x29) ───────────────────────────────────────────────────────
	// Header: keyLen=keyLen, valLen=0. Body: key.
	// Response: [1B OK][4B 8][8B version LE] — 0 when the key is absent.
	// The version is the token to pass in a TXN SETIF op.
	case binCmdGetVer:
		key, err := readStr(keyLen)
		if err != nil {
			return err
		}
		if err, ok := checkPerm(security.PermRead); !ok {
			return err
		}
		var ver [8]byte
		binary.LittleEndian.PutUint64(ver[:], engine.KeyVersion(key))
		return sendResp(binStatusOK, ver[:])
	}

	return sendErr(fmt.Sprintf("unknown extended opcode 0x%02x", cmd))
}
