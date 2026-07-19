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
		keys := engine.LookupBySecondary(name, value)
		if limit := int(int32(valLen)); limit > 0 && len(keys) > limit {
			keys = keys[:limit]
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
		matches, err := engine.SearchVector(defaultVectorNS, query, keyLen)
		if err != nil {
			return sendErr(err.Error())
		}
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
		entries, err := engine.QueryNS(ns, field, op, value, int(int32(valLen)))
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
