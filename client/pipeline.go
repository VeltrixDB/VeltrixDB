package client

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

// Binary protocol opcodes and status codes for the pipeline client.
const (
	binPut  byte = 0x01
	binGet  byte = 0x02
	binDel  byte = 0x03
	binMPut byte = 0x06
	binMGet byte = 0x07
	binAuth byte = 0x09

	binRange     byte = 0x20
	binScanCur   byte = 0x21
	binTxn       byte = 0x22
	binIdxCreate byte = 0x23
	binIdxDrop   byte = 0x24
	binIdxQuery  byte = 0x25
	binVSet      byte = 0x26
	binVSearch   byte = 0x27
	binQuery     byte = 0x28
	binGetVer    byte = 0x29

	binOK       byte = 0x00
	binErr      byte = 0x01
	binNotFound byte = 0x02
	binConflict byte = 0x05
)

// PipeResult holds the outcome of one pipelined command.
type PipeResult struct {
	Value    []byte
	NotFound bool
	Err      error
}

// pipeCmd is one deferred command in a Pipeline.
type pipeCmd struct {
	op  byte
	key string
	val []byte
	ttl int32
}

// pipeCmdSlicePool reuses []pipeCmd slices across pipeline executions to
// eliminate per-Exec heap allocation at high RPS.
var pipeCmdSlicePool = sync.Pool{
	New: func() any {
		s := make([]pipeCmd, 0, 64)
		return &s
	},
}

// BinaryConn is a single persistent TCP connection that always speaks the
// VeltrixDB binary wire protocol.  It is NOT safe for concurrent use.
type BinaryConn struct {
	addr string
	c    net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

// DialBinary opens a binary-protocol connection to addr.
func DialBinary(addr string, timeout time.Duration) (*BinaryConn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &BinaryConn{
		addr: addr,
		c:    c,
		r:    bufio.NewReaderSize(c, 64*1024),
		w:    bufio.NewWriterSize(c, 64*1024),
	}, nil
}

// Close closes the underlying connection.
func (bc *BinaryConn) Close() { bc.c.Close() }

// Redial drops the current connection and opens a fresh one.
func (bc *BinaryConn) Redial(timeout time.Duration) error {
	bc.c.Close()
	c, err := net.DialTimeout("tcp", bc.addr, timeout)
	if err != nil {
		return fmt.Errorf("redial %s: %w", bc.addr, err)
	}
	bc.c = c
	bc.r = bufio.NewReaderSize(c, 64*1024)
	bc.w = bufio.NewWriterSize(c, 64*1024)
	return nil
}

// Auth authenticates the connection with the binary AUTH frame (0x09).
// Required before other commands when the server runs with --auth-config.
func (bc *BinaryConn) Auth(username, password string) error {
	if err := bc.writeSingleFrame(binAuth, username, []byte(password)); err != nil {
		return err
	}
	if err := bc.w.Flush(); err != nil {
		return err
	}
	status, payload, err := bc.readResp()
	if err != nil {
		return err
	}
	if status != binOK {
		return fmt.Errorf("auth: %s", payload)
	}
	return nil
}

// Put sends a single binary PUT and waits for the response.
func (bc *BinaryConn) Put(key string, value []byte, ttl int32) error {
	if err := bc.writeSingleFrame(binPut, key, value); err != nil {
		return err
	}
	if err := bc.w.Flush(); err != nil {
		return err
	}
	status, _, err := bc.readResp()
	if err != nil {
		return err
	}
	if status != binOK {
		return fmt.Errorf("put %s: server error", key)
	}
	return nil
}

// Get sends a single binary GET and returns the value.
// Returns (nil, nil) when the key is not found.
func (bc *BinaryConn) Get(key string) ([]byte, error) {
	if err := bc.writeSingleFrame(binGet, key, nil); err != nil {
		return nil, err
	}
	if err := bc.w.Flush(); err != nil {
		return nil, err
	}
	status, payload, err := bc.readResp()
	if err != nil {
		return nil, err
	}
	if status == binNotFound {
		return nil, nil
	}
	return payload, nil
}

// Delete sends a single binary DEL.
func (bc *BinaryConn) Delete(key string) error {
	if err := bc.writeSingleFrame(binDel, key, nil); err != nil {
		return err
	}
	if err := bc.w.Flush(); err != nil {
		return err
	}
	status, _, err := bc.readResp()
	if err != nil {
		return err
	}
	if status != binOK {
		return fmt.Errorf("del %s: server error", key)
	}
	return nil
}

// writeSingleFrame encodes [1B cmd][2B keyLen LE][4B valLen LE][key][value]
// into the writer buffer without flushing.
func (bc *BinaryConn) writeSingleFrame(cmd byte, key string, val []byte) error {
	var hdr [7]byte
	hdr[0] = cmd
	hdr[1] = byte(len(key))
	hdr[2] = byte(len(key) >> 8)
	hdr[3] = byte(len(val))
	hdr[4] = byte(len(val) >> 8)
	hdr[5] = byte(len(val) >> 16)
	hdr[6] = byte(len(val) >> 24)
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := bc.w.WriteString(key); err != nil {
		return err
	}
	if len(val) > 0 {
		_, err := bc.w.Write(val)
		return err
	}
	return nil
}

// readResp reads [1B status][4B payloadLen LE][payload] from the server.
func (bc *BinaryConn) readResp() (status byte, payload []byte, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(bc.r, hdr[:]); err != nil {
		return
	}
	status = hdr[0]
	payLen := int(binary.LittleEndian.Uint32(hdr[1:]))
	if payLen > 0 {
		payload = make([]byte, payLen)
		_, err = io.ReadFull(bc.r, payload)
	}
	return
}

// ──────────────────────────────────────────────────────────────────────────────
// Extended binary ops — range scans, transactions, indexes, vectors, query
// ──────────────────────────────────────────────────────────────────────────────

// readKVList reads [1B status][4B count] + count × [2B keyLen][4B valLen][key][value].
// On status != OK the (already-read) payload is returned as the error message.
func (bc *BinaryConn) readKVList(what string) ([]KV, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(bc.r, hdr[:]); err != nil {
		return nil, fmt.Errorf("%s resp header: %w", what, err)
	}
	if hdr[0] != binOK {
		payLen := int(binary.LittleEndian.Uint32(hdr[1:]))
		msg := make([]byte, payLen)
		if _, err := io.ReadFull(bc.r, msg); err != nil {
			return nil, fmt.Errorf("%s: server error", what)
		}
		return nil, fmt.Errorf("%s: %s", what, msg)
	}
	count := int(binary.LittleEndian.Uint32(hdr[1:]))
	kvs := make([]KV, 0, count)
	var ent [6]byte
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(bc.r, ent[:]); err != nil {
			return nil, fmt.Errorf("%s entry %d: %w", what, i, err)
		}
		kl := int(binary.LittleEndian.Uint16(ent[0:2]))
		vl := int(binary.LittleEndian.Uint32(ent[2:6]))
		buf := make([]byte, kl+vl)
		if _, err := io.ReadFull(bc.r, buf); err != nil {
			return nil, fmt.Errorf("%s entry %d data: %w", what, i, err)
		}
		kvs = append(kvs, KV{Key: string(buf[:kl]), Value: buf[kl:]})
	}
	return kvs, nil
}

// RangeScan sends a RANGE (0x20) frame: up to limit pairs with
// start ≤ key < end in ascending order (descending when reverse).
//
// Request: [0x20][2B startLen LE][4B limit LE] + [2B endLen LE][1B flags] + start + end
func (bc *BinaryConn) RangeScan(start, end string, limit int, reverse bool) ([]KV, error) {
	var hdr [10]byte
	hdr[0] = binRange
	binary.LittleEndian.PutUint16(hdr[1:3], uint16(len(start)))
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(limit))
	binary.LittleEndian.PutUint16(hdr[7:9], uint16(len(end)))
	if reverse {
		hdr[9] = 0x01
	}
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err := bc.w.WriteString(start); err != nil {
		return nil, err
	}
	if _, err := bc.w.WriteString(end); err != nil {
		return nil, err
	}
	if err := bc.w.Flush(); err != nil {
		return nil, err
	}
	return bc.readKVList("range")
}

// ScanCursor sends a SCANCUR (0x21) frame. cursor "" starts pagination; the
// returned next cursor is "" when the keyspace is exhausted.
//
// Request:  [0x21][2B cursorLen LE][4B limit LE] + cursor
// Response: KV list + [2B nextLen LE][next]
func (bc *BinaryConn) ScanCursor(cursor string, limit int) ([]KV, string, error) {
	var hdr [7]byte
	hdr[0] = binScanCur
	binary.LittleEndian.PutUint16(hdr[1:3], uint16(len(cursor)))
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(limit))
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return nil, "", err
	}
	if _, err := bc.w.WriteString(cursor); err != nil {
		return nil, "", err
	}
	if err := bc.w.Flush(); err != nil {
		return nil, "", err
	}
	kvs, err := bc.readKVList("scancur")
	if err != nil {
		return nil, "", err
	}
	var nl [2]byte
	if _, err := io.ReadFull(bc.r, nl[:]); err != nil {
		return nil, "", fmt.Errorf("scancur next cursor: %w", err)
	}
	next := make([]byte, binary.LittleEndian.Uint16(nl[:]))
	if _, err := io.ReadFull(bc.r, next); err != nil {
		return nil, "", fmt.Errorf("scancur next cursor: %w", err)
	}
	return kvs, string(next), nil
}

// Txn submits all ops as one TXN (0x22) frame — a one-shot optimistic
// transaction: SETIF version guards are validated server-side, then the
// write set commits atomically per disk. Read-committed optimistic CAS,
// NOT serializable. Returns ErrTxnConflict when a guard failed.
//
// Request: [0x22][2B 0][4B opCount] then per op:
//
//	[1B opType 0=SET,1=SETIF,2=DEL][2B keyLen][4B valLen][4B ttl][8B expectedVersion][key][value]
func (bc *BinaryConn) Txn(ops []TxnOp) error {
	if len(ops) == 0 {
		return nil
	}
	var hdr [7]byte
	hdr[0] = binTxn
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(len(ops)))
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return err
	}
	var opHdr [19]byte
	for _, op := range ops {
		var opType byte
		switch op.Op {
		case "SET", "set":
			opType = 0
		case "SETIF", "setif":
			opType = 1
		case "DEL", "del":
			opType = 2
		default:
			return fmt.Errorf("txn: unknown op %q (want SET/SETIF/DEL)", op.Op)
		}
		opHdr[0] = opType
		binary.LittleEndian.PutUint16(opHdr[1:3], uint16(len(op.Key)))
		binary.LittleEndian.PutUint32(opHdr[3:7], uint32(len(op.Value)))
		ttl := int32(-1) // immortal
		binary.LittleEndian.PutUint32(opHdr[7:11], uint32(ttl))
		binary.LittleEndian.PutUint64(opHdr[11:19], op.ExpectedVersion)
		if _, err := bc.w.Write(opHdr[:]); err != nil {
			return err
		}
		if _, err := bc.w.WriteString(op.Key); err != nil {
			return err
		}
		if len(op.Value) > 0 {
			if _, err := bc.w.Write(op.Value); err != nil {
				return err
			}
		}
	}
	if err := bc.w.Flush(); err != nil {
		return err
	}
	status, payload, err := bc.readResp()
	if err != nil {
		return err
	}
	switch status {
	case binOK:
		return nil
	case binConflict:
		return ErrTxnConflict
	default:
		return fmt.Errorf("txn: %s", payload)
	}
}

// KeyVersion sends GETVER (0x29) and returns the optimistic version token
// for key (0 = absent) — the value to pass in TxnOp.ExpectedVersion.
func (bc *BinaryConn) KeyVersion(key string) (uint64, error) {
	if err := bc.writeSingleFrame(binGetVer, key, nil); err != nil {
		return 0, err
	}
	if err := bc.w.Flush(); err != nil {
		return 0, err
	}
	status, payload, err := bc.readResp()
	if err != nil {
		return 0, err
	}
	if status != binOK || len(payload) < 8 {
		return 0, fmt.Errorf("getver %s: %s", key, payload)
	}
	return binary.LittleEndian.Uint64(payload), nil
}

// IdxCreate sends IDXCREATE (0x23): index <field> of every value.
func (bc *BinaryConn) IdxCreate(name, field string) error {
	if err := bc.writeSingleFrame(binIdxCreate, name, []byte(field)); err != nil {
		return err
	}
	if err := bc.w.Flush(); err != nil {
		return err
	}
	status, payload, err := bc.readResp()
	if err != nil {
		return err
	}
	if status != binOK {
		return fmt.Errorf("idxcreate: %s", payload)
	}
	return nil
}

// IdxDrop sends IDXDROP (0x24).
func (bc *BinaryConn) IdxDrop(name string) error {
	if err := bc.writeSingleFrame(binIdxDrop, name, nil); err != nil {
		return err
	}
	if err := bc.w.Flush(); err != nil {
		return err
	}
	status, payload, err := bc.readResp()
	if err != nil {
		return err
	}
	if status != binOK {
		return fmt.Errorf("idxdrop: %s", payload)
	}
	return nil
}

// IdxQuery sends IDXQUERY (0x25) and returns matching primary keys.
//
// Request: [0x25][2B nameLen][4B limit] + [2B valueLen] + name + value
func (bc *BinaryConn) IdxQuery(name, value string, limit int) ([]string, error) {
	var hdr [9]byte
	hdr[0] = binIdxQuery
	binary.LittleEndian.PutUint16(hdr[1:3], uint16(len(name)))
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(limit))
	binary.LittleEndian.PutUint16(hdr[7:9], uint16(len(value)))
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err := bc.w.WriteString(name); err != nil {
		return nil, err
	}
	if _, err := bc.w.WriteString(value); err != nil {
		return nil, err
	}
	if err := bc.w.Flush(); err != nil {
		return nil, err
	}
	var respHdr [5]byte
	if _, err := io.ReadFull(bc.r, respHdr[:]); err != nil {
		return nil, fmt.Errorf("idxquery resp: %w", err)
	}
	if respHdr[0] != binOK {
		msg := make([]byte, binary.LittleEndian.Uint32(respHdr[1:]))
		_, _ = io.ReadFull(bc.r, msg)
		return nil, fmt.Errorf("idxquery: %s", msg)
	}
	count := int(binary.LittleEndian.Uint32(respHdr[1:]))
	keys := make([]string, 0, count)
	var kl [2]byte
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(bc.r, kl[:]); err != nil {
			return nil, err
		}
		buf := make([]byte, binary.LittleEndian.Uint16(kl[:]))
		if _, err := io.ReadFull(bc.r, buf); err != nil {
			return nil, err
		}
		keys = append(keys, string(buf))
	}
	return keys, nil
}

// VSet sends VSET (0x26): store a vector under key in the server's default
// vector namespace. The namespace dimensionality is fixed by the first VSET.
//
// Request: [0x26][2B keyLen][4B dim] + key + dim × 4B float32 LE
func (bc *BinaryConn) VSet(key string, vec []float32) error {
	var hdr [7]byte
	hdr[0] = binVSet
	binary.LittleEndian.PutUint16(hdr[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(len(vec)))
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := bc.w.WriteString(key); err != nil {
		return err
	}
	var fb [4]byte
	for _, f := range vec {
		binary.LittleEndian.PutUint32(fb[:], math.Float32bits(f))
		if _, err := bc.w.Write(fb[:]); err != nil {
			return err
		}
	}
	if err := bc.w.Flush(); err != nil {
		return err
	}
	status, payload, err := bc.readResp()
	if err != nil {
		return err
	}
	if status != binOK {
		return fmt.Errorf("vset %s: %s", key, payload)
	}
	return nil
}

// VSearch sends VSEARCH (0x27) and returns the top-k matches by cosine
// similarity.
//
// Request:  [0x27][2B k][4B dim] + dim × 4B float32 LE
// Response: [1B OK][4B count] + count × [2B idLen][4B score float32][id]
func (bc *BinaryConn) VSearch(k int, query []float32) ([]VectorResult, error) {
	var hdr [7]byte
	hdr[0] = binVSearch
	binary.LittleEndian.PutUint16(hdr[1:3], uint16(k))
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(len(query)))
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return nil, err
	}
	var fb [4]byte
	for _, f := range query {
		binary.LittleEndian.PutUint32(fb[:], math.Float32bits(f))
		if _, err := bc.w.Write(fb[:]); err != nil {
			return nil, err
		}
	}
	if err := bc.w.Flush(); err != nil {
		return nil, err
	}
	var respHdr [5]byte
	if _, err := io.ReadFull(bc.r, respHdr[:]); err != nil {
		return nil, fmt.Errorf("vsearch resp: %w", err)
	}
	if respHdr[0] != binOK {
		msg := make([]byte, binary.LittleEndian.Uint32(respHdr[1:]))
		_, _ = io.ReadFull(bc.r, msg)
		return nil, fmt.Errorf("vsearch: %s", msg)
	}
	count := int(binary.LittleEndian.Uint32(respHdr[1:]))
	out := make([]VectorResult, 0, count)
	var ent [6]byte
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(bc.r, ent[:]); err != nil {
			return nil, err
		}
		idLen := int(binary.LittleEndian.Uint16(ent[0:2]))
		score := math.Float32frombits(binary.LittleEndian.Uint32(ent[2:6]))
		id := make([]byte, idLen)
		if _, err := io.ReadFull(bc.r, id); err != nil {
			return nil, err
		}
		out = append(out, VectorResult{ID: string(id), Score: float64(score)})
	}
	return out, nil
}

// Query sends QUERY (0x28): field-predicate query over namespace ns.
// ops: = != > < >= <= contains.
//
// Request: [0x28][2B nsLen][4B limit] + [2B fieldLen][2B opLen][2B valueLen] + ns+field+op+value
func (bc *BinaryConn) Query(ns, field, op, value string, limit int) ([]KV, error) {
	var hdr [13]byte
	hdr[0] = binQuery
	binary.LittleEndian.PutUint16(hdr[1:3], uint16(len(ns)))
	binary.LittleEndian.PutUint32(hdr[3:7], uint32(limit))
	binary.LittleEndian.PutUint16(hdr[7:9], uint16(len(field)))
	binary.LittleEndian.PutUint16(hdr[9:11], uint16(len(op)))
	binary.LittleEndian.PutUint16(hdr[11:13], uint16(len(value)))
	if _, err := bc.w.Write(hdr[:]); err != nil {
		return nil, err
	}
	for _, s := range []string{ns, field, op, value} {
		if _, err := bc.w.WriteString(s); err != nil {
			return nil, err
		}
	}
	if err := bc.w.Flush(); err != nil {
		return nil, err
	}
	return bc.readKVList("query")
}

// ──────────────────────────────────────────────────────────────────────────────
// Pipeline — async request multiplexing
// ──────────────────────────────────────────────────────────────────────────────

// Pipeline accumulates Put / Get / Delete commands and dispatches them in a
// single network round trip (one Flush → one set of reads).  This amortises
// the per-syscall overhead across the entire batch, which is the primary
// driver of high-RPS performance on a single connection.
//
// Pipeline is NOT safe for concurrent use.
type Pipeline struct {
	bc   *BinaryConn
	cmds *[]pipeCmd // pooled slice
}

// NewPipeline returns a Pipeline backed by bc.
func NewPipeline(bc *BinaryConn) *Pipeline {
	cmds := pipeCmdSlicePool.Get().(*[]pipeCmd)
	*cmds = (*cmds)[:0]
	return &Pipeline{bc: bc, cmds: cmds}
}

// Put queues a write.
func (p *Pipeline) Put(key string, value []byte, ttl int32) {
	*p.cmds = append(*p.cmds, pipeCmd{op: binPut, key: key, val: value, ttl: ttl})
}

// Get queues a read.
func (p *Pipeline) Get(key string) {
	*p.cmds = append(*p.cmds, pipeCmd{op: binGet, key: key})
}

// Delete queues a delete.
func (p *Pipeline) Delete(key string) {
	*p.cmds = append(*p.cmds, pipeCmd{op: binDel, key: key})
}

// Exec sends all queued commands in one Flush and collects all responses.
// The returned slice has one PipeResult per queued command in order.
// The internal queue is reset so Exec can be called repeatedly.
func (p *Pipeline) Exec() ([]PipeResult, error) {
	cmds := *p.cmds
	if len(cmds) == 0 {
		return nil, nil
	}

	// Write all frames into the buffered writer without flushing.
	for _, cmd := range cmds {
		if err := p.bc.writeSingleFrame(cmd.op, cmd.key, cmd.val); err != nil {
			return nil, fmt.Errorf("pipeline write: %w", err)
		}
	}
	// One Flush → one syscall for all N requests.
	if err := p.bc.w.Flush(); err != nil {
		return nil, fmt.Errorf("pipeline flush: %w", err)
	}

	// Read all N responses in order.
	results := make([]PipeResult, len(cmds))
	for i := range cmds {
		status, payload, err := p.bc.readResp()
		if err != nil {
			return nil, fmt.Errorf("pipeline recv[%d]: %w", i, err)
		}
		switch status {
		case binOK:
			results[i] = PipeResult{Value: payload}
		case binNotFound:
			results[i] = PipeResult{NotFound: true}
		default:
			results[i] = PipeResult{Err: fmt.Errorf("%s", payload)}
		}
	}

	// Reset queue; return cmd slice to pool.
	*p.cmds = (*p.cmds)[:0]
	return results, nil
}

// Close returns the pipeline's command slice to the pool and closes the
// underlying connection.
func (p *Pipeline) Close() {
	pipeCmdSlicePool.Put(p.cmds)
	p.bc.Close()
}

// ──────────────────────────────────────────────────────────────────────────────
// Batch commands — single-frame MPUT / MGET
// ──────────────────────────────────────────────────────────────────────────────

// MPutEntry is one entry in a vectorized MPUT frame.
type MPutEntry struct {
	Key   string
	Value []byte
	TTL   int32 // seconds; -1 = immortal
}

// MPut sends all entries in a single MPUT frame and returns per-entry errors.
//
// Wire format (request):
//
//	[0x06][2B unused=0][4B count LE]
//	N × [2B keyLen LE][4B valLen LE][4B ttl LE][key][value]
//
// Wire format (response):
//
//	[1B 0x00 OK][4B count LE]
//	N × [1B status]
func (bc *BinaryConn) MPut(entries []MPutEntry) ([]error, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	// Batch frame header: [0x06][2B 0][4B count LE]
	var batchHdr [7]byte
	batchHdr[0] = binMPut
	binary.LittleEndian.PutUint32(batchHdr[3:], uint32(len(entries)))
	if _, err := bc.w.Write(batchHdr[:]); err != nil {
		return nil, err
	}

	// Per-entry: [2B keyLen LE][4B valLen LE][4B ttl LE][key][value]
	var entHdr [10]byte
	for _, e := range entries {
		binary.LittleEndian.PutUint16(entHdr[0:], uint16(len(e.Key)))
		binary.LittleEndian.PutUint32(entHdr[2:], uint32(len(e.Value)))
		binary.LittleEndian.PutUint32(entHdr[6:], uint32(e.TTL))
		if _, err := bc.w.Write(entHdr[:]); err != nil {
			return nil, err
		}
		if _, err := bc.w.WriteString(e.Key); err != nil {
			return nil, err
		}
		if len(e.Value) > 0 {
			if _, err := bc.w.Write(e.Value); err != nil {
				return nil, err
			}
		}
	}
	if err := bc.w.Flush(); err != nil {
		return nil, err
	}

	// Read batch response: [1B status][4B count][N × 1B per-entry status]
	var respHdr [5]byte
	if _, err := io.ReadFull(bc.r, respHdr[:]); err != nil {
		return nil, fmt.Errorf("mput resp header: %w", err)
	}
	if respHdr[0] != binOK {
		return nil, fmt.Errorf("mput: server error")
	}
	count := int(binary.LittleEndian.Uint32(respHdr[1:]))
	statuses := make([]byte, count)
	if _, err := io.ReadFull(bc.r, statuses); err != nil {
		return nil, fmt.Errorf("mput resp statuses: %w", err)
	}

	errs := make([]error, count)
	for i, s := range statuses {
		if s != binOK {
			errs[i] = fmt.Errorf("entry %d: server error", i)
		}
	}
	return errs, nil
}

// MGet sends all keys in a single MGET frame and returns per-key results.
//
// Wire format (request):
//
//	[0x07][2B unused=0][4B count LE]
//	N × [2B keyLen LE][key]
//
// Wire format (response):
//
//	[1B 0x00 OK][4B count LE]
//	N × [1B status][4B valLen LE][value]
func (bc *BinaryConn) MGet(keys []string) ([]PipeResult, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Batch frame header: [0x07][2B 0][4B count LE]
	var batchHdr [7]byte
	batchHdr[0] = binMGet
	binary.LittleEndian.PutUint32(batchHdr[3:], uint32(len(keys)))
	if _, err := bc.w.Write(batchHdr[:]); err != nil {
		return nil, err
	}

	var keyHdr [2]byte
	for _, key := range keys {
		binary.LittleEndian.PutUint16(keyHdr[:], uint16(len(key)))
		if _, err := bc.w.Write(keyHdr[:]); err != nil {
			return nil, err
		}
		if _, err := bc.w.WriteString(key); err != nil {
			return nil, err
		}
	}
	if err := bc.w.Flush(); err != nil {
		return nil, err
	}

	// Read batch response: [1B status][4B count][N × [1B status][4B valLen][value]]
	var respHdr [5]byte
	if _, err := io.ReadFull(bc.r, respHdr[:]); err != nil {
		return nil, fmt.Errorf("mget resp header: %w", err)
	}
	if respHdr[0] != binOK {
		return nil, fmt.Errorf("mget: server error")
	}
	count := int(binary.LittleEndian.Uint32(respHdr[1:]))

	results := make([]PipeResult, count)
	var entHdr [5]byte
	for i := range results {
		if _, err := io.ReadFull(bc.r, entHdr[:]); err != nil {
			return nil, fmt.Errorf("mget entry %d header: %w", i, err)
		}
		status := entHdr[0]
		valLen := int(binary.LittleEndian.Uint32(entHdr[1:]))

		if status == binNotFound || valLen == 0 {
			results[i] = PipeResult{NotFound: status == binNotFound}
			continue
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(bc.r, val); err != nil {
			return nil, fmt.Errorf("mget entry %d value: %w", i, err)
		}
		results[i] = PipeResult{Value: val}
	}
	return results, nil
}
