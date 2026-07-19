package client

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// TCPConn is a single persistent TCP connection to a VeltrixDB node.
// It speaks the line-based protocol:
//
//	PUT <key> <value>\n  →  OK\n
//	GET <key>\n          →  <value>\n  |  ERR …\n
//	DEL <key>\n          →  OK\n
//	PING\n               →  PONG\n
//	INFO\n               →  <stats line>\n
//	QUIT\n               →  BYE\n
//
// TCPConn is NOT safe for concurrent use — each goroutine should own one.
type TCPConn struct {
	addr string
	c    net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

// DialTCP dials addr and returns a ready TCPConn.
func DialTCP(addr string, timeout time.Duration) (*TCPConn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	tc := &TCPConn{
		addr: addr,
		c:    c,
		r:    bufio.NewReaderSize(c, 32*1024),
		w:    bufio.NewWriterSize(c, 32*1024),
	}
	return tc, nil
}

// Put sends PUT <key> <value> and waits for OK.
func (tc *TCPConn) Put(key string, value []byte) error {
	fmt.Fprintf(tc.w, "PUT %s %s\n", key, value)
	if err := tc.w.Flush(); err != nil {
		return err
	}
	line, err := tc.readLine()
	if err != nil {
		return err
	}
	if line != "OK" {
		return fmt.Errorf("put: %s", line)
	}
	return nil
}

// Get sends GET <key> and returns the value.
// Returns (nil, nil) when the key is not found (not counted as an error).
func (tc *TCPConn) Get(key string) ([]byte, error) {
	fmt.Fprintf(tc.w, "GET %s\n", key)
	if err := tc.w.Flush(); err != nil {
		return nil, err
	}
	line, err := tc.readLine()
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(line, "ERR") {
		return nil, nil // key not found — caller decides whether this is an error
	}
	return []byte(line), nil
}

// Delete sends DEL <key>.
func (tc *TCPConn) Delete(key string) error {
	fmt.Fprintf(tc.w, "DEL %s\n", key)
	if err := tc.w.Flush(); err != nil {
		return err
	}
	line, err := tc.readLine()
	if err != nil {
		return err
	}
	if line != "OK" {
		return fmt.Errorf("del: %s", line)
	}
	return nil
}

// Ping sends PING and verifies PONG.
func (tc *TCPConn) Ping() error {
	fmt.Fprintln(tc.w, "PING")
	if err := tc.w.Flush(); err != nil {
		return err
	}
	line, err := tc.readLine()
	if err != nil {
		return err
	}
	if line != "PONG" {
		return fmt.Errorf("ping: unexpected response %q", line)
	}
	return nil
}

// Auth sends AUTH <username> <password> and returns an error on failure.
// When the server has RBAC disabled, AUTH always returns OK.
func (tc *TCPConn) Auth(username, password string) error {
	fmt.Fprintf(tc.w, "AUTH %s %s\n", username, password)
	if err := tc.w.Flush(); err != nil {
		return err
	}
	line, err := tc.readLine()
	if err != nil {
		return err
	}
	if line != "OK" {
		return fmt.Errorf("auth: %s", line)
	}
	return nil
}

// Info sends INFO and returns the raw stats line.
func (tc *TCPConn) Info() (string, error) {
	fmt.Fprintln(tc.w, "INFO")
	if err := tc.w.Flush(); err != nil {
		return "", err
	}
	return tc.readLine()
}

// Topology sends TOPOLOGY and returns the single JSON line describing the
// cluster (node set, epoch, and — in raft mode — the current leader).  Used by
// the cluster-aware client to bootstrap consistent-hash routing.
func (tc *TCPConn) Topology() (string, error) {
	fmt.Fprintln(tc.w, "TOPOLOGY")
	if err := tc.w.Flush(); err != nil {
		return "", err
	}
	return tc.readLine()
}

// Close sends QUIT and closes the underlying connection.
func (tc *TCPConn) Close() {
	fmt.Fprintln(tc.w, "QUIT")
	_ = tc.w.Flush()
	tc.c.Close()
}

// Redial drops the current connection and opens a fresh one.
// Used by load test workers to recover after a broken connection.
func (tc *TCPConn) Redial(timeout time.Duration) error {
	tc.c.Close()
	c, err := net.DialTimeout("tcp", tc.addr, timeout)
	if err != nil {
		return fmt.Errorf("redial %s: %w", tc.addr, err)
	}
	tc.c = c
	tc.r = bufio.NewReaderSize(c, 32*1024)
	tc.w = bufio.NewWriterSize(c, 32*1024)
	return nil
}

func (tc *TCPConn) readLine() (string, error) {
	line, err := tc.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// ── Extended commands: range scans, transactions, indexes, vectors, query ────

// KV is one key-value pair returned by RangeScan / ScanCursor / Query.
type KV struct {
	Key   string
	Value []byte
}

// VectorResult is one VSEARCH match.
type VectorResult struct {
	ID    string
	Score float64 // cosine similarity in [-1, 1]
}

// TxnOp is one operation inside a Txn call.
//
//	Op="SET":   unconditional write.
//	Op="SETIF": write guarded by ExpectedVersion (from KeyVersion; 0 = key
//	            must not exist). Any failed guard aborts the whole batch.
//	Op="DEL":   delete.
type TxnOp struct {
	Op              string
	Key             string
	Value           []byte
	ExpectedVersion uint64
}

// ErrTxnConflict is returned by Txn when an optimistic version guard failed.
// Re-read versions with KeyVersion and retry.
var ErrTxnConflict = fmt.Errorf("veltrixdb: transaction conflict")

// readKVsUntil reads "key value" lines until the terminator line (returned
// verbatim). Lines starting with "ERR" abort with an error.
func (tc *TCPConn) readKVsUntil(term string) ([]KV, string, error) {
	var kvs []KV
	for {
		line, err := tc.readLine()
		if err != nil {
			return nil, "", err
		}
		if strings.HasPrefix(line, "ERR") {
			return nil, "", fmt.Errorf("%s", line)
		}
		if line == term || (term != "END" && strings.HasPrefix(line, term+" ")) || line == "END" {
			return kvs, line, nil
		}
		sp := strings.IndexByte(line, ' ')
		if sp < 0 {
			kvs = append(kvs, KV{Key: line})
			continue
		}
		kvs = append(kvs, KV{Key: line[:sp], Value: []byte(line[sp+1:])})
	}
}

// RangeScan sends RANGE <start> <end> [LIMIT n] [REV] and collects results.
func (tc *TCPConn) RangeScan(start, end string, limit int, reverse bool) ([]KV, error) {
	cmd := fmt.Sprintf("RANGE %s %s", start, end)
	if limit > 0 {
		cmd += fmt.Sprintf(" LIMIT %d", limit)
	}
	if reverse {
		cmd += " REV"
	}
	fmt.Fprintln(tc.w, cmd)
	if err := tc.w.Flush(); err != nil {
		return nil, err
	}
	kvs, _, err := tc.readKVsUntil("END")
	return kvs, err
}

// ScanCursor sends SCANCUR <cursor> <limit>. cursor "" starts pagination;
// the returned next cursor is "" when the keyspace is exhausted.
func (tc *TCPConn) ScanCursor(cursor string, limit int) ([]KV, string, error) {
	if cursor == "" {
		cursor = "-"
	}
	fmt.Fprintf(tc.w, "SCANCUR %s %d\n", cursor, limit)
	if err := tc.w.Flush(); err != nil {
		return nil, "", err
	}
	kvs, term, err := tc.readKVsUntil("CURSOR")
	if err != nil {
		return nil, "", err
	}
	next := strings.TrimPrefix(term, "CURSOR ")
	if next == "-" || next == term {
		next = ""
	}
	return kvs, next, nil
}

// Txn submits all ops as one atomic optimistic transaction (TXN command).
// Returns ErrTxnConflict when a SETIF version guard failed — re-read
// versions via KeyVersion and retry. Semantics: read-committed optimistic
// CAS, atomic within the batch, not serializable.
func (tc *TCPConn) Txn(ops []TxnOp) error {
	if len(ops) == 0 {
		return nil
	}
	fmt.Fprintf(tc.w, "TXN %d\n", len(ops))
	for _, op := range ops {
		switch strings.ToUpper(op.Op) {
		case "SET":
			fmt.Fprintf(tc.w, "SET %s %s\n", op.Key, op.Value)
		case "SETIF":
			fmt.Fprintf(tc.w, "SETIF %s %d %s\n", op.Key, op.ExpectedVersion, op.Value)
		case "DEL":
			fmt.Fprintf(tc.w, "DEL %s\n", op.Key)
		default:
			return fmt.Errorf("txn: unknown op %q (want SET/SETIF/DEL)", op.Op)
		}
	}
	if err := tc.w.Flush(); err != nil {
		return err
	}
	line, err := tc.readLine()
	if err != nil {
		return err
	}
	switch {
	case line == "OK":
		return nil
	case line == "CONFLICT":
		return ErrTxnConflict
	default:
		return fmt.Errorf("txn: %s", line)
	}
}

// KeyVersion sends VER <key> and returns the optimistic version token
// (0 = key absent) for use in TxnOp.ExpectedVersion.
func (tc *TCPConn) KeyVersion(key string) (uint64, error) {
	fmt.Fprintf(tc.w, "VER %s\n", key)
	if err := tc.w.Flush(); err != nil {
		return 0, err
	}
	line, err := tc.readLine()
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(line, "ERR") {
		return 0, fmt.Errorf("%s", line)
	}
	var v uint64
	_, err = fmt.Sscanf(line, "%d", &v)
	return v, err
}

// IdxCreate sends IDXCREATE <name> <field>.
func (tc *TCPConn) IdxCreate(name, field string) error {
	return tc.simpleOK(fmt.Sprintf("IDXCREATE %s %s", name, field))
}

// IdxDrop sends IDXDROP <name>.
func (tc *TCPConn) IdxDrop(name string) error {
	return tc.simpleOK("IDXDROP " + name)
}

// IdxQuery sends IDXQUERY <name> <value> [LIMIT n] and returns the primary keys.
func (tc *TCPConn) IdxQuery(name, value string, limit int) ([]string, error) {
	cmd := fmt.Sprintf("IDXQUERY %s %s", name, value)
	if limit > 0 {
		cmd += fmt.Sprintf(" LIMIT %d", limit)
	}
	fmt.Fprintln(tc.w, cmd)
	if err := tc.w.Flush(); err != nil {
		return nil, err
	}
	var keys []string
	for {
		line, err := tc.readLine()
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(line, "ERR") {
			return nil, fmt.Errorf("%s", line)
		}
		if line == "END" {
			return keys, nil
		}
		keys = append(keys, line)
	}
}

// VSet sends VSET <key> <floats...> into the server's default vector namespace.
func (tc *TCPConn) VSet(key string, vec []float32) error {
	var sb strings.Builder
	sb.WriteString("VSET ")
	sb.WriteString(key)
	for _, f := range vec {
		sb.WriteByte(' ')
		sb.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	return tc.simpleOK(sb.String())
}

// VSearch sends VSEARCH <k> <floats...> and returns the top-k matches.
func (tc *TCPConn) VSearch(k int, query []float32) ([]VectorResult, error) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "VSEARCH %d", k)
	for _, f := range query {
		sb.WriteByte(' ')
		sb.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	fmt.Fprintln(tc.w, sb.String())
	if err := tc.w.Flush(); err != nil {
		return nil, err
	}
	var out []VectorResult
	for {
		line, err := tc.readLine()
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(line, "ERR") {
			return nil, fmt.Errorf("%s", line)
		}
		if line == "END" {
			return out, nil
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		score, err := strconv.ParseFloat(line[sp+1:], 64)
		if err != nil {
			return nil, fmt.Errorf("vsearch: bad score line %q", line)
		}
		out = append(out, VectorResult{ID: line[:sp], Score: score})
	}
}

// Query sends QUERY <ns> WHERE <field> <op> <value> [LIMIT n].
// ops: = != > < >= <= contains.
func (tc *TCPConn) Query(ns, field, op, value string, limit int) ([]KV, error) {
	cmd := fmt.Sprintf("QUERY %s WHERE %s %s %s", ns, field, op, value)
	if limit > 0 {
		cmd += fmt.Sprintf(" LIMIT %d", limit)
	}
	fmt.Fprintln(tc.w, cmd)
	if err := tc.w.Flush(); err != nil {
		return nil, err
	}
	kvs, _, err := tc.readKVsUntil("END")
	return kvs, err
}

// simpleOK sends one command line and expects "OK".
func (tc *TCPConn) simpleOK(cmd string) error {
	fmt.Fprintln(tc.w, cmd)
	if err := tc.w.Flush(); err != nil {
		return err
	}
	line, err := tc.readLine()
	if err != nil {
		return err
	}
	if line != "OK" {
		return fmt.Errorf("%s", line)
	}
	return nil
}
