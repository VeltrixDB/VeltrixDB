// Package ycsbdriver registers VeltrixDB as a go-ycsb database ("veltrixdb"),
// so the same YCSB workloads and runner drive VeltrixDB, Aerospike
// (go-ycsb's "aerospike") and ScyllaDB (go-ycsb's "cassandra", CQL).
//
// A YCSB record (field → value) is stored as ONE key whose value is the
// fields encoded as [2B nameLen][name][4B valueLen][value]…, so a read or an
// insert is one round trip, as for the other two databases. A partial update
// (writeallfields=false) is read-modify-write — two round trips — because
// VeltrixDB has no server-side field merge on plain keys; set
// writeallfields=true (the compare workloads do) for a like-for-like
// comparison. Scans (workload E) use RANGE over the ordered key index.
//
// Properties:
//
//	veltrixdb.addr     host:port of the binary protocol (default 127.0.0.1:9000)
//	veltrixdb.timeout  dial timeout (default 5s)
package ycsbdriver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	"github.com/VeltrixDB/veltrixdb/client"
)

type creator struct{}

type db struct {
	addr    string
	timeout time.Duration
}

type ctxKey struct{}

func init() { ycsb.RegisterDBCreator("veltrixdb", creator{}) }

func (creator) Create(p *properties.Properties) (ycsb.DB, error) {
	return &db{
		addr:    p.GetString("veltrixdb.addr", "127.0.0.1:9000"),
		timeout: p.GetDuration("veltrixdb.timeout", 5*time.Second),
	}, nil
}

func (d *db) Close() error { return nil }

// InitThread gives every YCSB worker its own connection.
func (d *db) InitThread(ctx context.Context, _ int, _ int) context.Context {
	c, err := client.DialBinary(d.addr, d.timeout)
	if err != nil {
		panic(fmt.Sprintf("veltrixdb: dial %s: %v", d.addr, err))
	}
	return context.WithValue(ctx, ctxKey{}, c)
}

func (d *db) CleanupThread(ctx context.Context) { conn(ctx).Close() }

func conn(ctx context.Context) *client.BinaryConn { return ctx.Value(ctxKey{}).(*client.BinaryConn) }

func key(table, k string) string { return table + ":" + k }

func encode(values map[string][]byte) []byte {
	n := 0
	for f, v := range values {
		n += 6 + len(f) + len(v)
	}
	out := make([]byte, 0, n)
	var b [4]byte
	for f, v := range values {
		binary.LittleEndian.PutUint16(b[:2], uint16(len(f)))
		out = append(out, b[:2]...)
		out = append(out, f...)
		binary.LittleEndian.PutUint32(b[:], uint32(len(v)))
		out = append(out, b[:]...)
		out = append(out, v...)
	}
	return out
}

func decode(buf []byte, fields []string) (map[string][]byte, error) {
	want := map[string]bool{}
	for _, f := range fields {
		want[f] = true
	}
	out := map[string][]byte{}
	for len(buf) > 0 {
		if len(buf) < 2 {
			return nil, errors.New("veltrixdb: truncated record")
		}
		fl := int(binary.LittleEndian.Uint16(buf))
		buf = buf[2:]
		if len(buf) < fl+4 {
			return nil, errors.New("veltrixdb: truncated record")
		}
		f := string(buf[:fl])
		vl := int(binary.LittleEndian.Uint32(buf[fl:]))
		buf = buf[fl+4:]
		if len(buf) < vl {
			return nil, errors.New("veltrixdb: truncated record")
		}
		if len(want) == 0 || want[f] {
			out[f] = buf[:vl]
		}
		buf = buf[vl:]
	}
	return out, nil
}

func (d *db) Read(ctx context.Context, table, k string, fields []string) (map[string][]byte, error) {
	v, err := conn(ctx).Get(key(table, k))
	if err != nil {
		return nil, err
	}
	return decode(v, fields)
}

func (d *db) Scan(ctx context.Context, table, start string, count int, fields []string) ([]map[string][]byte, error) {
	kvs, err := conn(ctx).RangeScan(key(table, start), table+";", count, false) // ';' = ':'+1
	if err != nil {
		return nil, err
	}
	out := make([]map[string][]byte, 0, len(kvs))
	for _, kv := range kvs {
		m, err := decode(kv.Value, fields)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func (d *db) Update(ctx context.Context, table, k string, values map[string][]byte) error {
	c := conn(ctx)
	old, err := c.Get(key(table, k))
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
		return err
	}
	merged := map[string][]byte{}
	if err == nil {
		if merged, err = decode(old, nil); err != nil {
			return err
		}
	}
	for f, v := range values {
		merged[f] = v
	}
	return c.Put(key(table, k), encode(merged), 0)
}

func (d *db) Insert(ctx context.Context, table, k string, values map[string][]byte) error {
	return conn(ctx).Put(key(table, k), encode(values), 0)
}

func (d *db) Delete(ctx context.Context, table, k string) error {
	return conn(ctx).Delete(key(table, k))
}
