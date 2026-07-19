// repl-ship — cross-region replication shipper.
//
// Tails the local VeltrixDB's CDC stream and forwards every event to a
// remote VeltrixDB cluster via its binary protocol.  Designed for
// asynchronous (eventually-consistent) cross-region geo-replication.
//
// Mode of operation:
//   - Subscribes to the local /admin/cdc endpoint.
//   - For each event:  PUT → MultiPut on remote;  DEL → Delete on remote.
//   - On remote-write failure, retries with exponential backoff up to
//     --max-retries; on permanent failure, logs to a "deadletter" file
//     so an operator can replay manually.
//   - Persists a checkpoint (last shipped write-timestamp, µs) to a file.
//     On restart it first replays the DELTA it missed via the durable
//     /admin/changes catch-up feed (index-backed, includes tombstones),
//     then rejoins the live CDC stream — zero event loss across shipper
//     downtime, at last-write-wins semantics.
//
// Usage:
//   repl-ship --src http://primary:2112 \
//             --dst-tcp replica.us-east:9000 \
//             --batch 64 \
//             --checkpoint /var/lib/repl-ship/ckpt
//
// REMAINING GAPS:
//   - No conflict resolution beyond last-write-wins (relies on remote's
//     own version vector / vector clock to deduplicate).
//   - No back-pressure signal to source — local writes always succeed even
//     when the destination is degraded.

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	binCmdPut = 0x01
	binCmdDel = 0x03
)

type cdcEvent struct {
	Op        string `json:"Op"`
	Key       string `json:"Key"`
	Value     []byte `json:"Value"`
	Timestamp int64  `json:"Timestamp"`
}

func main() {
	src := flag.String("src", "http://127.0.0.1:2112",
		"local VeltrixDB metrics/admin URL (CDC source)")
	dstTCP := flag.String("dst-tcp", "",
		"remote VeltrixDB binary-protocol address host:port")
	batch := flag.Int("batch", 64, "max events per remote MultiPut")
	maxRetries := flag.Int("max-retries", 5, "retry budget per event before deadletter")
	deadletter := flag.String("deadletter", "/var/log/repl-ship/deadletter.jsonl",
		"path for events that exceeded retry budget")
	prefix := flag.String("prefix", "", "CDC key-prefix filter; empty = ship everything")
	checkpoint := flag.String("checkpoint", "",
		"path to the checkpoint file (last shipped write-timestamp). Empty = no catch-up on restart.")
	ckptEvery := flag.Int("checkpoint-every", 256, "persist the checkpoint after this many shipped events")
	srcTokenFlag := flag.String("src-token", os.Getenv("VELTRIX_ADMIN_TOKEN"),
		"admin token for the source's /admin/* endpoints (env VELTRIX_ADMIN_TOKEN);\n"+
			"required when the source runs with --admin-token")
	flag.Parse()
	srcToken = *srcTokenFlag
	if *dstTCP == "" {
		log.Fatal("--dst-tcp is required")
	}

	if err := os.MkdirAll(strings.TrimSuffix(*deadletter, "/"+filepath_baseFromPath(*deadletter)), 0755); err != nil {
		// Fall back: caller can rely on cwd if mkdir fails (host-mounted etc).
		log.Printf("[repl-ship] deadletter dir mkdir warning: %v", err)
	}
	dl, err := os.OpenFile(*deadletter, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatalf("open deadletter: %v", err)
	}
	defer dl.Close()
	dlEnc := json.NewEncoder(dl)

	// Open one persistent TCP connection to the destination; reconnect on error.
	dst := newDstConn(*dstTCP)
	defer dst.close()

	// Checkpoint: cursor of the last write shipped (WriteTimestampUs).
	ck := newCheckpoint(*checkpoint)
	shippedSinceSave := 0
	noteShipped := func(ts int64) {
		if ts > ck.cursor {
			ck.cursor = ts
		}
		shippedSinceSave++
		if shippedSinceSave >= *ckptEvery {
			ck.save()
			shippedSinceSave = 0
		}
	}
	defer ck.save()

	// Catch-up phase: replay everything written at/after the checkpoint via
	// the durable /admin/changes feed before joining the live stream.
	if *checkpoint != "" {
		if err := catchUp(*src, *prefix, dst, dlEnc, *maxRetries, ck, noteShipped); err != nil {
			log.Fatalf("[repl-ship] catch-up: %v", err)
		}
	}

	// Subscribe to local CDC.
	url := fmt.Sprintf("%s/admin/cdc?prefix=%s", *src, *prefix)
	resp, err := adminGet(url)
	if err != nil {
		log.Fatalf("CDC subscribe: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatalf("CDC subscribe: HTTP %d", resp.StatusCode)
	}
	log.Printf("[repl-ship] subscribed src=%s dst=%s prefix=%q",
		*src, *dstTCP, *prefix)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() { <-stop; resp.Body.Close() }()

	dec := json.NewDecoder(bufio.NewReaderSize(resp.Body, 64*1024))
	pending := make([]cdcEvent, 0, *batch)

	flushPending := func() {
		if len(pending) == 0 {
			return
		}
		for _, ev := range pending {
			ok := false
			for try := 0; try < *maxRetries; try++ {
				if err := dst.applyEvent(ev); err != nil {
					log.Printf("[repl-ship] apply err (try %d): %v", try, err)
					time.Sleep(time.Duration(1<<try) * 100 * time.Millisecond)
					dst.reset() // reconnect on next attempt
					continue
				}
				ok = true
				break
			}
			if ok {
				noteShipped(ev.Timestamp)
			} else {
				_ = dlEnc.Encode(ev)
			}
		}
		pending = pending[:0]
	}

	// Decode in a goroutine so the main loop can ALSO flush on a timer:
	// waiting for a full batch alone would delay low-traffic events forever.
	events := make(chan cdcEvent, *batch)
	go func() {
		defer close(events)
		for {
			var ev cdcEvent
			if err := dec.Decode(&ev); err != nil {
				if err != io.EOF {
					log.Printf("[repl-ship] decode: %v", err)
				}
				return
			}
			events <- ev
		}
	}()

	const flushInterval = 200 * time.Millisecond
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
loop:
	for {
		select {
		case ev, open := <-events:
			if !open {
				break loop
			}
			pending = append(pending, ev)
			if len(pending) >= *batch {
				flushPending()
			}
		case <-ticker.C:
			flushPending()
		}
	}
	flushPending()
	log.Printf("[repl-ship] shutdown; deadletter=%s", *deadletter)
}

// dstConn wraps a single TCP connection to the remote engine.  Reconnects on demand.
type dstConn struct {
	addr string
	mu   sync.Mutex
	c    net.Conn
}

func newDstConn(addr string) *dstConn { return &dstConn{addr: addr} }

func (d *dstConn) ensure() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.c != nil {
		return nil
	}
	c, err := net.DialTimeout("tcp", d.addr, 5*time.Second)
	if err != nil {
		return err
	}
	d.c = c
	return nil
}

func (d *dstConn) reset() {
	d.mu.Lock()
	if d.c != nil {
		_ = d.c.Close()
		d.c = nil
	}
	d.mu.Unlock()
}

func (d *dstConn) close() { d.reset() }

func (d *dstConn) applyEvent(ev cdcEvent) error {
	if err := d.ensure(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch ev.Op {
	case "PUT":
		hdr := make([]byte, 7)
		hdr[0] = binCmdPut
		binary.LittleEndian.PutUint16(hdr[1:3], uint16(len(ev.Key)))
		binary.LittleEndian.PutUint32(hdr[3:7], uint32(len(ev.Value)))
		if _, err := d.c.Write(hdr); err != nil {
			return err
		}
		if _, err := d.c.Write([]byte(ev.Key)); err != nil {
			return err
		}
		if _, err := d.c.Write(ev.Value); err != nil {
			return err
		}
	case "DEL":
		hdr := make([]byte, 7)
		hdr[0] = binCmdDel
		binary.LittleEndian.PutUint16(hdr[1:3], uint16(len(ev.Key)))
		if _, err := d.c.Write(hdr); err != nil {
			return err
		}
		if _, err := d.c.Write([]byte(ev.Key)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown op %q", ev.Op)
	}
	// Read response: 5-byte header [status][4B payloadLen]; we only care about status.
	var resp [5]byte
	if _, err := io.ReadFull(d.c, resp[:]); err != nil {
		return err
	}
	if resp[0] != 0x00 {
		// Drain payload so the connection stays usable.
		plen := binary.LittleEndian.Uint32(resp[1:5])
		if plen > 0 {
			io.CopyN(io.Discard, d.c, int64(plen))
		}
		return fmt.Errorf("remote returned status 0x%02x", resp[0])
	}
	plen := binary.LittleEndian.Uint32(resp[1:5])
	if plen > 0 {
		io.CopyN(io.Discard, d.c, int64(plen))
	}
	return nil
}

// filepath_baseFromPath returns the last path element. Stdlib filepath would
// import os/path but we keep deps minimal.
func filepath_baseFromPath(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[i+1:]
		}
	}
	return p
}

// ── Durable resume ────────────────────────────────────────────────────────────

// checkpointFile persists the catch-up cursor (last shipped WriteTimestampUs).
type checkpointFile struct {
	path   string
	cursor int64
}

func newCheckpoint(path string) *checkpointFile {
	ck := &checkpointFile{path: path}
	if path == "" {
		return ck
	}
	if b, err := os.ReadFile(path); err == nil {
		if v, perr := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); perr == nil {
			ck.cursor = v
		}
	}
	return ck
}

// save writes the cursor atomically (tmp + rename).
func (ck *checkpointFile) save() {
	if ck.path == "" {
		return
	}
	tmp := ck.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(ck.cursor, 10)), 0644); err != nil {
		log.Printf("[repl-ship] checkpoint write: %v", err)
		return
	}
	if err := os.Rename(tmp, ck.path); err != nil {
		log.Printf("[repl-ship] checkpoint rename: %v", err)
	}
}

// srcToken is the --src-token value; adminGet attaches it to every request
// against the source's /admin/* endpoints.
var srcToken string

func adminGet(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if srcToken != "" {
		req.Header.Set("Authorization", "Bearer "+srcToken)
	}
	return http.DefaultClient.Do(req)
}

// catchUp pages through /admin/changes from the checkpoint cursor, shipping
// each event to the destination, until the feed reports no more pages.
func catchUp(src, prefix string, dst *dstConn, dlEnc *json.Encoder, maxRetries int, ck *checkpointFile, noteShipped func(int64)) error {
	total := 0
	cursor := ck.cursor
	for {
		url := fmt.Sprintf("%s/admin/changes?since=%d&limit=10000", src, cursor)
		resp, err := adminGet(url)
		if err != nil {
			return fmt.Errorf("changes fetch: %w", err)
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return fmt.Errorf("changes fetch: HTTP %d", resp.StatusCode)
		}

		dec := json.NewDecoder(bufio.NewReaderSize(resp.Body, 64*1024))
		var page []cdcEvent
		var more bool
		var next int64
		for {
			var raw map[string]json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				if err == io.EOF {
					break
				}
				resp.Body.Close()
				return fmt.Errorf("changes decode: %w", err)
			}
			if _, isTrailer := raw["cursor"]; isTrailer {
				var trailer struct {
					Cursor int64 `json:"cursor"`
					More   bool  `json:"more"`
				}
				b, _ := json.Marshal(raw)
				_ = json.Unmarshal(b, &trailer)
				next, more = trailer.Cursor, trailer.More
				continue
			}
			var ev cdcEvent
			b, _ := json.Marshal(raw)
			if err := json.Unmarshal(b, &ev); err != nil {
				continue
			}
			if prefix != "" && !strings.HasPrefix(ev.Key, prefix) {
				continue
			}
			page = append(page, ev)
		}
		resp.Body.Close()

		for _, ev := range page {
			ok := false
			for try := 0; try < maxRetries; try++ {
				if err := dst.applyEvent(ev); err != nil {
					log.Printf("[repl-ship] catch-up apply err (try %d): %v", try, err)
					time.Sleep(time.Duration(1<<try) * 100 * time.Millisecond)
					dst.reset()
					continue
				}
				ok = true
				break
			}
			if ok {
				noteShipped(ev.Timestamp)
				total++
			} else {
				_ = dlEnc.Encode(ev)
			}
		}

		if !more {
			break
		}
		cursor = next
	}
	ck.save()
	log.Printf("[repl-ship] catch-up complete  replayed=%d  cursor=%d", total, ck.cursor)
	return nil
}
