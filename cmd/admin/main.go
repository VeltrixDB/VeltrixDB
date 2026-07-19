// veltrix-admin — administrative CLI for VeltrixDB.
//
// Subcommands:
//
//	scan          List all live keys from a running server (text protocol).
//	stats         Print server metrics (keys, writes, reads, cache hit rate …).
//	compact       Force an immediate VLog checkpoint on a running server.
//	check         Offline VLog CRC32C corruption scan — walks vlog_active.dat on disk.
//	repair        Delete a specific key on a running server (tombstone it).
//	hash-password Generate a SHA-256 password hash for the auth config JSON.
//
// Common flags:
//
//	--addr        Server TCP address (default :9000).
//	--user        Username for RBAC-enabled servers.
//	--password    Password for RBAC-enabled servers.
//	--data        Single data directory path (used by offline subcommands).
//	--data-dirs   Comma-separated data directory paths (used by offline subcommands).

package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
	"github.com/VeltrixDB/veltrixdb/security"
)

const (
	vlogMagic     = uint32(0x564C5402) // "VLT\x02"
	vlogBlockSize = 4096               // O_DIRECT alignment unit
	vlogHeaderSize = 24                // magic(4)+valLen(4)+crc(4)+reserved(4)+writeUs(8)

	// Header field offsets
	vhdrOffMagic  = 0
	vhdrOffValLen = 4
	vhdrOffCRC    = 8
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subCmd := os.Args[1]
	args := os.Args[2:]

	switch subCmd {
	case "scan":
		cmdScan(args)
	case "stats":
		cmdStats(args)
	case "compact":
		cmdCompact(args)
	case "check":
		cmdCheck(args)
	case "repair":
		cmdRepair(args)
	case "hash-password":
		cmdHashPassword(args)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %q\n\n", subCmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Print(`Usage: veltrix-admin <subcommand> [flags]

Subcommands:
  scan          List all live keys from a running server.
  stats         Print server metrics (keys, writes, reads, cache, disk …).
  compact       Force a WAL checkpoint on a running server.
  check         Offline VLog CRC32C corruption scan — reads vlog_active.dat.
  repair        Tombstone a key on a running server (DEL <key>).
  hash-password Generate a password hash for the auth config JSON.

Common flags:
  --addr        Server TCP address (default: :9000)
  --user        Username (when RBAC is enabled)
  --password    Password (when RBAC is enabled)
  --data        Single data directory (offline subcommands)
  --data-dirs   Comma-separated data directories (offline subcommands)

Examples:
  veltrix-admin scan --addr :9000
  veltrix-admin stats --addr :9000 --user admin --password secret
  veltrix-admin compact --addr :9000 --user admin --password secret
  veltrix-admin check --data ./veltrixdb-data
  veltrix-admin check --data-dirs /mnt/nvme0,/mnt/nvme1
  veltrix-admin repair --addr :9000 --key corrupt-key-001 --user admin --password secret
  veltrix-admin hash-password --user alice --password secret
`)
}

// ── scan ─────────────────────────────────────────────────────────────────────

func cmdScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	addr := fs.String("addr", ":9000", "Server address")
	user := fs.String("user", "", "Username")
	pass := fs.String("password", "", "Password")
	_ = fs.Parse(args)

	tc := mustDial(*addr)
	defer tc.Close()
	maybeAuth(tc, *user, *pass)

	// Ask for INFO to get key count, then use raw text connection for SCAN.
	// Since there's no dedicated SCAN command in the text protocol, we use
	// INFO to get the count and a direct TCP scan via the raw connection.
	info, err := tc.Info()
	if err != nil {
		log.Fatalf("INFO: %v", err)
	}
	fmt.Println("Server info:", info)
	fmt.Println()

	// Open a second raw connection to do a key-by-key scan via the scan
	// endpoint exposed by the text protocol as "SCAN\n" (if present) or
	// fall back to reading the WAL offline.
	rawConn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		log.Fatalf("dial for scan: %v", err)
	}
	defer rawConn.Close()

	w := bufio.NewWriter(rawConn)
	r := bufio.NewReader(rawConn)

	if *user != "" {
		fmt.Fprintf(w, "AUTH %s %s\n", *user, *pass)
		w.Flush()
		line, _ := r.ReadString('\n')
		if !strings.HasPrefix(strings.TrimSpace(line), "OK") {
			log.Fatalf("auth failed: %s", line)
		}
	}

	// Send SCAN command (added to server text protocol if supported,
	// otherwise the server returns ERR and we display the info line only).
	fmt.Fprintf(w, "SCAN\n")
	w.Flush()

	line, err := r.ReadString('\n')
	if err != nil {
		log.Fatalf("SCAN: %v", err)
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "ERR") {
		fmt.Println("Note: SCAN command not available — use 'veltrix-admin stats' for key count.")
		fmt.Println("Tip:  For offline key listing, run 'veltrix-admin check --data <dir>'.")
		return
	}

	// SCAN response: one key per line until "END\n"
	count := 0
	for {
		kline, err := r.ReadString('\n')
		if err != nil {
			break
		}
		kline = strings.TrimSpace(kline)
		if kline == "END" {
			break
		}
		fmt.Println(kline)
		count++
	}
	fmt.Printf("\n%d keys listed.\n", count)
}

// ── stats ────────────────────────────────────────────────────────────────────

func cmdStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	addr := fs.String("addr", ":9000", "Server address")
	user := fs.String("user", "", "Username")
	pass := fs.String("password", "", "Password")
	_ = fs.Parse(args)

	tc := mustDial(*addr)
	defer tc.Close()
	maybeAuth(tc, *user, *pass)

	info, err := tc.Info()
	if err != nil {
		log.Fatalf("INFO: %v", err)
	}

	fmt.Println("=== VeltrixDB Stats ===")
	for _, field := range strings.Fields(info) {
		kv := strings.SplitN(field, "=", 2)
		if len(kv) == 2 {
			fmt.Printf("  %-28s %s\n", kv[0], kv[1])
		}
	}
}

// ── compact ──────────────────────────────────────────────────────────────────

func cmdCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	addr := fs.String("addr", ":9000", "Server address")
	user := fs.String("user", "", "Username")
	pass := fs.String("password", "", "Password")
	_ = fs.Parse(args)

	rawConn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer rawConn.Close()

	w := bufio.NewWriter(rawConn)
	r := bufio.NewReader(rawConn)

	if *user != "" {
		fmt.Fprintf(w, "AUTH %s %s\n", *user, *pass)
		w.Flush()
		line, _ := r.ReadString('\n')
		if !strings.HasPrefix(strings.TrimSpace(line), "OK") {
			log.Fatalf("auth failed: %s", line)
		}
	}

	fmt.Fprintf(w, "COMPACT\n")
	w.Flush()

	line, err := r.ReadString('\n')
	if err != nil {
		log.Fatalf("COMPACT: %v", err)
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "ERR") {
		fmt.Println("Note: COMPACT command not yet wired in server text protocol.")
		fmt.Println("Tip:  The engine runs automatic GC every 30 s. This is a no-op stub.")
		return
	}
	fmt.Println(line)
}

// ── check ────────────────────────────────────────────────────────────────────

// cmdCheck scans each VLog file for CRC32C corruption without touching the
// running server.  For each record header it reads the stored CRC32C and
// compares it against the CRC32C of the value bytes.  Reports the first
// corrupt offset on each disk.
func cmdCheck(args []string) {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	dataDir := fs.String("data", "", "Single data directory path")
	dataDirs := fs.String("data-dirs", "", "Comma-separated data directory paths")
	_ = fs.Parse(args)

	dirs := resolveDirs(*dataDir, *dataDirs)
	if len(dirs) == 0 {
		log.Fatal("provide --data or --data-dirs")
	}

	totalRecords := 0
	totalCorrupt := 0

	for i, dir := range dirs {
		vlogPath := filepath.Join(dir, "vlog_active.dat")
		f, err := os.Open(vlogPath)
		if os.IsNotExist(err) {
			fmt.Printf("disk[%d] %s — vlog_active.dat not found (KV-separation may be disabled)\n", i, dir)
			continue
		}
		if err != nil {
			fmt.Printf("disk[%d] %s — open error: %v\n", i, dir, err)
			continue
		}

		records, corrupt := scanVLog(i, f)
		f.Close()

		totalRecords += records
		totalCorrupt += corrupt

		if corrupt == 0 {
			fmt.Printf("disk[%d] %s — OK  (%d records scanned)\n", i, dir, records)
		} else {
			fmt.Printf("disk[%d] %s — CORRUPT  (%d/%d records have bad CRC)\n", i, dir, corrupt, records)
		}
	}

	fmt.Printf("\nTotal: %d records  %d corrupt\n", totalRecords, totalCorrupt)
	if totalCorrupt > 0 {
		os.Exit(1)
	}
}

// scanVLog walks a VLog file sequentially, verifying each record's CRC32C.
// Returns (total records scanned, corrupt records count).
func scanVLog(diskIdx int, f *os.File) (int, int) {
	// VLog records start at the first vlogBlockSize boundary ≥ vlogBlockSize.
	offset := int64(vlogBlockSize)
	hdr := make([]byte, vlogHeaderSize)
	records := 0
	corrupt := 0

	for {
		// Seek to the next aligned record.
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			break
		}
		if _, err := io.ReadFull(f, hdr); err != nil {
			break // EOF or short read → end of log
		}

		magic := binary.LittleEndian.Uint32(hdr[vhdrOffMagic:])
		if magic != vlogMagic {
			// Not a valid record header — could be padding or end of written data.
			break
		}

		valLen := binary.LittleEndian.Uint32(hdr[vhdrOffValLen:])
		storedCRC := binary.LittleEndian.Uint32(hdr[vhdrOffCRC:])

		// Read value bytes.
		value := make([]byte, valLen)
		if _, err := io.ReadFull(f, value); err != nil {
			fmt.Printf("  disk[%d] offset=%d: short read for value (valLen=%d): %v\n",
				diskIdx, offset, valLen, err)
			corrupt++
			break
		}

		computedCRC := crc32.Checksum(value, crc32.MakeTable(crc32.Castagnoli))
		if computedCRC != storedCRC {
			fmt.Printf("  disk[%d] offset=%d: CRC mismatch stored=0x%08x computed=0x%08x (valLen=%d)\n",
				diskIdx, offset, storedCRC, computedCRC, valLen)
			corrupt++
		}

		records++

		// Advance to the next 4096-byte-aligned record.
		rawLen := int64(vlogHeaderSize) + int64(valLen)
		alignedLen := (rawLen + int64(vlogBlockSize) - 1) &^ (int64(vlogBlockSize) - 1)
		offset += alignedLen
	}

	return records, corrupt
}

// ── repair ───────────────────────────────────────────────────────────────────

func cmdRepair(args []string) {
	fs := flag.NewFlagSet("repair", flag.ExitOnError)
	addr := fs.String("addr", ":9000", "Server address")
	key := fs.String("key", "", "Key to tombstone (delete)")
	user := fs.String("user", "", "Username")
	pass := fs.String("password", "", "Password")
	_ = fs.Parse(args)

	if *key == "" {
		log.Fatal("--key is required")
	}

	tc := mustDial(*addr)
	defer tc.Close()
	maybeAuth(tc, *user, *pass)

	if err := tc.Delete(*key); err != nil {
		log.Fatalf("DEL %s: %v", *key, err)
	}
	fmt.Printf("OK: key %q tombstoned.\n", *key)
}

// ── hash-password ─────────────────────────────────────────────────────────────

func cmdHashPassword(args []string) {
	fs := flag.NewFlagSet("hash-password", flag.ExitOnError)
	user := fs.String("user", "", "Username")
	pass := fs.String("password", "", "Password to hash")
	legacy := fs.Bool("legacy-sha256", false, "Emit the deprecated SHA-256(password+username) hash instead of PBKDF2")
	_ = fs.Parse(args)

	if *user == "" || *pass == "" {
		log.Fatal("--user and --password are required")
	}

	var hash string
	if *legacy {
		hash = security.HashPassword(*pass, *user)
	} else {
		var err error
		hash, err = security.HashPasswordPBKDF2(*pass)
		if err != nil {
			log.Fatalf("hash password: %v", err)
		}
	}
	fmt.Printf("password_hash: %q\n\n", hash)
	fmt.Printf("Auth config snippet:\n")
	fmt.Printf("{\n  \"users\": [\n    {\n      \"username\": %q,\n      \"password_hash\": %q,\n      \"role\": \"readwrite\"\n    }\n  ]\n}\n", *user, hash)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func mustDial(addr string) *client.TCPConn {
	tc, err := client.DialTCP(addr, 5*time.Second)
	if err != nil {
		log.Fatalf("connect to %s: %v", addr, err)
	}
	return tc
}

func maybeAuth(tc *client.TCPConn, user, pass string) {
	if user == "" {
		return
	}
	if err := tc.Auth(user, pass); err != nil {
		log.Fatalf("AUTH: %v", err)
	}
}

func resolveDirs(single, multi string) []string {
	if multi != "" {
		var dirs []string
		for _, d := range strings.Split(multi, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				dirs = append(dirs, d)
			}
		}
		return dirs
	}
	if single != "" {
		return []string{single}
	}
	return nil
}
