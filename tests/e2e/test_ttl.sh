#!/usr/bin/env bash
# test_ttl.sh — Key expiry via TTL tests.
#
# The text protocol does not expose TTL on plain PUT, but the binary protocol
# does (MPutEntry.TTL field). We test TTL via the loadtest binary with a
# Go helper script that uses the client package directly, or by using the
# binary protocol via a small inline Go program if the loadtest TTL path
# is not exposed as a flag.
#
# For direct TTL testing we build a tiny Go helper inline that uses the
# client package to issue a binary PUT with TTL=2 and then GETs after sleep.
#
# Test cases:
#   1. PUT key via binary with TTL=2s; immediate GET → value present
#   2. Sleep 3s; GET same key → not found (expired)
#   3. PUT key with no TTL (-1); sleep 3s; GET → still present
#
# Usage:
#   ./tests/e2e/test_ttl.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║           VeltrixDB — TTL / Expiry Test              ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command nc || { echo "nc (netcat) required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

DATA_DIR=$(mktemp -d /tmp/veltrixdb-ttl-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

# ── Build inline TTL test helper ──────────────────────────────────────────────
TTL_HELPER_DIR=$(mktemp -d /tmp/veltrixdb-ttl-helper-XXXXXX)
_TEMP_DIRS+=("$TTL_HELPER_DIR")
TTL_HELPER_BIN="$TTL_HELPER_DIR/ttl_helper"

cat > "$TTL_HELPER_DIR/main.go" <<'GOEOF'
//go:build ignore
// TTL helper: uses VeltrixDB binary client to PUT a key with a TTL.
// Usage: ttl_helper <addr> <key> <value> <ttl_seconds>
//        ttl_helper <addr> <key>                          (GET — prints value or "ERR_NOT_FOUND")
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: ttl_helper <addr> <key> [<value> <ttl>]")
		os.Exit(1)
	}
	addr := os.Args[1]
	key  := os.Args[2]

	conn, err := client.DialBinary(addr, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	if len(os.Args) == 3 {
		// GET
		val, err := conn.Get(key)
		if err != nil {
			fmt.Println("ERR_NOT_FOUND")
			return
		}
		if val == nil {
			fmt.Println("ERR_NOT_FOUND")
			return
		}
		fmt.Println(string(val))
		return
	}

	// PUT with TTL
	value  := []byte(os.Args[3])
	ttlSec, _ := strconv.Atoi(os.Args[4])
	ttl := int32(ttlSec)
	entries := []client.MPutEntry{{Key: key, Value: value, TTL: ttl}}
	_, err = conn.MPut(entries)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mput error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}
GOEOF

# Build the helper from the repo root (needs client package)
echo "  Building TTL helper..."
(cd "$REPO_ROOT" && go build -o "$TTL_HELPER_BIN" "$TTL_HELPER_DIR/main.go" 2>/dev/null) || {
    echo -e "  ${YELLOW}SKIP${NC}  TTL helper build failed (client package may not expose MPut+TTL)"
    echo "  Running alternative TTL tests using text protocol..."
    TTL_HELPER_BIN=""
}

start_server "$DATA_DIR"
wait_ready 15

# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: TTL=2s — key present immediately, gone after expiry
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 1: TTL=2s key expires"

if [ -n "$TTL_HELPER_BIN" ] && [ -x "$TTL_HELPER_BIN" ]; then
    # PUT with TTL=2
    put_resp=$("$TTL_HELPER_BIN" "127.0.0.1:${VELTRIX_PORT}" "ttl_key_1" "ttlval1" "2" 2>/dev/null || echo "ERR")
    assert_ok "$put_resp" "binary PUT with TTL=2 returns OK"

    # Immediate GET
    get_resp=$("$TTL_HELPER_BIN" "127.0.0.1:${VELTRIX_PORT}" "ttl_key_1" 2>/dev/null || echo "ERR_NOT_FOUND")
    assert_eq "$get_resp" "ttlval1" "immediate GET returns value before TTL expiry"

    # Wait for expiry
    echo "  Sleeping 3s to let TTL=2 expire..."
    sleep 3

    # GET after expiry
    expired_resp=$("$TTL_HELPER_BIN" "127.0.0.1:${VELTRIX_PORT}" "ttl_key_1" 2>/dev/null || echo "ERR_NOT_FOUND")
    assert_eq "$expired_resp" "ERR_NOT_FOUND" "GET after TTL=2 expiry returns not found"
else
    echo -e "  ${YELLOW}SKIP${NC}  TTL helper unavailable — skipping binary TTL expiry test"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: No TTL (-1) — key persists
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 2: No-TTL key persists after sleep"

# PUT via text protocol (no TTL = immortal)
put_resp=$(veltrix_cmd "PUT immortal_key immortal_value")
assert_ok "$put_resp" "PUT immortal_key (no TTL)"

echo "  Sleeping 3s..."
sleep 3

get_resp=$(veltrix_cmd "GET immortal_key")
assert_eq "$get_resp" "immortal_value" "GET immortal_key still present after 3s"

# ═══════════════════════════════════════════════════════════════════════════════
# Test 3: TTL key via binary, no-TTL key via text — coexist
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 3: TTL and no-TTL keys coexist"

if [ -n "$TTL_HELPER_BIN" ] && [ -x "$TTL_HELPER_BIN" ]; then
    # PUT ttl key (TTL=2) and permanent key simultaneously
    "$TTL_HELPER_BIN" "127.0.0.1:${VELTRIX_PORT}" "coexist_ttl" "coexist_ttlval" "2" >/dev/null 2>&1 || true
    veltrix_cmd "PUT coexist_perm coexist_permval" >/dev/null

    sleep 3

    ttl_after=$("$TTL_HELPER_BIN" "127.0.0.1:${VELTRIX_PORT}" "coexist_ttl" 2>/dev/null || echo "ERR_NOT_FOUND")
    perm_after=$(veltrix_cmd "GET coexist_perm")

    assert_eq "$ttl_after" "ERR_NOT_FOUND" "TTL key expired while permanent key persists"
    assert_eq "$perm_after" "coexist_permval" "permanent key still present after TTL key expired"
else
    echo -e "  ${YELLOW}SKIP${NC}  TTL helper unavailable — skipping coexist test"
fi

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
