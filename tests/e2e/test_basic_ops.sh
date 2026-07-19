#!/usr/bin/env bash
# test_basic_ops.sh — Tests for core text-protocol commands:
#   PUT, GET, DEL, PING, INFO, QUIT, overwrite semantics, bulk ops.
#
# Usage:
#   ./tests/e2e/test_basic_ops.sh
#   VELTRIX_PORT=9001 ./tests/e2e/test_basic_ops.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║           VeltrixDB — Basic Operations Test          ║"
echo "╚══════════════════════════════════════════════════════╝"

# ── Build & start ──────────────────────────────────────────────────────────────
require_command nc || { echo "nc (netcat) required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

DATA_DIR=$(mktemp -d /tmp/veltrixdb-basic-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")
start_server "$DATA_DIR"
wait_ready 15

# ── PING ──────────────────────────────────────────────────────────────────────
section "PING"
resp=$(veltrix_cmd "PING")
assert_eq "$resp" "PONG" "PING returns PONG"

# ── PUT / GET ─────────────────────────────────────────────────────────────────
section "PUT / GET"
resp=$(veltrix_cmd "PUT key1 hello")
assert_ok "$resp" "PUT key1 hello"

resp=$(veltrix_cmd "GET key1")
assert_eq "$resp" "hello" "GET key1 == hello"

# GET nonexistent key should return ERR
resp=$(veltrix_cmd "GET nonexistent_key_xyz")
# The server returns the value or "ERR ..." — a missing key returns "ERR ..."
assert_contains "$resp" "ERR" "GET nonexistent key returns ERR"

# ── DEL ───────────────────────────────────────────────────────────────────────
section "DEL"
resp=$(veltrix_cmd "DEL key1")
assert_ok "$resp" "DEL key1"

resp=$(veltrix_cmd "GET key1")
assert_contains "$resp" "ERR" "GET after DEL returns ERR"

# ── Overwrite ─────────────────────────────────────────────────────────────────
section "Overwrite"
resp=$(veltrix_cmd "PUT overwrite_key value1")
assert_ok "$resp" "PUT overwrite_key value1"

resp=$(veltrix_cmd "PUT overwrite_key value2")
assert_ok "$resp" "PUT overwrite_key value2 (overwrite)"

resp=$(veltrix_cmd "GET overwrite_key")
assert_eq "$resp" "value2" "GET overwrite_key returns latest value"

# ── Value with spaces ─────────────────────────────────────────────────────────
section "Value with spaces"
# Text protocol: SplitN(line, " ", 3) so everything after key is the value.
resp=$(veltrix_cmd "PUT spacekey hello world foo")
assert_ok "$resp" "PUT key with spaces in value"

resp=$(veltrix_cmd "GET spacekey")
assert_eq "$resp" "hello world foo" "GET key returns value with spaces"

# ── INFO ──────────────────────────────────────────────────────────────────────
section "INFO"
resp=$(veltrix_cmd "INFO")
assert_contains "$resp" "keys=" "INFO contains keys="
assert_contains "$resp" "writes=" "INFO contains writes="

# ── Bulk PUT: 100 keys ────────────────────────────────────────────────────────
section "Bulk PUT / INFO key count"
for i in $(seq 1 100); do
    veltrix_cmd "PUT bulk_key_${i} value_${i}" >/dev/null
done

resp=$(veltrix_cmd "INFO")
# Extract keys= value and check it is >= 100
keys_val=$(echo "$resp" | grep -oE 'keys=[0-9]+' | head -1 | cut -d= -f2)
if [ -n "$keys_val" ] && [ "$keys_val" -ge 100 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  INFO shows keys >= 100 after bulk PUT (keys=${keys_val})"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  INFO shows keys >= 100 after bulk PUT (keys=${keys_val:-unknown})"
    FAIL=$((FAIL + 1))
fi

# ── Bulk DEL: remove all 100 keys ────────────────────────────────────────────
section "Bulk DEL / INFO key count drops"
for i in $(seq 1 100); do
    veltrix_cmd "DEL bulk_key_${i}" >/dev/null
done

# Give the engine a moment to update atomics
sleep 0.3
resp=$(veltrix_cmd "INFO")
assert_contains "$resp" "keys=" "INFO still responds after bulk DEL"

# ── SET alias ─────────────────────────────────────────────────────────────────
section "SET alias"
resp=$(veltrix_cmd "SET alias_key alias_val")
assert_ok "$resp" "SET (alias for PUT) returns OK"

resp=$(veltrix_cmd "GET alias_key")
assert_eq "$resp" "alias_val" "GET key set via SET alias"

# ── DELETE alias ─────────────────────────────────────────────────────────────
section "DELETE alias"
resp=$(veltrix_cmd "DELETE alias_key")
assert_ok "$resp" "DELETE (alias for DEL) returns OK"

resp=$(veltrix_cmd "GET alias_key")
assert_contains "$resp" "ERR" "GET returns ERR after DELETE"

# ── QUIT ──────────────────────────────────────────────────────────────────────
section "QUIT"
if nc -h 2>&1 | grep -q "\-q"; then
    quit_resp=$(printf 'QUIT\r\n' | nc -q1 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null | head -1 | tr -d '\r')
else
    quit_resp=$(printf 'QUIT\r\n' | nc 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null | head -1 | tr -d '\r')
fi
assert_eq "$quit_resp" "BYE" "QUIT returns BYE"

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
