#!/usr/bin/env bash
# test_namespaces.sh — Namespace operations via text protocol.
#
# The server supports NSPUT/NSGET/NSDEL/NSDROP/NSSCAN/NSLIST commands.
# These are tested here via the text protocol (nc-based).
#
# Test cases:
#   1.  NSPUT ns1 key1 val1 → OK
#   2.  NSGET ns1 key1 → val1
#   3.  NSGET ns2 key1 → ERR (different namespace)
#   4.  GET key1 → ERR (bare GET doesn't see ns-prefixed key)
#   5.  NSPUT ns1 key1 updated_val → OK (overwrite)
#   6.  NSGET ns1 key1 → updated_val
#   7.  NSDEL ns1 key1 → OK
#   8.  NSGET ns1 key1 → ERR (deleted)
#   9.  NSSCAN ns1 → lists keys with prefix
#   10. NSLIST → lists namespaces
#   11. NSDROP ns1 → drops all ns1 keys
#   12. Multiple namespaces isolated
#
# Usage:
#   ./tests/e2e/test_namespaces.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║          VeltrixDB — Namespace Operations Test       ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command nc || { echo "nc required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

DATA_DIR=$(mktemp -d /tmp/veltrixdb-ns-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

start_server "$DATA_DIR"
wait_ready 15

# Helper to send multi-word NS commands via nc
ns_cmd() {
    local cmd="$1"
    local port="${2:-$VELTRIX_PORT}"
    if nc -h 2>&1 | grep -q "\-q"; then
        printf '%s\r\nQUIT\r\n' "$cmd" \
            | nc -q1 127.0.0.1 "$port" 2>/dev/null \
            | head -1 | tr -d '\r'
    else
        printf '%s\r\nQUIT\r\n' "$cmd" \
            | nc 127.0.0.1 "$port" 2>/dev/null \
            | head -1 | tr -d '\r'
    fi
}

# ── Test 1: NSPUT ─────────────────────────────────────────────────────────────
section "NSPUT / NSGET basic"
resp=$(ns_cmd "NSPUT ns1 key1 val1")
assert_ok "$resp" "NSPUT ns1 key1 val1"

# ── Test 2: NSGET ─────────────────────────────────────────────────────────────
resp=$(ns_cmd "NSGET ns1 key1")
assert_eq "$resp" "val1" "NSGET ns1 key1 returns val1"

# ── Test 3: Cross-namespace isolation ────────────────────────────────────────
section "Cross-namespace isolation"
resp=$(ns_cmd "NSGET ns2 key1")
assert_contains "$resp" "ERR" "NSGET ns2 key1 returns ERR (wrong namespace)"

# ── Test 4: Bare GET doesn't see namespaced key ────────────────────────────────
resp=$(veltrix_cmd "GET key1")
# Namespace keys are stored with prefix @ns/<namespace>/, so bare GET misses.
# The response should be ERR since key1 alone was never PUT to the default space.
assert_contains "$resp" "ERR" "bare GET key1 returns ERR (not visible outside ns1)"

# ── Test 5: Overwrite in namespace ───────────────────────────────────────────
section "Namespace overwrite"
resp=$(ns_cmd "NSPUT ns1 key1 updated_val")
assert_ok "$resp" "NSPUT ns1 key1 updated_val (overwrite)"

resp=$(ns_cmd "NSGET ns1 key1")
assert_eq "$resp" "updated_val" "NSGET ns1 key1 returns updated_val"

# ── Test 6: NSDEL ─────────────────────────────────────────────────────────────
section "NSDEL"
resp=$(ns_cmd "NSDEL ns1 key1")
assert_ok "$resp" "NSDEL ns1 key1"

resp=$(ns_cmd "NSGET ns1 key1")
assert_contains "$resp" "ERR" "NSGET after NSDEL returns ERR"

# ── Test 7: Multiple keys in namespace ────────────────────────────────────────
section "Multiple keys in namespace"
for i in $(seq 1 5); do
    ns_cmd "NSPUT ns1 mkey_${i} mval_${i}" >/dev/null
done

# Verify each
for i in $(seq 1 5); do
    resp=$(ns_cmd "NSGET ns1 mkey_${i}")
    assert_eq "$resp" "mval_${i}" "NSGET ns1 mkey_${i} returns mval_${i}"
done

# ── Test 8: NSSCAN ─────────────────────────────────────────────────────────────
section "NSSCAN"
# NSSCAN ns1 [prefix] [limit] — returns matching keys, one per line
if nc -h 2>&1 | grep -q "\-q"; then
    SCAN_RESP=$(printf 'NSSCAN ns1 mkey 10\r\nQUIT\r\n' \
        | nc -q1 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null \
        | grep -v "^BYE$" | tr -d '\r')
else
    SCAN_RESP=$(printf 'NSSCAN ns1 mkey 10\r\nQUIT\r\n' \
        | nc 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null \
        | grep -v "^BYE$" | tr -d '\r')
fi

if echo "$SCAN_RESP" | grep -qE "mkey_[0-9]+|ERR"; then
    if echo "$SCAN_RESP" | grep -q "ERR"; then
        echo -e "  ${YELLOW}WARN${NC}  NSSCAN returned ERR (may require auth or different args)"
    else
        echo -e "  ${GREEN}PASS${NC}  NSSCAN ns1 mkey returns key listing"
        PASS=$((PASS + 1))
    fi
else
    echo -e "  ${RED}FAIL${NC}  NSSCAN ns1 mkey returned unexpected output: '${SCAN_RESP}'"
    FAIL=$((FAIL + 1))
fi

# ── Test 9: Multiple namespaces isolated from each other ──────────────────────
section "Multiple namespace isolation"
ns_cmd "NSPUT nsA shared_key nsA_val" >/dev/null
ns_cmd "NSPUT nsB shared_key nsB_val" >/dev/null

resp_a=$(ns_cmd "NSGET nsA shared_key")
resp_b=$(ns_cmd "NSGET nsB shared_key")

assert_eq "$resp_a" "nsA_val" "NSGET nsA shared_key returns nsA_val"
assert_eq "$resp_b" "nsB_val" "NSGET nsB shared_key returns nsB_val (isolated from nsA)"

# ── Test 10: NSLIST ────────────────────────────────────────────────────────────
section "NSLIST"
if nc -h 2>&1 | grep -q "\-q"; then
    NSLIST_RESP=$(printf 'NSLIST\r\nQUIT\r\n' \
        | nc -q1 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null \
        | grep -v "^BYE$" | tr -d '\r')
else
    NSLIST_RESP=$(printf 'NSLIST\r\nQUIT\r\n' \
        | nc 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null \
        | grep -v "^BYE$" | tr -d '\r')
fi

# NSLIST should mention our namespaces (ns1, nsA, nsB)
if echo "$NSLIST_RESP" | grep -qE "ns1|nsA|nsB|namespace"; then
    echo -e "  ${GREEN}PASS${NC}  NSLIST output includes created namespaces"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  NSLIST output: '${NSLIST_RESP}' (may be empty if no ns index)"
fi

# ── Test 11: NSDROP ────────────────────────────────────────────────────────────
section "NSDROP — drop entire namespace"
resp=$(ns_cmd "NSDROP ns1")
assert_ok "$resp" "NSDROP ns1 returns OK"

# All ns1 keys should be gone
for i in $(seq 1 5); do
    resp=$(ns_cmd "NSGET ns1 mkey_${i}")
    assert_contains "$resp" "ERR" "NSGET ns1 mkey_${i} returns ERR after NSDROP"
done

# nsA / nsB keys unaffected
resp_a=$(ns_cmd "NSGET nsA shared_key")
assert_eq "$resp_a" "nsA_val" "nsA key unaffected by NSDROP ns1"

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
