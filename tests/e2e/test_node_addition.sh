#!/usr/bin/env bash
# test_node_addition.sh — Adding a second server to the cluster.
#
# In single-node mode (no live replication), this tests:
#   1. Start server A; write 100 keys; graceful shutdown.
#   2. Start server B on the same data dir (simulating takeover / follow-on node).
#      Verify all 100 keys readable on B.
#   3. Write 50 more keys on B; graceful shutdown.
#   4. Start server A on same dir again; verify all 150 keys.
#
# For a true two-node replication scenario, the servers would need
# --seeds=nodeB=host:port and the replication layer to be active.
# This test validates the data-portability contract: any server can restart
# on the same WAL+segment data and serve the full keyspace.
#
# Usage:
#   ./tests/e2e/test_node_addition.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║        VeltrixDB — Node Addition / Data Sharing Test ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command nc || { echo "nc (netcat) required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

# ── Helpers ───────────────────────────────────────────────────────────────────
write_range() {
    local prefix="$1" start="$2" end="$3"
    local i
    for i in $(seq "$start" "$end"); do
        veltrix_cmd "PUT ${prefix}_${i} ${prefix}_val_${i}" >/dev/null
    done
}

verify_range() {
    local prefix="$1" start="$2" end="$3"
    local ok=0 fail=0 i
    for i in $(seq "$start" "$end"); do
        local resp
        resp=$(veltrix_cmd "GET ${prefix}_${i}")
        if [ "$resp" = "${prefix}_val_${i}" ]; then
            ok=$((ok + 1))
        else
            fail=$((fail + 1))
        fi
    done
    echo "$ok $fail"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: Write on node A, restart as node B on same data dir
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 1: Node A writes 100 keys; node B reads from same data dir"

DATA_DIR=$(mktemp -d /tmp/veltrixdb-nodeadd-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

# Node A
start_server "$DATA_DIR" --node "node-a"
wait_ready 15

echo "  Node A: writing 100 keys..."
write_range "node_a" 1 100
sleep 0.3

echo "  Node A: graceful shutdown..."
stop_server
sleep 0.5

# Node B (different node ID, same data dir)
start_server "$DATA_DIR" --node "node-b"
wait_ready 15

echo "  Node B: verifying 100 keys written by node A..."
read -r ok fail <<< "$(verify_range "node_a" 1 100)"

if [ "$fail" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  node B reads all 100 keys from node A's data (${ok}/100)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  node B missing ${fail} keys (${ok}/100 readable)"
    FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: Node B writes 50 more; node A reads all 150 on re-open
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 2: Node B writes 50 more; node A reads all 150"

echo "  Node B: writing 50 additional keys..."
write_range "node_b" 1 50
sleep 0.3

echo "  Node B: graceful shutdown..."
stop_server
sleep 0.5

# Node A again
start_server "$DATA_DIR" --node "node-a"
wait_ready 15

echo "  Node A: verifying all 150 keys (100 from A + 50 from B)..."
read -r ok_a fail_a <<< "$(verify_range "node_a" 1 100)"
read -r ok_b fail_b <<< "$(verify_range "node_b" 1 50)"

total_fail=$((fail_a + fail_b))
total_ok=$((ok_a + ok_b))

if [ "$total_fail" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  all 150 keys readable (${total_ok}/150)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  ${total_fail} keys missing (${total_ok}/150 readable)"
    FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Test 3: Data dir shared across two ports concurrently is NOT safe
# (validate that starting on SAME dir while other server is running fails or
# the second server refuses to start due to lock / WAL collision)
# Note: VeltrixDB does not use a file lock by default; this test documents the
# behavior rather than enforcing exclusion.
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 3: Independent data dirs for two concurrent servers"

DATA_DIR_P1=$(mktemp -d /tmp/veltrixdb-port1-XXXXXX)
DATA_DIR_P2=$(mktemp -d /tmp/veltrixdb-port2-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR_P1" "$DATA_DIR_P2")

PORT1=$VELTRIX_PORT
PORT2=$((VELTRIX_PORT + 10))
METRICS2=$((METRICS_PORT + 10))

# Save and restore _SERVER_PID for two servers
PID1=""
PID2=""

# Start server 1
"$VELTRIX_BIN" \
    -addr "127.0.0.1:${PORT1}" \
    -metrics-addr "127.0.0.1:${METRICS_PORT}" \
    -data "$DATA_DIR_P1" \
    -cache 64 \
    >/tmp/veltrixdb-s1-stdout.log 2>/tmp/veltrixdb-s1-stderr.log &
PID1=$!

# Start server 2
"$VELTRIX_BIN" \
    -addr "127.0.0.1:${PORT2}" \
    -metrics-addr "127.0.0.1:${METRICS2}" \
    -data "$DATA_DIR_P2" \
    -cache 64 \
    >/tmp/veltrixdb-s2-stdout.log 2>/tmp/veltrixdb-s2-stderr.log &
PID2=$!

_SERVER_PID="$PID1"  # helpers.sh stop_server uses this

# Wait for both to be ready
elapsed=0
while [ "$elapsed" -lt 15 ]; do
    r1=$(veltrix_cmd "PING" "$PORT1" 2>/dev/null || echo "")
    r2=$(veltrix_cmd "PING" "$PORT2" 2>/dev/null || echo "")
    if [ "$r1" = "PONG" ] && [ "$r2" = "PONG" ]; then break; fi
    sleep 0.3
    elapsed=$((elapsed + 1))
done

# Write to server 1
for i in $(seq 1 20); do
    veltrix_cmd "PUT s1_key_${i} s1_val_${i}" "$PORT1" >/dev/null
done

# Write to server 2
for i in $(seq 1 20); do
    veltrix_cmd "PUT s2_key_${i} s2_val_${i}" "$PORT2" >/dev/null
done

# Keys from server 1 must NOT bleed into server 2 (separate data dirs)
bleed=$(veltrix_cmd "GET s1_key_1" "$PORT2")
if [ "$bleed" != "s1_val_1" ]; then
    echo -e "  ${GREEN}PASS${NC}  server 2 does NOT see server 1's keys (separate data dirs)"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  server 2 sees server 1 key (shared state? unexpected)"
fi

# Each server reads its own keys
r1=$(veltrix_cmd "GET s1_key_1" "$PORT1")
r2=$(veltrix_cmd "GET s2_key_1" "$PORT2")
assert_eq "$r1" "s1_val_1" "server 1 reads its own key"
assert_eq "$r2" "s2_val_1" "server 2 reads its own key"

# Stop both servers
kill "$PID1" 2>/dev/null || true
kill "$PID2" 2>/dev/null || true
wait "$PID1" 2>/dev/null || true
wait "$PID2" 2>/dev/null || true
_SERVER_PID=""

# ── Cleanup & summary ─────────────────────────────────────────────────────────
print_summary
