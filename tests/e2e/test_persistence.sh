#!/usr/bin/env bash
# test_persistence.sh — WAL replay / crash recovery tests.
#
# Test cases:
#   1. Start server; PUT 50 keys; SIGKILL; restart same data dir; verify 50 keys.
#   2. Graceful shutdown: PUT 50 keys; SIGTERM; restart; verify keys.
#
# Usage:
#   ./tests/e2e/test_persistence.sh
#   VELTRIX_PORT=9001 ./tests/e2e/test_persistence.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║         VeltrixDB — Persistence / WAL Replay Test    ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command nc || { echo "nc (netcat) required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

# ── Helper: write N keys ──────────────────────────────────────────────────────
write_keys() {
    local prefix="$1"
    local count="$2"
    local port="${3:-$VELTRIX_PORT}"
    local i
    for i in $(seq 1 "$count"); do
        veltrix_cmd "PUT ${prefix}_key_${i} ${prefix}_val_${i}" "$port" >/dev/null
    done
}

# ── Helper: verify N keys ─────────────────────────────────────────────────────
verify_keys() {
    local prefix="$1"
    local count="$2"
    local port="${3:-$VELTRIX_PORT}"
    local ok=0 fail=0 i
    for i in $(seq 1 "$count"); do
        local resp
        resp=$(veltrix_cmd "GET ${prefix}_key_${i}" "$port")
        if [ "$resp" = "${prefix}_val_${i}" ]; then
            ok=$((ok + 1))
        else
            fail=$((fail + 1))
        fi
    done
    echo "$ok $fail"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: Crash recovery (SIGKILL)
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 1: Crash recovery (SIGKILL)"

DATA_DIR=$(mktemp -d /tmp/veltrixdb-persist-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

start_server "$DATA_DIR"
wait_ready 15

echo "  Writing 50 keys before crash..."
write_keys "crash" 50

# Give WAL a moment to flush (default 15ms window)
sleep 0.5

echo "  Sending SIGKILL to simulate crash..."
kill_server_hard

# Brief pause so OS releases port
sleep 0.5

echo "  Restarting server on same data dir..."
start_server "$DATA_DIR"
wait_ready 15

echo "  Verifying 50 keys survived crash..."
read -r ok_count fail_count <<< "$(verify_keys "crash" 50)"

if [ "$fail_count" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  crash recovery: all 50 keys readable (${ok_count}/50)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  crash recovery: ${fail_count} keys missing after SIGKILL (${ok_count}/50 survived)"
    FAIL=$((FAIL + 1))
fi

stop_server
sleep 0.3

# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: Graceful shutdown (SIGTERM)
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 2: Graceful shutdown (SIGTERM)"

DATA_DIR2=$(mktemp -d /tmp/veltrixdb-graceful-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR2")

start_server "$DATA_DIR2"
wait_ready 15

echo "  Writing 50 keys before graceful shutdown..."
write_keys "graceful" 50

sleep 0.3

echo "  Sending SIGTERM for graceful shutdown..."
stop_server
sleep 0.5

echo "  Restarting server on same data dir..."
start_server "$DATA_DIR2"
wait_ready 15

echo "  Verifying 50 keys survived graceful shutdown..."
read -r ok_count fail_count <<< "$(verify_keys "graceful" 50)"

if [ "$fail_count" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  graceful recovery: all 50 keys readable (${ok_count}/50)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  graceful recovery: ${fail_count} keys missing after restart (${ok_count}/50 survived)"
    FAIL=$((FAIL + 1))
fi

stop_server
sleep 0.3

# ═══════════════════════════════════════════════════════════════════════════════
# Test 3: Multiple restarts preserve data
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 3: Multiple restarts preserve data"

DATA_DIR3=$(mktemp -d /tmp/veltrixdb-multirestart-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR3")

start_server "$DATA_DIR3"
wait_ready 15

echo "  Writing 20 keys..."
write_keys "multi" 20
sleep 0.3

# Restart 3 times
for restart_num in 1 2 3; do
    echo "  Restart #${restart_num}..."
    stop_server
    sleep 0.3
    start_server "$DATA_DIR3"
    wait_ready 15
done

echo "  Verifying 20 keys after 3 restarts..."
read -r ok_count fail_count <<< "$(verify_keys "multi" 20)"

if [ "$fail_count" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  multi-restart: all 20 keys readable (${ok_count}/20)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  multi-restart: ${fail_count} keys missing (${ok_count}/20 survived)"
    FAIL=$((FAIL + 1))
fi

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
