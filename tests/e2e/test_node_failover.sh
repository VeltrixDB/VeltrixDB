#!/usr/bin/env bash
# test_node_failover.sh — Node crash / restart and data durability.
#
# In single-node mode (no clustering flags) this tests:
#   1. Kill server (SIGKILL); restart; keys survive.
#   2. Write before kill; write more after restart; all keys accessible.
#   3. Multiple kill/restart cycles preserve data.
#
# For a real 3-node cluster scenario the servers would need --seeds flags
# and replication to be wired. That requires a running cluster overlay.
# This test focuses on the single-node durability contract which is the
# foundation of the crash-recovery guarantee.
#
# Usage:
#   ./tests/e2e/test_node_failover.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║       VeltrixDB — Node Failover / Restart Test       ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command nc || { echo "nc (netcat) required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

# ── Helper ────────────────────────────────────────────────────────────────────
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
# Test 1: Write before crash; verify after restart
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 1: 100 keys survive SIGKILL restart"

DATA_DIR=$(mktemp -d /tmp/veltrixdb-failover-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

start_server "$DATA_DIR"
wait_ready 15

echo "  Writing 100 keys..."
write_range "pre_crash" 1 100
sleep 0.5   # Let WAL flush window close

echo "  SIGKILL server..."
kill_server_hard
sleep 0.5

echo "  Restarting..."
start_server "$DATA_DIR"
wait_ready 15

echo "  Verifying 100 pre-crash keys..."
read -r ok fail <<< "$(verify_range "pre_crash" 1 100)"

if [ "$fail" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  all 100 pre-crash keys survived (${ok}/100)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  ${fail} keys lost (${ok}/100 survived)"
    FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: Write more after restart; all keys (pre + post) readable
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 2: Write 10 more keys after restart; all 110 readable"

echo "  Writing 10 more keys after restart..."
write_range "post_crash" 1 10
sleep 0.3

echo "  Verifying all 110 keys..."
read -r ok_pre fail_pre <<< "$(verify_range "pre_crash" 1 100)"
read -r ok_post fail_post <<< "$(verify_range "post_crash" 1 10)"

total_fail=$((fail_pre + fail_post))
total_ok=$((ok_pre + ok_post))

if [ "$total_fail" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  all 110 keys readable (${total_ok}/110)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  ${total_fail} keys missing (${total_ok}/110 readable)"
    FAIL=$((FAIL + 1))
fi

stop_server
sleep 0.3

# ═══════════════════════════════════════════════════════════════════════════════
# Test 3: Multiple kill/restart cycles
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 3: 3 kill/restart cycles preserve data"

DATA_DIR2=$(mktemp -d /tmp/veltrixdb-cycles-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR2")

start_server "$DATA_DIR2"
wait_ready 15

echo "  Writing 30 keys..."
write_range "cycle" 1 30
sleep 0.5

for cycle in 1 2 3; do
    echo "  Kill/restart cycle ${cycle}..."
    kill_server_hard
    sleep 0.3
    start_server "$DATA_DIR2"
    wait_ready 15

    read -r ok fail <<< "$(verify_range "cycle" 1 30)"
    if [ "$fail" -eq 0 ]; then
        echo -e "  ${GREEN}PASS${NC}  cycle ${cycle}: all 30 keys readable (${ok}/30)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC}  cycle ${cycle}: ${fail} keys missing (${ok}/30 survived)"
        FAIL=$((FAIL + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════════
# Test 4: INFO key count consistency after restart
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 4: INFO key count consistent after restart"

# At this point we still have the cycle data dir with ~30 keys.
# INFO should report keys >= 30.
resp=$(veltrix_cmd "INFO")
keys_val=$(echo "$resp" | grep -oE 'keys=[0-9]+' | head -1 | cut -d= -f2)

if [ -n "$keys_val" ] && [ "$keys_val" -ge 30 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  INFO reports keys=${keys_val} (>= 30) after restart"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  INFO reports keys=${keys_val:-unknown} (expected >= 30)"
    FAIL=$((FAIL + 1))
fi

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
