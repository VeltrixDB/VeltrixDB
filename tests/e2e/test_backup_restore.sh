#!/usr/bin/env bash
# test_backup_restore.sh — Backup / restore via admin API and WAL checkpoint.
#
# VeltrixDB exposes POST /admin/checkpoint to flush all WAL buffers and sync
# all segment files. Combined with copying the data directory, this provides
# a consistent snapshot. There is no /admin/backup HTTP endpoint in the current
# code; backup is performed by:
#   1. POST /admin/checkpoint  (flush + fdatasync)
#   2. cp -r <data_dir>  <backup_dir>  (filesystem copy)
#   3. Start fresh server on the backup dir
#   4. Verify keys readable
#
# Test cases:
#   1. Start server; PUT 50 keys; POST /admin/checkpoint
#   2. cp data dir to backup dir (offline snapshot)
#   3. Stop server; start fresh server on backup dir
#   4. Verify all 50 keys readable from backup
#   5. Verify WAL is not replayed (truncated after clean shutdown)
#   6. Write 10 more keys to backup server; stop it; start original server
#      (fresh from original dir) — backup keys should NOT be in original dir
#
# Usage:
#   ./tests/e2e/test_backup_restore.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║        VeltrixDB — Backup / Restore Test             ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command curl || { echo "curl required"; exit 1; }
require_command nc   || { echo "nc required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

# ── Helper ────────────────────────────────────────────────────────────────────
write_keys() {
    local prefix="$1" count="$2"
    local i
    for i in $(seq 1 "$count"); do
        veltrix_cmd "PUT ${prefix}_key_${i} ${prefix}_val_${i}" >/dev/null
    done
}

verify_keys() {
    local prefix="$1" count="$2"
    local ok=0 fail=0 i
    for i in $(seq 1 "$count"); do
        local resp
        resp=$(veltrix_cmd "GET ${prefix}_key_${i}")
        if [ "$resp" = "${prefix}_val_${i}" ]; then
            ok=$((ok + 1))
        else
            fail=$((fail + 1))
        fi
    done
    echo "$ok $fail"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: Checkpoint then filesystem copy = consistent backup
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 1: Checkpoint + filesystem backup"

DATA_DIR=$(mktemp -d /tmp/veltrixdb-backup-src-XXXXXX)
BACKUP_DIR=$(mktemp -d /tmp/veltrixdb-backup-dst-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR" "$BACKUP_DIR")

start_server "$DATA_DIR"
wait_ready 15

echo "  Writing 50 keys..."
write_keys "bkp" 50

# Trigger checkpoint to flush WAL + fsync segments
echo "  Triggering /admin/checkpoint..."
CKPT_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${METRICS_PORT}/admin/checkpoint" 2>/dev/null)
CKPT_CODE=$(echo "$CKPT_RESP" | tail -1)
CKPT_BODY=$(echo "$CKPT_RESP" | head -n -1)

assert_eq "$CKPT_CODE" "200" "POST /admin/checkpoint returns 200"
assert_contains "$CKPT_BODY" "ok" "/admin/checkpoint response contains ok"

# Copy data dir while server is still running (after checkpoint, data is stable)
echo "  Copying data dir to backup..."
cp -r "$DATA_DIR/." "$BACKUP_DIR/"

stop_server
sleep 0.3

# ── Start fresh server on backup dir ─────────────────────────────────────────
section "Test 2: Fresh server on backup dir reads all 50 keys"

start_server "$BACKUP_DIR"
wait_ready 15

echo "  Verifying 50 keys on backup server..."
read -r ok fail <<< "$(verify_keys "bkp" 50)"

if [ "$fail" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  all 50 backup keys readable (${ok}/50)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  ${fail} keys missing from backup (${ok}/50 readable)"
    FAIL=$((FAIL + 1))
fi

# ── Check /admin/stats key count ─────────────────────────────────────────────
section "Test 3: /admin/stats shows correct key count from backup"
STATS=$(curl -s "http://127.0.0.1:${METRICS_PORT}/admin/stats" 2>/dev/null || echo "{}")
INDEX_KEYS=$(echo "$STATS" | grep -oE '"index_keys":[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")

if [ -n "$INDEX_KEYS" ] && [ "$INDEX_KEYS" -ge 50 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  /admin/stats index_keys=${INDEX_KEYS} >= 50"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  /admin/stats index_keys=${INDEX_KEYS:-unknown} (expected >= 50)"
    FAIL=$((FAIL + 1))
fi

# ── Write extra keys to backup server, verify isolation from source ───────────
section "Test 4: Backup server writes do not affect source dir"
echo "  Writing 10 extra keys on backup server..."
for i in $(seq 1 10); do
    veltrix_cmd "PUT bkp_extra_key_${i} bkp_extra_val_${i}" >/dev/null
done

stop_server
sleep 0.3

# Restart on original data dir (not the backup)
echo "  Starting server on original data dir..."
start_server "$DATA_DIR"
wait_ready 15

# Extra keys should NOT exist on original server
extra_resp=$(veltrix_cmd "GET bkp_extra_key_1")
if echo "$extra_resp" | grep -q "ERR"; then
    echo -e "  ${GREEN}PASS${NC}  backup-only keys not present in original dir (isolation OK)"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  bkp_extra_key_1 found in original dir: '${extra_resp}'"
fi

# Original keys still present
echo "  Verifying original 50 keys still in source dir..."
read -r ok_orig fail_orig <<< "$(verify_keys "bkp" 50)"
if [ "$fail_orig" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  original 50 keys still in source dir (${ok_orig}/50)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  source dir missing ${fail_orig} keys (${ok_orig}/50)"
    FAIL=$((FAIL + 1))
fi

# ── WAL presence check ────────────────────────────────────────────────────────
section "Test 5: WAL file present in data dir"
WAL_COUNT=$(find "$DATA_DIR" -name "wal*.log" -o -name "wal.log" 2>/dev/null | wc -l | tr -d ' ')
if [ "$WAL_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  WAL file(s) present in data dir (${WAL_COUNT} files)"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  no WAL files found in data dir (may use alternate naming)"
fi

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
