#!/usr/bin/env bash
# test_metrics.sh — Prometheus metrics and HTTP health endpoint tests.
#
# Test cases:
#   1. /healthz → HTTP 200
#   2. /readyz  → HTTP 200
#   3. /metrics → HTTP 200, content-type text/plain
#   4. /metrics contains expected metric names
#   5. PUT 100 keys; veltrixdb_storage_puts_total increases
#   6. /admin/stats → JSON with index_keys field
#   7. /admin/version → JSON with current_schema_version field
#
# Usage:
#   ./tests/e2e/test_metrics.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║         VeltrixDB — Metrics & Health Test            ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command curl || { echo "curl required"; exit 1; }
require_command nc   || { echo "nc (netcat) required"; exit 1; }

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

DATA_DIR=$(mktemp -d /tmp/veltrixdb-metrics-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

start_server "$DATA_DIR"
wait_ready 15
wait_metrics_ready 15

METRICS_BASE="http://127.0.0.1:${METRICS_PORT}"
ADMIN_BASE="http://127.0.0.1:${METRICS_PORT}/admin"

# ── /healthz ──────────────────────────────────────────────────────────────────
section "/healthz"
assert_http_status "${METRICS_BASE}/healthz" "200" "GET /healthz returns HTTP 200"

# ── /readyz ───────────────────────────────────────────────────────────────────
section "/readyz"
# /readyz may return 503 if engine is still warming up; retry briefly
readyz_code="000"
for _attempt in 1 2 3 4 5; do
    readyz_code=$(curl -s -o /dev/null -w "%{http_code}" "${METRICS_BASE}/readyz" 2>/dev/null || echo "000")
    if [ "$readyz_code" = "200" ]; then break; fi
    sleep 1
done
assert_http_status "${METRICS_BASE}/readyz" "200" "GET /readyz returns HTTP 200"

# ── /metrics ──────────────────────────────────────────────────────────────────
section "/metrics"
METRICS_RESP=$(curl -s -w "\n%{http_code}" "${METRICS_BASE}/metrics" 2>/dev/null)
METRICS_BODY=$(echo "$METRICS_RESP" | head -n -1)
METRICS_CODE=$(echo "$METRICS_RESP" | tail -1)

assert_eq "$METRICS_CODE" "200" "GET /metrics returns HTTP 200"

# Content-type check
CONTENT_TYPE=$(curl -s -o /dev/null -w "%{content_type}" "${METRICS_BASE}/metrics" 2>/dev/null || echo "")
if echo "$CONTENT_TYPE" | grep -q "text/plain"; then
    echo -e "  ${GREEN}PASS${NC}  /metrics content-type is text/plain"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  /metrics content-type: ${CONTENT_TYPE}"
    FAIL=$((FAIL + 1))
fi

# Known metric names
for metric_name in \
    "veltrixdb_storage_puts_total" \
    "veltrixdb_storage_gets_total" \
    "veltrixdb_storage_deletes_total" \
    "veltrixdb_cache_hits_total" \
    "veltrixdb_cache_misses_total"; do
    assert_contains "$METRICS_BODY" "$metric_name" "/metrics contains ${metric_name}"
done

# ── Metric counter increases after PUT ────────────────────────────────────────
section "PUT counter increment"

# Read baseline puts_total
baseline_raw=$(echo "$METRICS_BODY" | grep "^veltrixdb_storage_puts_total " | awk '{print $2}' | head -1)
baseline="${baseline_raw:-0}"

# Do 100 PUTs
for i in $(seq 1 100); do
    veltrix_cmd "PUT metric_key_${i} val_${i}" >/dev/null
done

# Allow metrics to update
sleep 0.5

NEW_METRICS=$(curl -s "${METRICS_BASE}/metrics" 2>/dev/null)
new_raw=$(echo "$NEW_METRICS" | grep "^veltrixdb_storage_puts_total " | awk '{print $2}' | head -1)
new_val="${new_raw:-0}"

# Convert to integers (may be floats like 100.0)
baseline_int=$(printf "%.0f" "$baseline" 2>/dev/null || echo 0)
new_int=$(printf "%.0f" "$new_val" 2>/dev/null || echo 0)

if [ "$new_int" -gt "$baseline_int" ] 2>/dev/null; then
    delta=$((new_int - baseline_int))
    echo -e "  ${GREEN}PASS${NC}  veltrixdb_storage_puts_total increased by ${delta} after 100 PUTs"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  veltrixdb_storage_puts_total did not increase (baseline=${baseline_int}, new=${new_int})"
    FAIL=$((FAIL + 1))
fi

# ── /admin/stats ─────────────────────────────────────────────────────────────
section "/admin/stats"
STATS_RESP=$(curl -s -w "\n%{http_code}" "${ADMIN_BASE}/stats" 2>/dev/null)
STATS_BODY=$(echo "$STATS_RESP" | head -n -1)
STATS_CODE=$(echo "$STATS_RESP" | tail -1)

assert_eq "$STATS_CODE" "200" "GET /admin/stats returns HTTP 200"
assert_contains "$STATS_BODY" "index_keys" "/admin/stats contains index_keys"
assert_contains "$STATS_BODY" "writes_total" "/admin/stats contains writes_total"

# ── /admin/version ────────────────────────────────────────────────────────────
section "/admin/version"
VERSION_RESP=$(curl -s -w "\n%{http_code}" "${ADMIN_BASE}/version" 2>/dev/null)
VERSION_BODY=$(echo "$VERSION_RESP" | head -n -1)
VERSION_CODE=$(echo "$VERSION_RESP" | tail -1)

assert_eq "$VERSION_CODE" "200" "GET /admin/version returns HTTP 200"
assert_contains "$VERSION_BODY" "current_schema_version" "/admin/version contains current_schema_version"

# ── /admin/checkpoint ────────────────────────────────────────────────────────
section "/admin/checkpoint"
CKPT_RESP=$(curl -s -w "\n%{http_code}" -X POST "${ADMIN_BASE}/checkpoint" 2>/dev/null)
CKPT_CODE=$(echo "$CKPT_RESP" | tail -1)
CKPT_BODY=$(echo "$CKPT_RESP" | head -n -1)

assert_eq "$CKPT_CODE" "200" "POST /admin/checkpoint returns HTTP 200"
assert_contains "$CKPT_BODY" "ok" "/admin/checkpoint body contains ok"

# ── /admin/quotas ─────────────────────────────────────────────────────────────
section "/admin/quotas"
QUOTAS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${ADMIN_BASE}/quotas" 2>/dev/null || echo "000")
assert_eq "$QUOTAS_CODE" "200" "GET /admin/quotas returns HTTP 200"

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
