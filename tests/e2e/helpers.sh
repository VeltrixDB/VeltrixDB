#!/usr/bin/env bash
# helpers.sh — shared utilities for VeltrixDB e2e tests.
# Source this file at the top of every test script:
#   source "$(dirname "$0")/helpers.sh"
set -euo pipefail

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'   # No Color

# ── Env overrides ─────────────────────────────────────────────────────────────
VELTRIX_BIN="${VELTRIX_BIN:-/tmp/veltrixdb}"
LOADTEST_BIN="${LOADTEST_BIN:-/tmp/veltrix-loadtest}"
VELTRIX_PORT="${VELTRIX_PORT:-9000}"
METRICS_PORT="${METRICS_PORT:-2112}"
REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

# ── Counters ──────────────────────────────────────────────────────────────────
PASS=0
FAIL=0
_SERVER_PID=""
_TEMP_DIRS=()

# ── Netcat compatibility ───────────────────────────────────────────────────────
# Linux nc supports -q1; macOS BSD nc does not.
# We detect once and use a wrapper.
_nc_cmd() {
    local data="$1"
    local port="${2:-$VELTRIX_PORT}"
    if nc -h 2>&1 | grep -q "\-q"; then
        # GNU/Linux netcat
        printf '%s\r\n' "$data" | nc -q1 127.0.0.1 "$port" 2>/dev/null
    else
        # BSD netcat (macOS) — use timeout trick via /dev/tcp or just nc
        printf '%s\r\n' "$data" | nc 127.0.0.1 "$port" 2>/dev/null
    fi
}

# ── veltrix_cmd <command_string> ──────────────────────────────────────────────
# Send a single text-protocol command, return first line of response.
# Appends QUIT so nc closes cleanly.
veltrix_cmd() {
    local cmd="$1"
    local port="${2:-$VELTRIX_PORT}"
    local response
    if nc -h 2>&1 | grep -q "\-q"; then
        response=$(printf '%s\r\nQUIT\r\n' "$cmd" | nc -q1 127.0.0.1 "$port" 2>/dev/null | head -1 | tr -d '\r')
    else
        response=$(printf '%s\r\nQUIT\r\n' "$cmd" | nc 127.0.0.1 "$port" 2>/dev/null | head -1 | tr -d '\r')
    fi
    echo "$response"
}

# ── assert_eq <actual> <expected> <test_name> ─────────────────────────────────
assert_eq() {
    local actual="$1"
    local expected="$2"
    local name="$3"
    if [ "$actual" = "$expected" ]; then
        echo -e "  ${GREEN}PASS${NC}  $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC}  $name"
        echo -e "       expected: ${YELLOW}${expected}${NC}"
        echo -e "       actual:   ${YELLOW}${actual}${NC}"
        FAIL=$((FAIL + 1))
    fi
}

# ── assert_contains <string> <substring> <test_name> ─────────────────────────
assert_contains() {
    local str="$1"
    local sub="$2"
    local name="$3"
    if echo "$str" | grep -qF "$sub" 2>/dev/null; then
        echo -e "  ${GREEN}PASS${NC}  $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC}  $name"
        echo -e "       expected to contain: ${YELLOW}${sub}${NC}"
        echo -e "       actual string:        ${YELLOW}${str}${NC}"
        FAIL=$((FAIL + 1))
    fi
}

# ── assert_not_contains <string> <substring> <test_name> ─────────────────────
assert_not_contains() {
    local str="$1"
    local sub="$2"
    local name="$3"
    if echo "$str" | grep -qF "$sub" 2>/dev/null; then
        echo -e "  ${RED}FAIL${NC}  $name"
        echo -e "       expected NOT to contain: ${YELLOW}${sub}${NC}"
        echo -e "       actual string:            ${YELLOW}${str}${NC}"
        FAIL=$((FAIL + 1))
    else
        echo -e "  ${GREEN}PASS${NC}  $name"
        PASS=$((PASS + 1))
    fi
}

# ── assert_ok <response> <test_name> ─────────────────────────────────────────
assert_ok() {
    local response="$1"
    local name="$2"
    assert_eq "$response" "OK" "$name"
}

# ── assert_http_status <url> <expected_code> <test_name> ─────────────────────
assert_http_status() {
    local url="$1"
    local expected="$2"
    local name="$3"
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    assert_eq "$code" "$expected" "$name"
}

# ── start_server [data_dir] [extra_args...] ───────────────────────────────────
# Starts the server in background. Sets _SERVER_PID.
# data_dir defaults to a temp dir if not supplied.
start_server() {
    local data_dir="${1:-}"
    shift || true
    local extra_args=("$@")

    if [ -z "$data_dir" ]; then
        data_dir=$(mktemp -d /tmp/veltrixdb-test-XXXXXX)
        _TEMP_DIRS+=("$data_dir")
    fi

    # Always launch on the configured port; metrics on METRICS_PORT.
    "$VELTRIX_BIN" \
        -addr "127.0.0.1:${VELTRIX_PORT}" \
        -metrics-addr "127.0.0.1:${METRICS_PORT}" \
        -data "$data_dir" \
        -cache 64 \
        "${extra_args[@]}" \
        >/tmp/veltrixdb-server-stdout.log 2>/tmp/veltrixdb-server-stderr.log &
    _SERVER_PID=$!
    echo -e "  ${CYAN}INFO${NC}  server started  pid=${_SERVER_PID}  data=${data_dir}"
}

# ── stop_server ───────────────────────────────────────────────────────────────
stop_server() {
    if [ -n "$_SERVER_PID" ] && kill -0 "$_SERVER_PID" 2>/dev/null; then
        kill "$_SERVER_PID" 2>/dev/null || true
        wait "$_SERVER_PID" 2>/dev/null || true
    fi
    _SERVER_PID=""
}

# ── kill_server_hard ──────────────────────────────────────────────────────────
kill_server_hard() {
    if [ -n "$_SERVER_PID" ] && kill -0 "$_SERVER_PID" 2>/dev/null; then
        kill -9 "$_SERVER_PID" 2>/dev/null || true
        wait "$_SERVER_PID" 2>/dev/null || true
    fi
    _SERVER_PID=""
}

# ── wait_ready [timeout_secs] ─────────────────────────────────────────────────
# Blocks until the server responds to PING or timeout is reached.
wait_ready() {
    local timeout="${1:-15}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local resp
        resp=$(veltrix_cmd "PING" 2>/dev/null) || true
        if [ "$resp" = "PONG" ]; then
            return 0
        fi
        sleep 0.2
        elapsed=$((elapsed + 1))
    done
    echo -e "  ${RED}ERROR${NC}  server not ready after ${timeout}s — last server log:"
    tail -20 /tmp/veltrixdb-server-stderr.log 2>/dev/null || true
    return 1
}

# ── wait_metrics_ready [timeout_secs] ─────────────────────────────────────────
wait_metrics_ready() {
    local timeout="${1:-15}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${METRICS_PORT}/healthz" 2>/dev/null || echo "000")
        if [ "$code" = "200" ]; then
            return 0
        fi
        sleep 0.2
        elapsed=$((elapsed + 1))
    done
    return 1
}

# ── build_binary ──────────────────────────────────────────────────────────────
build_binary() {
    echo -e "  ${CYAN}BUILD${NC}  building server binary..."
    (cd "$REPO_ROOT" && go build -o "$VELTRIX_BIN" ./cmd/server) || {
        echo -e "  ${RED}ERROR${NC}  failed to build server"
        exit 1
    }
    echo -e "  ${GREEN}OK${NC}    built: $VELTRIX_BIN"
}

# ── build_loadtest ────────────────────────────────────────────────────────────
build_loadtest() {
    echo -e "  ${CYAN}BUILD${NC}  building loadtest binary..."
    (cd "$REPO_ROOT" && go build -o "$LOADTEST_BIN" ./cmd/loadtest) || {
        echo -e "  ${RED}ERROR${NC}  failed to build loadtest"
        exit 1
    }
    echo -e "  ${GREEN}OK${NC}    built: $LOADTEST_BIN"
}

# ── require_command <cmd> ─────────────────────────────────────────────────────
require_command() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo -e "  ${RED}SKIP${NC}  required command not found: $cmd"
        return 1
    fi
    return 0
}

# ── cleanup ───────────────────────────────────────────────────────────────────
# Called automatically via EXIT trap. Stops server and removes temp dirs.
cleanup() {
    stop_server
    for d in "${_TEMP_DIRS[@]:-}"; do
        rm -rf "$d" 2>/dev/null || true
    done
}

# ── print_summary ─────────────────────────────────────────────────────────────
print_summary() {
    local total=$((PASS + FAIL))
    echo ""
    echo "────────────────────────────────────────────"
    echo -e "  Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}  (${total} total)"
    if [ "$FAIL" -eq 0 ]; then
        echo -e "  ${GREEN}=== PASSED ===${NC}"
        echo "────────────────────────────────────────────"
        return 0
    else
        echo -e "  ${RED}=== FAILED ===${NC}"
        echo "────────────────────────────────────────────"
        return 1
    fi
}

# ── section <name> ────────────────────────────────────────────────────────────
section() {
    echo ""
    echo -e "${CYAN}▶ $1${NC}"
}

# Register cleanup on exit
trap cleanup EXIT
