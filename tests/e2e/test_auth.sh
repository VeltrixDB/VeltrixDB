#!/usr/bin/env bash
# test_auth.sh — Authentication and RBAC tests.
#
# Uses the --auth-config flag (JSON file). The auth system requires
# AUTH <user> <password> before any command when a config is loaded.
#
# Hash format: sha256(password + username) — see security/auth.go.
#
# Test cases:
#   1. Server with no auth-config: no AUTH needed; PUT/GET work immediately.
#   2. Server with auth-config: unauthenticated PUT returns ERR.
#   3. AUTH with wrong password → ERR.
#   4. AUTH with correct credentials → OK; subsequent PUT/GET succeed.
#   5. Readonly user can GET but PUT returns ERR.
#   6. Admin user can PUT, GET, DEL, INFO.
#
# Usage:
#   ./tests/e2e/test_auth.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║         VeltrixDB — Authentication / RBAC Test       ║"
echo "╚══════════════════════════════════════════════════════╝"

require_command nc     || { echo "nc required"; exit 1; }
require_command python3 || require_command python || {
    echo -e "  ${YELLOW}SKIP${NC}  python not available for hash generation"
    exit 0
}

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi

# ── Hash generation helper ────────────────────────────────────────────────────
# sha256hex(password + username) — matches security/auth.go
hash_password() {
    local password="$1"
    local username="$2"
    local input="${password}${username}"
    if command -v python3 >/dev/null 2>&1; then
        python3 -c "import hashlib,sys; print(hashlib.sha256('${input}'.encode()).hexdigest())"
    else
        python -c "import hashlib; print(hashlib.sha256('${input}'.encode()).hexdigest())"
    fi
}

# ── Auth config builder ───────────────────────────────────────────────────────
AUTH_CONF_DIR=$(mktemp -d /tmp/veltrixdb-auth-XXXXXX)
_TEMP_DIRS+=("$AUTH_CONF_DIR")

ADMIN_HASH=$(hash_password "adminpass" "admin")
RO_HASH=$(hash_password "readpass" "reader")
RW_HASH=$(hash_password "rwpass" "rwuser")

AUTH_CONF="$AUTH_CONF_DIR/auth.json"
cat > "$AUTH_CONF" <<EOF
{
  "users": [
    {
      "username": "admin",
      "password_hash": "${ADMIN_HASH}",
      "role": "admin"
    },
    {
      "username": "reader",
      "password_hash": "${RO_HASH}",
      "role": "readonly"
    },
    {
      "username": "rwuser",
      "password_hash": "${RW_HASH}",
      "role": "readwrite"
    }
  ]
}
EOF

# ── Multi-command helper (AUTH + command in one TCP session) ──────────────────
auth_cmd() {
    local user="$1"
    local pass="$2"
    local cmd="$3"
    local port="${4:-$VELTRIX_PORT}"
    if nc -h 2>&1 | grep -q "\-q"; then
        printf '%s\r\n%s\r\nQUIT\r\n' "AUTH ${user} ${pass}" "$cmd" \
            | nc -q1 127.0.0.1 "$port" 2>/dev/null \
            | grep -v "^OK$\|^BYE$" | head -1 | tr -d '\r'
    else
        printf '%s\r\n%s\r\nQUIT\r\n' "AUTH ${user} ${pass}" "$cmd" \
            | nc 127.0.0.1 "$port" 2>/dev/null \
            | grep -v "^OK$\|^BYE$" | head -1 | tr -d '\r'
    fi
}

# ── unauthenticated_cmd — send command WITHOUT auth ───────────────────────────
unauth_cmd() {
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

# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: No auth-config — commands work without AUTH
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 1: No auth-config — unauthenticated access allowed"

DATA_DIR=$(mktemp -d /tmp/veltrixdb-noauth-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

start_server "$DATA_DIR"
wait_ready 15

resp=$(veltrix_cmd "PUT noauth_key noauth_val")
assert_ok "$resp" "PUT without auth (no config) returns OK"

resp=$(veltrix_cmd "GET noauth_key")
assert_eq "$resp" "noauth_val" "GET without auth returns value"

stop_server
sleep 0.3

# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: auth-config present — unauthenticated commands return ERR
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 2: auth-config — unauthenticated PUT returns ERR"

DATA_DIR2=$(mktemp -d /tmp/veltrixdb-auth-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR2")

start_server "$DATA_DIR2" --auth-config "$AUTH_CONF"
wait_ready 15

resp=$(unauth_cmd "PUT protected_key value1")
assert_contains "$resp" "ERR" "unauthenticated PUT returns ERR when auth configured"

resp=$(unauth_cmd "GET protected_key")
assert_contains "$resp" "ERR" "unauthenticated GET returns ERR when auth configured"

# ═══════════════════════════════════════════════════════════════════════════════
# Test 3: Wrong password → ERR
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 3: Wrong password returns ERR"
# We check the AUTH response directly
if nc -h 2>&1 | grep -q "\-q"; then
    auth_resp=$(printf 'AUTH admin wrongpassword\r\nQUIT\r\n' \
        | nc -q1 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null \
        | head -1 | tr -d '\r')
else
    auth_resp=$(printf 'AUTH admin wrongpassword\r\nQUIT\r\n' \
        | nc 127.0.0.1 "$VELTRIX_PORT" 2>/dev/null \
        | head -1 | tr -d '\r')
fi
assert_contains "$auth_resp" "ERR" "AUTH with wrong password returns ERR"

# ═══════════════════════════════════════════════════════════════════════════════
# Test 4: Correct credentials → subsequent PUT/GET succeed
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 4: Correct credentials (admin) allow PUT/GET"

# Admin writes a key
resp=$(auth_cmd "admin" "adminpass" "PUT auth_key auth_val")
assert_ok "$resp" "admin PUT returns OK"

resp=$(auth_cmd "admin" "adminpass" "GET auth_key")
assert_eq "$resp" "auth_val" "admin GET returns correct value"

resp=$(auth_cmd "admin" "adminpass" "DEL auth_key")
assert_ok "$resp" "admin DEL returns OK"

# ═══════════════════════════════════════════════════════════════════════════════
# Test 5: Readonly user — GET OK, PUT ERR
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 5: Readonly user: GET succeeds, PUT fails"

# First, admin writes a key that reader can read
auth_cmd "admin" "adminpass" "PUT ro_test_key ro_test_val" >/dev/null

# Reader can GET
resp=$(auth_cmd "reader" "readpass" "GET ro_test_key")
assert_eq "$resp" "ro_test_val" "readonly user GET returns value"

# Reader cannot PUT
resp=$(auth_cmd "reader" "readpass" "PUT ro_test_key new_val")
assert_contains "$resp" "ERR" "readonly user PUT returns ERR"

# Reader cannot DEL
resp=$(auth_cmd "reader" "readpass" "DEL ro_test_key")
assert_contains "$resp" "ERR" "readonly user DEL returns ERR"

# ═══════════════════════════════════════════════════════════════════════════════
# Test 6: Readwrite user — can PUT and GET
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 6: Readwrite user: PUT and GET succeed"

resp=$(auth_cmd "rwuser" "rwpass" "PUT rw_key rw_val")
assert_ok "$resp" "readwrite user PUT returns OK"

resp=$(auth_cmd "rwuser" "rwpass" "GET rw_key")
assert_eq "$resp" "rw_val" "readwrite user GET returns value"

# ═══════════════════════════════════════════════════════════════════════════════
# Test 7: PING is always allowed (no auth check on PING)
# ═══════════════════════════════════════════════════════════════════════════════
section "Test 7: PING allowed without auth"
resp=$(unauth_cmd "PING")
assert_eq "$resp" "PONG" "PING returns PONG without auth"

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
