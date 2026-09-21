#!/usr/bin/env bash
# repair-scan-fleet.sh — assess value-transform metadata damage across a fleet.
#
# WHAT THIS IS FOR
#
# Builds that predate the value-transform WAL fix lost the compression and
# encryption flags on restart. Compression is on by default (zstd), so this
# affects default deployments for any value over the 256-byte threshold.
#
#   - After a CLEAN shutdown, reads SUCCEED and silently return the raw
#     compressed or encrypted blob. No error, no metric. This is the one to
#     worry about.
#   - After a CRASH, reads FAIL with a CRC32C mismatch.
#
# Upgrading does not repair existing data — the metadata was never written
# down. This script tells you which nodes are affected and how badly.
#
# WHAT IT DOES NOT DO
#
# It only SCANS. It never writes. Repair is a separate, deliberate step
# (veltrix-repair --repair), run per node with the server stopped.
#
# USAGE
#
#   # Scan every node listed in a hosts file (one "user@host" per line)
#   ./scripts/repair-scan-fleet.sh --hosts hosts.txt --data-dirs /mnt/nvme0,/mnt/nvme1
#
#   # Scan the local node only
#   ./scripts/repair-scan-fleet.sh --local --data-dirs /mnt/nvme0
#
#   # Encrypted deployments: the key must be reachable or encrypted records
#   # cannot be resolved and will be reported as unresolved.
#   VELTRIXDB_ENCRYPTION_KEY=... ./scripts/repair-scan-fleet.sh --local --encrypt ...
#
# IMPORTANT: veltrix-repair opens the data directory. The server on that node
# must be STOPPED before scanning it. This script refuses to run against a
# host where the port is still accepting connections unless --force is given.
set -uo pipefail

HOSTS_FILE=""
DATA_DIRS=""
LOCAL=false
ENCRYPT=false
KEY_PATH=""
FORCE=false
PORT=9000
BIN="${VELTRIX_REPAIR_BIN:-./veltrix-repair}"

usage() { sed -n '2,40p' "$0"; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts)      HOSTS_FILE="$2"; shift 2 ;;
    --data-dirs)  DATA_DIRS="$2";  shift 2 ;;
    --local)      LOCAL=true;      shift ;;
    --encrypt)    ENCRYPT=true;    shift ;;
    --key-path)   KEY_PATH="$2";   shift 2 ;;
    --port)       PORT="$2";       shift 2 ;;
    --bin)        BIN="$2";        shift 2 ;;
    --force)      FORCE=true;      shift ;;
    -h|--help)    usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage 1 ;;
  esac
done

[[ -z "$DATA_DIRS" ]] && { echo "error: --data-dirs is required" >&2; usage 1; }
if [[ "$LOCAL" == false && -z "$HOSTS_FILE" ]]; then
  echo "error: pass --local or --hosts <file>" >&2; usage 1
fi

REPAIR_ARGS=(--data-dirs "$DATA_DIRS" --scan)
[[ "$ENCRYPT" == true ]] && REPAIR_ARGS+=(--encrypt-at-rest)
[[ -n "$KEY_PATH" ]] && REPAIR_ARGS+=(--encryption-key-path "$KEY_PATH")

# Exit codes from cmd/veltrix-repair.
declare -r EXIT_CLEAN=0 EXIT_NEEDS_WORK=1 EXIT_UNRESOLVED=2

affected=(); clean=(); unresolved=(); failed=()

scan_one() {
  local label="$1"; shift
  local out rc
  out="$("$@" 2>&1)"; rc=$?
  local summary
  summary="$(grep -E '^scan \(dry run\):' <<<"$out" || true)"
  [[ -z "$summary" ]] && summary="$(tail -3 <<<"$out")"

  case "$rc" in
    "$EXIT_CLEAN")       clean+=("$label");      printf '  %-28s CLEAN\n' "$label" ;;
    "$EXIT_NEEDS_WORK")  affected+=("$label");   printf '  %-28s NEEDS REPAIR\n' "$label" ;;
    "$EXIT_UNRESOLVED")  unresolved+=("$label"); printf '  %-28s UNRESOLVED ENTRIES\n' "$label" ;;
    *)                   failed+=("$label");     printf '  %-28s SCAN FAILED (rc=%d)\n' "$label" "$rc" ;;
  esac
  [[ -n "$summary" ]] && printf '      %s\n' "$summary"
}

port_open() { # host -> 0 if something is listening
  local h="$1"
  if [[ "$h" == "localhost" ]]; then
    nc -z 127.0.0.1 "$PORT" >/dev/null 2>&1
  else
    ssh -o BatchMode=yes -o ConnectTimeout=5 "$h" "nc -z 127.0.0.1 $PORT" >/dev/null 2>&1
  fi
}

echo "=== VeltrixDB value-transform scan (read-only) ==="
echo "data-dirs: $DATA_DIRS"
echo

if [[ "$LOCAL" == true ]]; then
  if port_open localhost && [[ "$FORCE" == false ]]; then
    echo "  localhost                    SKIPPED — server still listening on :$PORT"
    echo "      Stop it first, or pass --force if you know it is not this cluster."
    failed+=("localhost")
  else
    scan_one "localhost" "$BIN" "${REPAIR_ARGS[@]}"
  fi
else
  while read -r host; do
    [[ -z "$host" || "$host" == \#* ]] && continue
    if port_open "$host" && [[ "$FORCE" == false ]]; then
      echo "  $(printf '%-28s' "$host") SKIPPED — server still listening on :$PORT"
      failed+=("$host")
      continue
    fi
    scan_one "$host" ssh -o BatchMode=yes -o ConnectTimeout=10 "$host" \
      "$BIN $(printf '%q ' "${REPAIR_ARGS[@]}")"
  done < "$HOSTS_FILE"
fi

echo
echo "=== summary ==="
printf 'clean:            %d\n' "${#clean[@]}"
printf 'need repair:      %d %s\n' "${#affected[@]}"   "${affected[*]:-}"
printf 'unresolved:       %d %s\n' "${#unresolved[@]}" "${unresolved[*]:-}"
printf 'scan failed:      %d %s\n' "${#failed[@]}"     "${failed[*]:-}"
echo

if (( ${#unresolved[@]} > 0 )); then
  cat <<'MSG'
UNRESOLVED entries were found. The repair could not reconstruct a plaintext
matching the entry's stored CRC. Either:
  - the data is encrypted and the scan ran without the key (re-run with
    --encrypt and the correct key), or
  - those records are genuinely corrupt and need a restore from backup.
MSG
fi

if (( ${#affected[@]} > 0 )); then
  cat <<'MSG'
To repair an affected node (server STOPPED; each wal.log is backed up first):

  veltrix-repair --data-dirs <dirs> --repair

Then restart and spot-check a few large values end to end.
MSG
fi

# Non-zero if anything needs attention, so this can gate a pipeline.
(( ${#affected[@]} + ${#unresolved[@]} + ${#failed[@]} > 0 )) && exit 1
exit 0
