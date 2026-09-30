#!/usr/bin/env bash
# compare.sh — YCSB (and optionally vector) comparison of VeltrixDB,
# Aerospike and ScyllaDB on ONE machine, one database at a time.
#
#   ./compare.sh                          # all three, workloads a b c f
#   DBS="veltrixdb scylla" WORKLOADS="a b" RECORDS=5000000 ./compare.sh
#   START=0 VELTRIX_ADDR=10.0.0.5:9000 ./compare.sh   # databases already running
#
# Env:
#   DBS          veltrixdb aerospike scylla        WORKLOADS  a b c f
#   RECORDS      1000000     OPS  1000000           THREADS    64
#   START        1 = docker compose up/down each database in turn (default)
#   OUT          results/<timestamp>
#   VECTORS      1 = also run vecbench on VeltrixDB (needs ANN_DIR, see
#                scripts/ann-dataset.py); no Aerospike / Scylla vector driver
#                is included (see cmd/vecbench)
#
# Writes OUT/<db>-<workload>.txt (raw go-ycsb output), OUT/summary.md and
# OUT/environment.txt. Nothing here makes numbers comparable by itself: use
# the same machine, the same disk, the same durability setting (README).
set -euo pipefail
cd "$(dirname "$0")"

DBS="${DBS:-veltrixdb aerospike scylla}"
WORKLOADS="${WORKLOADS:-a b c f}"
RECORDS="${RECORDS:-1000000}"
OPS="${OPS:-1000000}"
THREADS="${THREADS:-64}"
START="${START:-1}"
OUT="${OUT:-results/$(date -u +%Y%m%dT%H%M%SZ)}"
VELTRIX_ADDR="${VELTRIX_ADDR:-127.0.0.1:9000}"
AEROSPIKE_HOST="${AEROSPIKE_HOST:-127.0.0.1}"
SCYLLA_HOSTS="${SCYLLA_HOSTS:-127.0.0.1}"
mkdir -p "$OUT"

go build -o "$OUT/ycsb" ./cmd/ycsb

{
  echo "date: $(date -u +%FT%TZ)"
  echo "host: $(hostname)"
  uname -a
  (command -v lscpu >/dev/null && lscpu | grep -E 'Model name|^CPU\(s\)') || sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  (command -v free >/dev/null && free -g | head -2) || true
  df -h "${BENCH_DATA:-./data}" 2>/dev/null || true
  echo "records=$RECORDS ops=$OPS threads=$THREADS workloads=$WORKLOADS"
} > "$OUT/environment.txt"

db_props() {
  case "$1" in
    veltrixdb) echo "-p veltrixdb.addr=$VELTRIX_ADDR" ;;
    aerospike) echo "-p aerospike.host=$AEROSPIKE_HOST -p aerospike.port=3000 -p aerospike.ns=test" ;;
    scylla)    echo "-p cassandra.cluster=$SCYLLA_HOSTS -p cassandra.keyspace=ycsb -p cassandra.connections=8" ;;
  esac
}
driver() { [ "$1" = scylla ] && echo cassandra || echo "$1"; }

wait_port() { # host port
  for _ in $(seq 1 120); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && return 0; sleep 1; done
  echo "timeout waiting for $1:$2" >&2; return 1
}

for db in $DBS; do
  if [ "$START" = 1 ]; then
    docker compose up -d --build "$db"
    case "$db" in
      veltrixdb) wait_port 127.0.0.1 9000 ;;
      aerospike) wait_port 127.0.0.1 3000 ;;
      scylla)    wait_port 127.0.0.1 9042; sleep 20 ;;
    esac
  fi
  if [ "$db" = scylla ]; then
    docker compose exec -T scylla cqlsh -e \
      "CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = {'class':'SimpleStrategy','replication_factor':1};"
  fi
  props=$(db_props "$db")
  echo "== $db: load $RECORDS records"
  # shellcheck disable=SC2086
  "$OUT/ycsb" load "$(driver "$db")" -P workloads/common -P workloads/workloada \
    -p recordcount="$RECORDS" -p operationcount="$OPS" $props -threads "$THREADS" > "$OUT/$db-load.txt" 2>&1
  for w in $WORKLOADS; do
    echo "== $db: workload $w"
    # shellcheck disable=SC2086
    "$OUT/ycsb" run "$(driver "$db")" -P workloads/common -P "workloads/workload$w" \
      -p recordcount="$RECORDS" -p operationcount="$OPS" $props -threads "$THREADS" > "$OUT/$db-$w.txt" 2>&1
  done
  if [ "$db" = veltrixdb ] && [ "${VECTORS:-0}" = 1 ]; then
    go run ./cmd/vecbench -addr "$VELTRIX_ADDR" -data "${ANN_DIR:?set ANN_DIR}" -out "$OUT" \
      ${VEC_ARGS:-} > "$OUT/veltrixdb-vectors.txt" 2>&1
  fi
  if [ "$START" = 1 ]; then docker compose stop "$db"; fi
done

python3 - "$OUT" <<'PY'
import glob, os, re, sys
out = sys.argv[1]
rows = []
for f in sorted(glob.glob(os.path.join(out, "*-*.txt"))):
    db, wl = os.path.basename(f)[:-4].split("-", 1)
    if wl in ("vectors",):
        continue
    for line in open(f):
        m = re.match(r"^(\w+)\s+- Takes\(s\): [\d.]+, Count: (\d+), OPS: ([\d.]+), Avg\(us\): (\d+).*?99th\(us\): (\d+), 99\.9th\(us\): (\d+)", line)
        if m:
            rows.append((wl, db, m.group(1), int(m.group(2)), float(m.group(3)), int(m.group(4)), int(m.group(5)), int(m.group(6))))
with open(os.path.join(out, "summary.md"), "w") as s:
    s.write("| workload | db | op | count | ops/s | avg µs | p99 µs | p99.9 µs |\n|--|--|--|--|--|--|--|--|\n")
    for r in sorted(rows):
        s.write("| %s | %s | %s | %d | %.0f | %d | %d | %d |\n" % r)
print(open(os.path.join(out, "summary.md")).read())
PY
echo "results in $OUT"
