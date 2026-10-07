#!/usr/bin/env bash
# compare.sh — YCSB (and optionally vector) comparison of VeltrixDB,
# Aerospike and ScyllaDB, one database at a time, single nodes (NODES=1) or
# 3-node clusters with replication factor 3 (NODES=3).
#
#   ./compare.sh                          # all three, workloads a b c f, 1 node each
#   DBS="veltrixdb scylla" WORKLOADS="a b" RECORDS=5000000 ./compare.sh
#   START=0 VELTRIX_ADDR=10.0.0.5:9000 ./compare.sh   # databases already running
#
#   # 3-node clusters on separate hosts (the real multi-node run, README
#   # "Multi-host runbook"); the client runs on a 4th machine:
#   START=0 NODES=3 VELTRIX_MODE=raft \
#     VELTRIX_ADDRS=10.0.0.11:9000,10.0.0.12:9000,10.0.0.13:9000 \
#     SCYLLA_HOSTS=10.0.0.21,10.0.0.22,10.0.0.23 \
#     AEROSPIKE_HOSTS=10.0.0.31,10.0.0.32,10.0.0.33 ./compare.sh
#
#   NODES=3 ./compare.sh   # START=1: docker-compose.cluster.yml, 3 containers per
#                          # database on THIS box — functional check only, not
#                          # performance (all nodes share the machine)
#
# Env:
#   DBS          veltrixdb aerospike scylla        WORKLOADS  a b c f
#   RECORDS      1000000     OPS  1000000           THREADS    64
#   NODES        1 (default) | 3 — replication factor = NODES
#   START        1 = docker compose up/down each database in turn (default)
#   OUT          results/<timestamp>
#   VELTRIX_ADDR / VELTRIX_ADDRS   host:port[,host:port…] (binary protocol)
#   VELTRIX_MODE raft (default) | replicated — NODES=3; START=1 starts it,
#                START=0 records it (and checks /admin/cluster when reachable)
#   VELTRIX_CONSISTENCY  replicated mode: quorum (default) | strong | eventual
#   VELTRIX_LINEARIZABLE_READS  raft mode: false (default) | true
#   VELTRIX_RAFT_PIPELINE  raft mode: false (default) | true — the servers'
#                --raft-pipeline; START=1 passes it, START=0 records it and
#                checks each node's /admin/cluster reports it
#   VELTRIX_SPREAD  true (default): YCSB threads spread over the listed nodes
#   VELTRIX_METRICS_ADDRS  host:port of each node's --metrics-addr (default
#                host:2112) — only used to record /admin/cluster
#   SCYLLA_HOSTS     h1[,h2,h3]     AEROSPIKE_HOSTS  h1[,h2,h3] (AEROSPIKE_HOST
#                    still works; the client seeds from the first host and
#                    discovers the rest)
#   SCYLLA_COMMITLOG batch (default) | periodic — START=1 applies it, START=0
#                    records what you say the servers run
#   AEROSPIKE_FLUSH_MAX_MS  1 (default)   AEROSPIKE_COMMIT_TO_DEVICE 0 | 1 (Enterprise)
#                    START=1 renders them into OUT/aerospike-conf/
#   VECTORS      1 = also run vecbench on VeltrixDB (needs ANN_DIR, see
#                scripts/ann-dataset.py); no Aerospike / Scylla vector driver
#                is included (see cmd/vecbench)
#
# Writes OUT/<db>-<workload>.txt (raw go-ycsb output), OUT/summary.md and
# OUT/environment.txt. Nothing here makes numbers comparable by itself: use
# the same machines, the same disks, the same durability setting (README).
set -euo pipefail
cd "$(dirname "$0")"

DBS="${DBS:-veltrixdb aerospike scylla}"
WORKLOADS="${WORKLOADS:-a b c f}"
RECORDS="${RECORDS:-1000000}"
OPS="${OPS:-1000000}"
THREADS="${THREADS:-64}"
START="${START:-1}"
NODES="${NODES:-1}"
OUT="${OUT:-results/$(date -u +%Y%m%dT%H%M%SZ)}"
VELTRIX_MODE="${VELTRIX_MODE:-raft}"
VELTRIX_CONSISTENCY="${VELTRIX_CONSISTENCY:-quorum}"
VELTRIX_LINEARIZABLE_READS="${VELTRIX_LINEARIZABLE_READS:-false}"
VELTRIX_RAFT_PIPELINE="${VELTRIX_RAFT_PIPELINE:-false}"
VELTRIX_SPREAD="${VELTRIX_SPREAD:-true}"
SCYLLA_COMMITLOG="${SCYLLA_COMMITLOG:-batch}"
AEROSPIKE_FLUSH_MAX_MS="${AEROSPIKE_FLUSH_MAX_MS:-1}"
AEROSPIKE_COMMIT_TO_DEVICE="${AEROSPIKE_COMMIT_TO_DEVICE:-0}"
export VELTRIX_MODE VELTRIX_CONSISTENCY VELTRIX_LINEARIZABLE_READS VELTRIX_RAFT_PIPELINE SCYLLA_COMMITLOG

case "$NODES" in 1|3) ;; *) echo "NODES must be 1 or 3" >&2; exit 2 ;; esac
case "$VELTRIX_MODE" in raft|replicated) ;; *) echo "VELTRIX_MODE must be raft or replicated" >&2; exit 2 ;; esac
case "$VELTRIX_RAFT_PIPELINE" in true|false) ;; *) echo "VELTRIX_RAFT_PIPELINE must be true or false" >&2; exit 2 ;; esac
RF="$NODES"
COMPOSE=(docker compose)
if [ "$NODES" = 3 ] && [ "$START" = 1 ]; then
  # docker-compose.cluster.yml: fixed IPs on the "bench" network.
  COMPOSE=(docker compose -f docker-compose.cluster.yml)
  VELTRIX_ADDRS=172.28.0.11:9000,172.28.0.12:9000,172.28.0.13:9000
  VELTRIX_METRICS_ADDRS=172.28.0.11:2112,172.28.0.12:2112,172.28.0.13:2112
  AEROSPIKE_HOSTS=172.28.0.21,172.28.0.22,172.28.0.23
  SCYLLA_HOSTS=172.28.0.31,172.28.0.32,172.28.0.33
  export VELTRIX_CLUSTER_SECRET="${VELTRIX_CLUSTER_SECRET:-$(head -c 32 /dev/urandom | base64 | tr -d '/+=\n')}"
fi
VELTRIX_ADDRS="${VELTRIX_ADDRS:-${VELTRIX_ADDR:-127.0.0.1:9000}}"
AEROSPIKE_HOSTS="${AEROSPIKE_HOSTS:-${AEROSPIKE_HOST:-127.0.0.1}}"
SCYLLA_HOSTS="${SCYLLA_HOSTS:-127.0.0.1}"
count() { local IFS=,; set -- $1; echo $#; }
first() { echo "${1%%,*}"; }
if [ "$NODES" = 3 ]; then
  for v in VELTRIX_ADDRS AEROSPIKE_HOSTS SCYLLA_HOSTS; do
    db=$(echo "$v" | sed 's/_.*//' | tr 'A-Z' 'a-z'); db=${db/veltrix/veltrixdb}
    case " $DBS " in *" $db "*) [ "$(count "${!v}")" = 3 ] || { echo "NODES=3 needs 3 entries in $v (got ${!v})" >&2; exit 2; } ;; esac
  done
fi
mkdir -p "$OUT"
OUT_ABS=$(cd "$OUT" && pwd)

go build -o "$OUT/ycsb" ./cmd/ycsb
go build -o "$OUT/cqlexec" ./cmd/cqlexec
go build -o "$OUT/keycount" ./cmd/keycount

# Aerospike durability switches → rendered confs (also archived with the results).
mkdir -p "$OUT/aerospike-conf"
sed_as=(-e "s/^\([[:space:]]*\)flush-max-ms .*/\1flush-max-ms $AEROSPIKE_FLUSH_MAX_MS/")
if [ "$AEROSPIKE_COMMIT_TO_DEVICE" = 1 ]; then
  sed_as+=(-e 's/^\([[:space:]]*\)#[[:space:]]*commit-to-device true.*/\1commit-to-device true/')
fi
for f in aerospike.conf aerospike-cluster-1.conf aerospike-cluster-2.conf aerospike-cluster-3.conf; do
  sed "${sed_as[@]}" "$f" > "$OUT/aerospike-conf/$f"
done
export AEROSPIKE_CONF_DIR="$OUT_ABS/aerospike-conf"

if [ "$NODES" = 1 ]; then SCYLLA_CL="QUORUM (RF 1: = ONE)"; else SCYLLA_CL="QUORUM (2 of 3)"; fi
if [ "$VELTRIX_MODE" = raft ]; then
  VELTRIX_CL="raft: writes quorum-committed through the leader; reads $( [ "$VELTRIX_LINEARIZABLE_READS" = true ] && echo "linearizable (leader, ReadIndex)" || echo "local on any node (followers may be stale)"); --raft-pipeline=$VELTRIX_RAFT_PIPELINE"
else
  VELTRIX_CL="replicated: any node writes locally, ACK at --consistency=$VELTRIX_CONSISTENCY; reads local"
fi
if [ "$NODES" = 1 ]; then VELTRIX_CL="standalone (single node)"; fi
AEROSPIKE_CL="AP (Community; strong-consistency is Enterprise); writes COMMIT_ALL (client default), reads from master"
AEROSPIKE_DUR="flush-max-ms $AEROSPIKE_FLUSH_MAX_MS, commit-to-device $( [ "$AEROSPIKE_COMMIT_TO_DEVICE" = 1 ] && echo on || echo off)"

{
  echo "date: $(date -u +%FT%TZ)"
  echo "host: $(hostname)"
  uname -a
  (command -v lscpu >/dev/null && lscpu | grep -E 'Model name|^CPU\(s\)') || sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  (command -v free >/dev/null && free -g | head -2) || true
  df -h "${BENCH_DATA:-./data}" 2>/dev/null || true
  echo "records=$RECORDS ops=$OPS threads=$THREADS workloads=$WORKLOADS"
  echo "nodes=$NODES rf=$RF start=$START dbs=$DBS"
  if [ "$NODES" = 3 ] && [ "$START" = 1 ]; then echo "NOTE: START=1 NODES=3 runs all 3 nodes of each database on this one machine — functional check, not a performance result"; fi
  echo "veltrixdb: addrs=$VELTRIX_ADDRS mode=$( [ "$NODES" = 1 ] && echo standalone || echo "$VELTRIX_MODE") consistency=\"$VELTRIX_CL\" spread=$VELTRIX_SPREAD durability=fdatasync-before-ACK client=$(git describe --always --dirty 2>/dev/null || echo unknown)"
  echo "scylla: hosts=$SCYLLA_HOSTS rf=$RF consistency=\"$SCYLLA_CL\" commitlog_sync=$SCYLLA_COMMITLOG$( [ "$START" = 0 ] && echo ' (as declared; START=0 cannot check)' || echo " image=${SCYLLA_VERSION:-latest}")"
  echo "aerospike: hosts=$AEROSPIKE_HOSTS rf=$RF consistency=\"$AEROSPIKE_CL\" durability=\"$AEROSPIKE_DUR\"$( [ "$START" = 0 ] && echo ' (as declared)' || echo " image=${AEROSPIKE_VERSION:-latest}")"
} > "$OUT/environment.txt"

db_props() {
  case "$1" in
    veltrixdb) echo "-p veltrixdb.addr=$VELTRIX_ADDRS -p veltrixdb.spread=$VELTRIX_SPREAD" ;;
    # go-ycsb's aerospike driver takes ONE seed host; the client discovers the
    # other nodes from it and sends each key to its master.
    aerospike) echo "-p aerospike.host=$(first "$AEROSPIKE_HOSTS") -p aerospike.port=3000 -p aerospike.ns=test" ;;
    # go-ycsb v1.0.3's cassandra driver hard-codes consistency QUORUM for
    # reads and writes (db/cassandra/db.go); there is no property for it.
    scylla)    echo "-p cassandra.cluster=$SCYLLA_HOSTS -p cassandra.keyspace=ycsb -p cassandra.connections=8" ;;
  esac
}
driver() { [ "$1" = scylla ] && echo cassandra || echo "$1"; }
services() { if [ "$NODES" = 3 ]; then echo "$1-1 $1-2 $1-3"; else echo "$1"; fi; }

wait_port() { # host port
  for _ in $(seq 1 180); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && return 0; sleep 1; done
  echo "timeout waiting for $1:$2" >&2; return 1
}

# record_veltrix_topology: /admin/cluster of every node (mode, raft leader).
record_veltrix_topology() {
  local i=0 addr host maddrs
  IFS=, read -r -a maddrs <<< "${VELTRIX_METRICS_ADDRS:-}"
  for addr in ${VELTRIX_ADDRS//,/ }; do
    host=${addr%:*}
    local m=${maddrs[$i]:-$host:2112}
    i=$((i+1))
    local js; js=$(curl -s -m 3 "http://$m/admin/cluster" 2>/dev/null || true)
    [ -n "$js" ] || { echo "veltrixdb $addr: /admin/cluster at $m not reachable"; continue; }
    JS="$js" python3 - "$addr" "$VELTRIX_MODE" "$NODES" "$VELTRIX_RAFT_PIPELINE" <<'PY'
import json, os, sys
addr, want, nodes, pipe = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "true"
j = json.loads(os.environ["JS"])
r = j.get("raft") or {}
print("veltrixdb %s: node=%s mode=%s leader=%s role=%s members=%d%s" % (addr, j.get("node_id"), j.get("mode"),
      r.get("leader_id", "-"), r.get("role", "-"), len(j.get("nodes", [])),
      (" pipeline=%s" % str(r["pipeline"]).lower()) if "pipeline" in r else ""))
if nodes == "3" and j.get("mode") != want:
    print("WARNING: VELTRIX_MODE=%s but %s runs mode=%s" % (want, addr, j.get("mode")))
if nodes == "3" and want == "raft" and r and r.get("pipeline", False) != pipe:
    print("WARNING: VELTRIX_RAFT_PIPELINE=%s but %s runs --raft-pipeline=%s%s" % (str(pipe).lower(), addr,
          str(r.get("pipeline", False)).lower(), "" if "pipeline" in r else " (build without the flag)"))
PY
  done
}

for db in $DBS; do
  if [ "$START" = 1 ]; then
    # shellcheck disable=SC2046
    "${COMPOSE[@]}" up -d --build $(services "$db")
    case "$db" in
      veltrixdb) for a in ${VELTRIX_ADDRS//,/ }; do wait_port "${a%:*}" "${a##*:}"; done
                 if [ "$NODES" = 3 ]; then sleep 5; fi ;;  # leader election
      aerospike) for h in ${AEROSPIKE_HOSTS//,/ }; do wait_port "$h" 3000; done
                 if [ "$NODES" = 3 ]; then sleep 10; fi ;; # mesh forms, partitions rebalance
      scylla)    for h in ${SCYLLA_HOSTS//,/ }; do wait_port "$h" 9042; done; sleep 20 ;;
    esac
  fi
  case "$db" in
    veltrixdb) record_veltrix_topology >> "$OUT/environment.txt" ;;
    scylla)
      # NetworkTopologyStrategy with the replication_factor shorthand works with
      # vnodes and tablets; ALTER fixes a keyspace left over with another RF.
      repl="{'class':'NetworkTopologyStrategy','replication_factor':$RF}"
      "$OUT/cqlexec" -hosts "$SCYLLA_HOSTS" \
        -e "CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = $repl" \
        -e "ALTER KEYSPACE ycsb WITH replication = $repl" \
        -e "SELECT release_version FROM system.local" \
        -e "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='ycsb'" \
        | sed 's/^/scylla: /' >> "$OUT/environment.txt"
      ;;
    aerospike)
      if command -v asinfo >/dev/null; then
        for h in ${AEROSPIKE_HOSTS//,/ }; do
          echo "aerospike $h: build=$(asinfo -h "$h" -v build 2>/dev/null) $(asinfo -h "$h" -v 'namespace/test' 2>/dev/null | tr ';' '\n' | grep -E '^(replication-factor|strong-consistency)=' | tr '\n' ' ')"
        done >> "$OUT/environment.txt"
      fi ;;
  esac
  props=$(db_props "$db")
  echo "== $db: load $RECORDS records (nodes=$NODES)"
  # shellcheck disable=SC2086
  "$OUT/ycsb" load "$(driver "$db")" -P workloads/common -P workloads/workloada \
    -p recordcount="$RECORDS" -p operationcount="$OPS" $props -threads "$THREADS" > "$OUT/$db-load.txt" 2>&1
  for w in $WORKLOADS; do
    echo "== $db: workload $w"
    # shellcheck disable=SC2086
    "$OUT/ycsb" run "$(driver "$db")" -P workloads/common -P "workloads/workload$w" \
      -p recordcount="$RECORDS" -p operationcount="$OPS" $props -threads "$THREADS" > "$OUT/$db-$w.txt" 2>&1
  done
  if [ "$db" = veltrixdb ] && [ "$NODES" = 3 ]; then
    # RF 3 on 3 nodes: every node must hold every record. YCSB cannot see a
    # missing replica (a read of an absent key is an empty success).
    "$OUT/keycount" -addrs "$VELTRIX_ADDRS" > "$OUT/veltrixdb-keycount.txt" 2>&1 \
      || echo "WARNING: VeltrixDB replicas differ — see $OUT/veltrixdb-keycount.txt" | tee -a "$OUT/veltrixdb-keycount.txt"
  fi
  if [ "$db" = veltrixdb ] && [ "${VECTORS:-0}" = 1 ]; then
    go run ./cmd/vecbench -addr "$(first "$VELTRIX_ADDRS")" -data "${ANN_DIR:?set ANN_DIR}" -out "$OUT" \
      ${VEC_ARGS:-} > "$OUT/veltrixdb-vectors.txt" 2>&1
  fi
  # shellcheck disable=SC2046
  if [ "$START" = 1 ]; then "${COMPOSE[@]}" stop $(services "$db"); fi
done

python3 - "$OUT" <<'PY'
import glob, os, re, sys
out = sys.argv[1]
rows, routing = {}, []
for f in sorted(glob.glob(os.path.join(out, "*-*.txt"))):
    db, wl = os.path.basename(f)[:-4].split("-", 1)
    if wl in ("vectors", "keycount"):
        continue
    for line in open(f):
        m = re.match(r"^(\w+)\s+- Takes\(s\): [\d.]+, Count: (\d+), OPS: ([\d.]+), Avg\(us\): (\d+).*?99th\(us\): (\d+), 99\.9th\(us\): (\d+)", line)
        if m:  # go-ycsb also prints these every 10 s while running: the last one is the result
            rows[(wl, db, m.group(1))] = (int(m.group(2)), float(m.group(3)), int(m.group(4)), int(m.group(5)), int(m.group(6)))
        elif line.startswith("# veltrixdb:"):
            routing.append((wl, line[len("# veltrixdb:"):].strip()))
env = [l.rstrip("\n") for l in open(os.path.join(out, "environment.txt"))]
cfg = [l for l in env if re.match(r"^(nodes=|NOTE:|veltrixdb|scylla|aerospike)", l)]
with open(os.path.join(out, "summary.md"), "w") as s:
    s.write("## Configuration\n\n")
    for l in cfg:
        s.write("- %s\n" % l)
    s.write("\n## Results\n\n| workload | db | op | count | ops/s | avg µs | p99 µs | p99.9 µs |\n|--|--|--|--|--|--|--|--|\n")
    for k in sorted(rows):
        s.write("| %s | %s | %s | %d | %.0f | %d | %d | %d |\n" % (k + rows[k]))
    kc = os.path.join(out, "veltrixdb-keycount.txt")
    if os.path.exists(kc):
        s.write("\n## VeltrixDB replica check (records per node after the run)\n\n")
        for l in open(kc):
            s.write("- %s\n" % l.strip())
    if routing:
        s.write("\n## VeltrixDB client routing (redirects / failovers / ops answered per node)\n\n| workload | routing |\n|--|--|\n")
        for wl, r in routing:
            s.write("| %s | %s |\n" % (wl, r))
print(open(os.path.join(out, "summary.md")).read())
PY
echo "results in $OUT"
