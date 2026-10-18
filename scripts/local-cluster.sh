#!/usr/bin/env bash
# local-cluster.sh — a 3-node VeltrixDB cluster (or one standalone node) on
# this machine, for trying the cluster modes and the benchmarks. Not a
# performance setup for clusters: all three nodes share one CPU and one disk.
#
#   ./scripts/local-cluster.sh up              # raft (default)
#   MODE=replicated ./scripts/local-cluster.sh up
#   MODE=standalone ./scripts/local-cluster.sh up   # one node on BASE_PORT
#   ./scripts/local-cluster.sh status
#   ./scripts/local-cluster.sh down            # stop, keep data
#   ./scripts/local-cluster.sh clean           # stop and delete data
#
# Env:
#   MODE         raft (default) | replicated | standalone
#   CONSISTENCY  replicated mode: quorum (default) | strong | eventual
#   BASE_PORT    9000 — node i listens on BASE_PORT+10*(i-1); each node also
#                uses +1 (replication), +2 (raft), +3 (gossip), +4 (metrics,
#                /healthz, /admin) and +5 (transfer)
#   DIR          ./.local-cluster — binary, data and logs
#   BIN          an existing server binary (default: built into DIR)
#   EXTRA_FLAGS  extra server flags for every node, e.g. "-raft-pipeline"
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${MODE:-raft}"
CONSISTENCY="${CONSISTENCY:-quorum}"
BASE_PORT="${BASE_PORT:-9000}"
DIR="${DIR:-$ROOT/.local-cluster}"
BIN="${BIN:-}"
EXTRA_FLAGS="${EXTRA_FLAGS:-}"

port() { echo $((BASE_PORT + 10 * ($1 - 1))); }
mport() { echo $(($(port "$1") + 4)); }
PEERS="n1@127.0.0.1:$(port 1),n2@127.0.0.1:$(port 2),n3@127.0.0.1:$(port 3)"
nodes() { [ "$(cat "$DIR/mode" 2>/dev/null || echo "$MODE")" = standalone ] && echo 1 || echo 1 2 3; }

pid_alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

up() {
  case "$MODE" in raft|replicated|standalone) ;; *) echo "MODE must be raft, replicated or standalone" >&2; exit 2 ;; esac
  mkdir -p "$DIR"
  if [ -z "$BIN" ]; then
    BIN="$DIR/veltrixdb"
    echo "building server → $BIN"
    (cd "$ROOT" && go build -o "$BIN" ./cmd/server)
  fi
  for i in 1 2 3; do
    if pid_alive "$DIR/n$i.pid"; then
      echo "n$i already running (pid $(cat "$DIR/n$i.pid")); run '$0 down' first" >&2
      exit 1
    fi
  done
  echo "$MODE" >"$DIR/mode"
  for i in $(nodes); do
    mkdir -p "$DIR/n$i"
    local extra=()
    [ "$MODE" != standalone ] && extra+=(-mode "$MODE" -node-id "n$i" -peers "$PEERS")
    [ "$MODE" = replicated ] && extra+=(-consistency "$CONSISTENCY")
    # shellcheck disable=SC2206
    [ -n "$EXTRA_FLAGS" ] && extra+=($EXTRA_FLAGS)
    "$BIN" -addr "127.0.0.1:$(port "$i")" -metrics-addr "127.0.0.1:$(mport "$i")" \
      -data "$DIR/n$i" ${extra[@]+"${extra[@]}"} >"$DIR/n$i.log" 2>&1 &
    echo $! >"$DIR/n$i.pid"
  done

  # Wait for every node's /readyz, then (raft) for a leader.
  for i in $(nodes); do
    for _ in $(seq 1 60); do
      curl -fsS "http://127.0.0.1:$(mport "$i")/readyz" >/dev/null 2>&1 && break
      pid_alive "$DIR/n$i.pid" || { echo "n$i exited; last log lines:" >&2; tail -20 "$DIR/n$i.log" >&2; exit 1; }
      sleep 0.5
    done
  done
  if [ "$MODE" = raft ]; then
    for _ in $(seq 1 60); do
      leader="$(leader_id)"
      [ -n "$leader" ] && break
      sleep 0.5
    done
  fi
  status
  cat <<EOF

Try it:
  echo -e "PUT hello world\nGET hello" | nc 127.0.0.1 $(port 1)
EOF
  [ "$MODE" != standalone ] && echo "Benchmark it (YCSB, follows raft MOVED redirects): make bench-ycsb-cluster"
  return 0
}

leader_id() {
  curl -fsS "http://127.0.0.1:$(mport 1)/admin/cluster" 2>/dev/null |
    sed -n 's/.*"leader_id": *"\([^"]*\)".*/\1/p' | head -1
}

status() {
  local mode; mode="$(cat "$DIR/mode" 2>/dev/null || echo "$MODE")"
  echo "mode: $mode   data: $DIR"
  for i in $(nodes); do
    local st=stopped
    pid_alive "$DIR/n$i.pid" && st="running (pid $(cat "$DIR/n$i.pid"))"
    printf '  n%s  client 127.0.0.1:%s  metrics http://127.0.0.1:%s  %s\n' "$i" "$(port "$i")" "$(mport "$i")" "$st"
  done
  if [ "$mode" = raft ]; then
    local l; l="$(leader_id)"
    echo "  raft leader: ${l:-none yet}"
  fi
  if [ "$mode" = standalone ]; then
    echo "  addresses: 127.0.0.1:$(port 1)"
  else
    echo "  addresses: 127.0.0.1:$(port 1),127.0.0.1:$(port 2),127.0.0.1:$(port 3)"
  fi
}

down() {
  for i in 1 2 3; do
    if pid_alive "$DIR/n$i.pid"; then
      kill "$(cat "$DIR/n$i.pid")" 2>/dev/null || true
    fi
  done
  for i in 1 2 3; do
    for _ in $(seq 1 40); do pid_alive "$DIR/n$i.pid" || break; sleep 0.25; done
    rm -f "$DIR/n$i.pid"
  done
  echo "stopped (data kept in $DIR)"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  clean) down; rm -rf "$DIR"; echo "deleted $DIR" ;;
  *) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
