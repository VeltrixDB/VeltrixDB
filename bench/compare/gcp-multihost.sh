#!/usr/bin/env bash
# gcp-multihost.sh — the README "Multi-host runbook" for VeltrixDB on GCP,
# end to end: 3 server VMs (each with its own local NVMe SSD) + 1 client VM in
# one zone and one compact placement policy, the VeltrixDB cluster started in
# each configuration in turn, compare.sh run from the client, results copied
# back, and every resource deleted at the end.
#
#   PROJECT=my-sandbox ./gcp-multihost.sh
#   PROJECT=my-sandbox CONFIGS="raft-on raft-off" RUNS=3 RECORDS=2000000 ./gcp-multihost.sh
#   DRY_RUN=1 PROJECT=x ./gcp-multihost.sh      # print the gcloud commands only
#
# Env:
#   PROJECT      GCP project (required)        ZONE      us-central1-a
#   MACHINE      n2-standard-8 (servers)       CLIENT_MACHINE  same as MACHINE
#   IMAGE_FAMILY ubuntu-2404-lts-amd64         PREFIX    vxbench
#   CONFIGS      "raft-off raft-on replicated" — raft-off / raft-on = --mode=raft
#                with --raft-pipeline=false / true; replicated = --mode=replicated
#                --consistency=quorum (the reference)
#   RUNS         2 (each config is wiped and restarted before every run)
#   RECORDS 1000000  OPS 1000000  THREADS 64  WORKLOADS "a b c"
#   CACHE_MB     8192
#   PLACEMENT    1 = compact placement policy (needs a machine family that
#                supports it, e.g. n2 / c2 / c3); 0 = none
#   IAP          1 = ssh through IAP and create VMs without external IPs (the
#                client then needs Cloud NAT for apt / Go downloads); 0 (default)
#   KEEP         1 = leave the VMs running at the end (default 0: delete all)
#   GO_VERSION   Go installed on the client to build go-ycsb (default: from
#                bench/compare/go.mod)
#
# Cost: 4 × n2-standard-8 + 3 local SSDs ≈ US$1.7/hour on-demand in
# us-central1 (check current pricing). The trap deletes everything on exit,
# including Ctrl-C; if the script is killed hard, run it again with
# CLEANUP_ONLY=1 and the same PROJECT / ZONE / PREFIX.
#
# Results land in results/gcp-<timestamp>/<config>-r<N>/ plus combined.md.
set -euo pipefail
cd "$(dirname "$0")"
HERE=$(pwd)
REPO=$(cd ../.. && pwd)

PROJECT="${PROJECT:-}"
ZONE="${ZONE:-us-central1-a}"
REGION="${ZONE%-*}"
MACHINE="${MACHINE:-n2-standard-8}"
CLIENT_MACHINE="${CLIENT_MACHINE:-$MACHINE}"
IMAGE_FAMILY="${IMAGE_FAMILY:-ubuntu-2404-lts-amd64}"
PREFIX="${PREFIX:-vxbench}"
CONFIGS="${CONFIGS:-raft-off raft-on replicated}"
RUNS="${RUNS:-2}"
RECORDS="${RECORDS:-1000000}"
OPS="${OPS:-1000000}"
THREADS="${THREADS:-64}"
WORKLOADS="${WORKLOADS:-a b c}"
CACHE_MB="${CACHE_MB:-8192}"
PLACEMENT="${PLACEMENT:-1}"
IAP="${IAP:-0}"
KEEP="${KEEP:-0}"
DRY_RUN="${DRY_RUN:-0}"
CLEANUP_ONLY="${CLEANUP_ONLY:-0}"
GO_VERSION="${GO_VERSION:-$(awk '/^go /{print $2; exit}' go.mod)}"

[ -n "$PROJECT" ] || { echo "PROJECT is required (a project where you may create VMs)" >&2; exit 2; }
for c in $CONFIGS; do
  case "$c" in raft-off|raft-on|replicated) ;; *) echo "unknown config $c" >&2; exit 2 ;; esac
done

SERVERS="$PREFIX-s1 $PREFIX-s2 $PREFIX-s3"
CLIENT="$PREFIX-client"
POLICY="$PREFIX-compact"
FW="$PREFIX-internal"
TS=$(date -u +%Y%m%dT%H%M%SZ)
RESULTS="$HERE/results/gcp-$TS"
WORK=$(mktemp -d)

G() { # gcloud with project; prints instead of running under DRY_RUN
  if [ "$DRY_RUN" = 1 ]; then echo "+ gcloud $* --project=$PROJECT" >&2; return 0; fi
  gcloud "$@" --project="$PROJECT"
}
IAP_FLAG=(); [ "$IAP" = 1 ] && IAP_FLAG=(--tunnel-through-iap)
ssh_() { # vm command
  G compute ssh "$1" --zone="$ZONE" --quiet ${IAP_FLAG[@]+"${IAP_FLAG[@]}"} --command="$2"
}
scp_() { # src… dst (vm:path)
  G compute scp --zone="$ZONE" --quiet ${IAP_FLAG[@]+"${IAP_FLAG[@]}"} "$@"
}
ip_of() {
  if [ "$DRY_RUN" = 1 ]; then echo "10.0.0.1${1: -1}"; return; fi
  gcloud compute instances describe "$1" --zone="$ZONE" --project="$PROJECT" \
    --format='get(networkInterfaces[0].networkIP)'
}

cleanup() {
  rm -rf "$WORK"
  if [ "$KEEP" = 1 ]; then echo "KEEP=1: VMs left running: $SERVERS $CLIENT"; return; fi
  echo "== deleting VMs, firewall rule and placement policy"
  # shellcheck disable=SC2086
  G compute instances delete $SERVERS $CLIENT --zone="$ZONE" --quiet 2>/dev/null || true
  G compute firewall-rules delete "$FW" --quiet 2>/dev/null || true
  [ "$PLACEMENT" = 1 ] && { G compute resource-policies delete "$POLICY" --region="$REGION" --quiet 2>/dev/null || true; }
}
if [ "$CLEANUP_ONLY" = 1 ]; then cleanup; exit 0; fi
trap cleanup EXIT

# ── build locally ────────────────────────────────────────────────────────────
echo "== building linux/amd64 server binary"
(cd "$REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$WORK/veltrixdb" ./cmd/server)
if ! grep -aq "raft-pipeline" "$WORK/veltrixdb"; then
  case " $CONFIGS " in *" raft-on "*|*" raft-off "*)
    echo "this build has no --raft-pipeline flag; use CONFIGS=\"raft replicated\" on older builds" >&2; exit 2 ;;
  esac
fi
head -c 32 /dev/urandom | base64 > "$WORK/cluster.secret"
# Source for the client: compare.sh builds go-ycsb against the repo (replace ../..).
tar -C "$REPO" --exclude=.git --exclude='bench/compare/results' --exclude='*-data' \
  --exclude='bench/compare/data' -czf "$WORK/src.tgz" .
GIT_DESC=$(cd "$REPO" && git describe --always --dirty 2>/dev/null || echo unknown)

# ── infrastructure ───────────────────────────────────────────────────────────
echo "== creating VMs in $PROJECT/$ZONE ($MACHINE ×3 + $CLIENT_MACHINE client)"
POLICY_FLAG=()
if [ "$PLACEMENT" = 1 ]; then
  G compute resource-policies create group-placement "$POLICY" --region="$REGION" \
    --collocation=collocated --vm-count=4
  POLICY_FLAG=(--resource-policies="$POLICY" --maintenance-policy=TERMINATE)
fi
G compute firewall-rules create "$FW" --network=default --direction=INGRESS \
  --allow=tcp:9000-9005,tcp:2112 --source-tags="$PREFIX" --target-tags="$PREFIX"
ADDR_FLAG=(); [ "$IAP" = 1 ] && ADDR_FLAG=(--no-address)
# shellcheck disable=SC2086
G compute instances create $SERVERS --zone="$ZONE" --machine-type="$MACHINE" \
  --image-family="$IMAGE_FAMILY" --image-project=ubuntu-os-cloud --boot-disk-size=50GB \
  --local-ssd=interface=NVME --tags="$PREFIX" ${POLICY_FLAG[@]+"${POLICY_FLAG[@]}"} ${ADDR_FLAG[@]+"${ADDR_FLAG[@]}"}
G compute instances create "$CLIENT" --zone="$ZONE" --machine-type="$CLIENT_MACHINE" \
  --image-family="$IMAGE_FAMILY" --image-project=ubuntu-os-cloud --boot-disk-size=50GB \
  --tags="$PREFIX" ${POLICY_FLAG[@]+"${POLICY_FLAG[@]}"} ${ADDR_FLAG[@]+"${ADDR_FLAG[@]}"}

echo "== waiting for ssh"
for vm in $SERVERS $CLIENT; do
  for i in $(seq 1 30); do ssh_ "$vm" true >/dev/null 2>&1 && break; [ "$i" = 30 ] && { echo "no ssh to $vm" >&2; exit 1; }; sleep 10; done
done

IPS=(); for vm in $SERVERS; do IPS+=("$(ip_of "$vm")"); done
PEERS="n1@${IPS[0]}:9000,n2@${IPS[1]}:9000,n3@${IPS[2]}:9000"
ADDRS="${IPS[0]}:9000,${IPS[1]}:9000,${IPS[2]}:9000"
echo "servers: ${IPS[*]}"

echo "== preparing servers (local NVMe → /mnt/nvme0, binary, secret)"
for vm in $SERVERS; do
  scp_ "$WORK/veltrixdb" "$WORK/cluster.secret" "$vm:/tmp/"
  ssh_ "$vm" "set -e
    dev=/dev/disk/by-id/google-local-nvme-ssd-0
    sudo mkfs.ext4 -q -F \$dev && sudo mkdir -p /mnt/nvme0 && sudo mount -o noatime \$dev /mnt/nvme0
    sudo install -d -m 755 /opt/veltrixdb /etc/veltrixdb
    sudo install -m 755 /tmp/veltrixdb /opt/veltrixdb/veltrixdb
    sudo install -m 600 /tmp/cluster.secret /etc/veltrixdb/cluster.secret
    # one-line disk fact for the results: synchronous 4 KiB writes per second
    sudo dd if=/dev/zero of=/mnt/nvme0/ddtest bs=4k count=2000 oflag=dsync 2>&1 | tail -1 | sed 's/^/dsync 4k: /'
    sudo rm -f /mnt/nvme0/ddtest" | tee "$WORK/disk-$vm.txt"
done

echo "== preparing client (Go $GO_VERSION, source)"
scp_ "$WORK/src.tgz" "$CLIENT:/tmp/"
ssh_ "$CLIENT" "set -e
  sudo apt-get -qq update >/dev/null && sudo apt-get -qq install -y curl python3 >/dev/null
  curl -sSfL https://go.dev/dl/go$GO_VERSION.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
  rm -rf ~/vx && mkdir -p ~/vx && tar -C ~/vx -xzf /tmp/src.tgz
  cd ~/vx/bench/compare && PATH=/usr/local/go/bin:\$PATH go mod download"

start_cluster() { # config
  local mode=raft pipe=false extra="" n=0
  case "$1" in
    raft-on) pipe=true ;;
    replicated) mode=replicated; extra="--consistency=quorum" ;;
  esac
  local pipeflag="--raft-pipeline=$pipe"; [ "$mode" = replicated ] && pipeflag=""
  for vm in $SERVERS; do
    n=$((n+1))
    local ip=${IPS[$((n-1))]}
    ssh_ "$vm" "set -e
      sudo systemctl stop veltrixdb 2>/dev/null || true; sudo systemctl reset-failed veltrixdb 2>/dev/null || true
      sudo rm -rf /mnt/nvme0/vx && sudo mkdir -p /mnt/nvme0/vx
      sudo systemd-run --unit=veltrixdb --property=LimitNOFILE=1048576 /opt/veltrixdb/veltrixdb \
        --mode=$mode --node-id=n$n --addr=$ip:9000 --peers=$PEERS \
        --data-dirs=/mnt/nvme0/vx --cache=$CACHE_MB --metrics-addr=$ip:2112 \
        --cluster-secret-file=/etc/veltrixdb/cluster.secret $pipeflag $extra"
  done
  # Ready = every node answers /admin/cluster with 3 members (and a leader in raft mode).
  ssh_ "$CLIENT" "for i in \$(seq 1 120); do
      ok=1
      for ip in ${IPS[*]}; do
        curl -s -m 2 http://\$ip:2112/admin/cluster | python3 -c '
import json,sys
j=json.load(sys.stdin); r=j.get(\"raft\") or {}
sys.exit(0 if len(j.get(\"nodes\",[]))==3 and (\"$mode\"!=\"raft\" or r.get(\"leader_id\")) else 1)' || ok=0
      done
      [ \$ok = 1 ] && exit 0; sleep 1
    done; echo 'cluster not ready' >&2; exit 1"
}

mkdir -p "$RESULTS"
{
  echo "date=$TS project=$PROJECT zone=$ZONE machine=$MACHINE client=$CLIENT_MACHINE image=$IMAGE_FAMILY placement=$PLACEMENT"
  echo "server build: $GIT_DESC"
  echo "records=$RECORDS ops=$OPS threads=$THREADS workloads=$WORKLOADS runs=$RUNS configs=$CONFIGS cache=${CACHE_MB}MB"
  for vm in $SERVERS; do echo "$vm: $(cat "$WORK/disk-$vm.txt" 2>/dev/null)"; done
} > "$RESULTS/setup.txt"

for run in $(seq 1 "$RUNS"); do
  for cfg in $CONFIGS; do
    echo "== $cfg run $run"
    start_cluster "$cfg"
    mode=raft; [ "$cfg" = replicated ] && mode=replicated
    pipe=""; case "$cfg" in raft-on) pipe=true ;; raft-off) pipe=false ;; esac
    out="results/$cfg-r$run"
    ssh_ "$CLIENT" "cd ~/vx/bench/compare && PATH=/usr/local/go/bin:\$PATH \
      START=0 NODES=3 DBS=veltrixdb VELTRIX_MODE=$mode VELTRIX_RAFT_PIPELINE=$pipe \
      VELTRIX_ADDRS=$ADDRS RECORDS=$RECORDS OPS=$OPS THREADS=$THREADS WORKLOADS='$WORKLOADS' \
      OUT=$out ./compare.sh && tar -C results -czf /tmp/$cfg-r$run.tgz $cfg-r$run"
    scp_ "$CLIENT:/tmp/$cfg-r$run.tgz" "$WORK/"
    [ "$DRY_RUN" = 1 ] || tar -C "$RESULTS" -xzf "$WORK/$cfg-r$run.tgz"
  done
done

for vm in $SERVERS; do ssh_ "$vm" "sudo systemctl stop veltrixdb" || true; done

{
  echo "# VeltrixDB multi-host run $TS"; echo
  echo '```'; cat "$RESULTS/setup.txt"; echo '```'
  for d in "$RESULTS"/*-r*; do
    [ -d "$d" ] || continue
    echo; echo "## $(basename "$d")"; echo
    cat "$d/summary.md" 2>/dev/null || echo "(no summary.md)"
  done
} > "$RESULTS/combined.md"
echo "== done: $RESULTS/combined.md"
