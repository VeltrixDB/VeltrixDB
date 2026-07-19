#!/usr/bin/env bash
# scripts/hugepages.sh — One-time host preparation for VeltrixDB on any VM.
# Run as root before starting veltrixdb.
#
# What it does:
#   1. Allocates 512 × 2MB hugepages
#   2. Raises file descriptor limits
#   3. Sets NVMe I/O schedulers to "none" (bypass CFQ/mq-deadline)
#   4. Pins CPU performance governor
#   5. Applies TCP network tuning

set -euo pipefail

log() { echo "[hugepages.sh] $*"; }

# ── 1. HugePages ─────────────────────────────────────────────────────────────
log "Allocating 512 x 2MB hugepages..."
echo 512 > /proc/sys/vm/nr_hugepages
ACTUAL=$(cat /proc/sys/vm/nr_hugepages)
if [[ "${ACTUAL}" -lt 512 ]]; then
  log "WARNING: only ${ACTUAL}/512 hugepages allocated (fragmented memory?)"
  log "Try: echo 3 > /proc/sys/vm/drop_caches && echo 512 > /proc/sys/vm/nr_hugepages"
else
  log "HugePages: ${ACTUAL} allocated"
fi

# ── 2. File descriptors ───────────────────────────────────────────────────────
log "Raising file descriptor limits..."
ulimit -n 1048576 2>/dev/null || log "WARNING: could not raise ulimit -n (try /etc/security/limits.conf)"

# ── 3. NVMe I/O scheduler ─────────────────────────────────────────────────────
log "Setting NVMe I/O schedulers to 'none'..."
for dev in /sys/block/nvme*n1/queue/scheduler; do
  if [[ -f "${dev}" ]]; then
    echo "none" > "${dev}"
    log "  ${dev}: none"
  fi
done

# ── 4. CPU performance governor ───────────────────────────────────────────────
if [[ -d /sys/devices/system/cpu/cpu0/cpufreq ]]; then
  log "Setting CPU governor to 'performance'..."
  for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance > "${gov}" 2>/dev/null || true
  done
fi

# ── 5. TCP tuning ─────────────────────────────────────────────────────────────
log "Applying TCP tuning..."
sysctl -w net.core.rmem_max=134217728 >/dev/null
sysctl -w net.core.wmem_max=134217728 >/dev/null
sysctl -w net.core.netdev_max_backlog=65536 >/dev/null
sysctl -w net.ipv4.tcp_max_syn_backlog=65536 >/dev/null

# ── 6. Locked memory limit ────────────────────────────────────────────────────
log "Setting memlock to unlimited..."
ulimit -l unlimited 2>/dev/null || log "WARNING: could not set ulimit -l unlimited"

log ""
log "Host preparation complete. You can now start veltrixdb."
log "  Verify: grep -i hugepages /proc/meminfo"
log "  Run:    ./veltrixdb --config ./config.yaml"
