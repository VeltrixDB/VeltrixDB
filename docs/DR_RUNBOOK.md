# VeltrixDB Disaster Recovery Runbook

> Assumes `kubectl` admin rights and pods labelled `app.kubernetes.io/name=veltrixdb` in `-n veltrixdb` (the `kubectl veltrix` plugin's defaults, `cmd/kubectl-veltrix`). The plugin reaches `/admin/*` through `kubectl port-forward` (a loopback connection) and sends no admin token, so it only works on servers started without `--admin-token`.

---

## Severity

| Class | Definition | Engage on-call |
|-------|------------|----------------|
| SEV-0 | Cluster-wide write outage OR confirmed data loss | < 5 min |
| SEV-1 | One pod down, replicas serving | < 15 min |
| SEV-2 | Read latency > 5× baseline | < 1 hour |
| SEV-3 | Single disk slow, scrubber alerting, no user impact | < 4 hours |

---

## 1. Pod crash loop (SEV-1)

```bash
kubectl logs -n veltrixdb POD --previous | tail -100
kubectl describe pod -n veltrixdb POD
```

| Log line | Cause | Fix |
|----------|-------|-----|
| `[wal] replay of … stopped at byte N of M after K records` | Torn final write on crash (small `M − N`), or WAL damage (large) — every record is CRC32C-checked and replay stops at the first bad one | Restart pod once; if recurring or `M − N` is large, the WAL is damaged: wipe this node and let replication refill it (§2), or restore from backup (§5) in standalone mode |
| `panic: native index: insert failed (out of memory?)` | `vm.max_map_count` too low for the off-heap native index (cgo builds) | Apply `scripts/sysctl.conf` (`vm.max_map_count = 262144`) on the node; or set `VELTRIXDB_INDEX=map` to use the Go map index |
| `vlog bad magic at offset O` / `vlog CRC32C mismatch at offset O` | Silent disk corruption | Drain node, replace disk, re-join |
| `[search] rebuilt N vectors and M text documents in T` | Normal after every start: search indexes are rebuilt from the persisted keys; searches are refused until this line (`INFO` → `search_ready=1`) | Nothing — size restart windows by T for large vector namespaces |
| client error `search indexes are still rebuilding after restart (loaded x of y)` | A search reached a node before its rebuild finished | Retry; or run with `--search-allow-partial` if incomplete results are acceptable |
| `[transfer] WARNING: listener … is unauthenticated` | No `--cluster-secret-file` and no cluster mTLS | Mount the same secret on every node (`VELTRIXDB_CLUSTER_SECRET`) and restart |
| `encryption: enabled but no key in VELTRIXDB_ENCRYPTION_KEY and no EncryptionKeyPath set` | `--encrypt-at-rest` with no key (env unset, no `--encryption-key-path`) | `kubectl create secret generic veltrixdb-enc --from-literal=key=BASE64_32B` |

---

## 2. Data corruption (SEV-0)

The image is distroless (no shell, `curl` or `rm`), so commands that need
tools run from your workstation, an ephemeral debug container that shares the
server's process namespace (`--target`, the server's filesystem is then at
`/proc/1/root`), or a node debug pod (host filesystem at `/host`).

```bash
# Confirm
kubectl port-forward -n veltrixdb POD 2112:2112 &
curl -s localhost:2112/metrics | grep -E 'scrub_corruption_total|vlog_gc_read_errors_total'

# Find the bad disk
kubectl logs -n veltrixdb POD | grep -E '\[scrub\] disk=.*MISMATCH'

# Quarantine the pod
kubectl label pod -n veltrixdb POD veltrixdb.io/quarantine=true --overwrite

# Wipe this node's data (raft / replicated modes only — replication refills it;
# in standalone mode restore from backup instead, §5)
kubectl debug -n veltrixdb POD -it --image=busybox:1.36 --target=veltrixdb -- \
  sh -c 'rm -f /proc/1/root/mnt/nvme*/vlog_active.dat /proc/1/root/mnt/nvme*/wal.log*'
kubectl delete pod -n veltrixdb POD

# After replay completes
kubectl label pod -n veltrixdb POD veltrixdb.io/quarantine- --overwrite

# Root-cause: SMART data from the node
kubectl debug node/NODE -it --image=ubuntu:24.04 -- \
  chroot /host smartctl -a /dev/nvme0n1
```

---

## 3. Write outage (SEV-0)

```bash
kubectl get pods -n veltrixdb -o wide
kubectl debug -n veltrixdb POD -it --image=busybox:1.36 --target=veltrixdb -- \
  sh -c 'df -h /proc/1/root/mnt/nvme*'
kubectl port-forward -n veltrixdb POD 2112:2112 &
curl -s localhost:2112/metrics | grep -E 'storage_wal_flushes_total|vlog_garbage_ratio'
```

| Cause | Fix |
|-------|-----|
| Disk 100% full + GC paused | `kubectl veltrix checkpoint`; check `kubectl veltrix gc-status` |
| GC can't keep up | `kubectl veltrix quota-set NS 1000 5000000` — throttle writes |
| Network partition | `kubectl debug -n veltrixdb POD -it --image=busybox:1.36 -- nc -zv OTHER_POD 9000` — check CNI/NetworkPolicies |

---

## 4. GC death-spiral / garbage ratio > 65% (SEV-2)

```bash
# 1. Throttle writes
kubectl veltrix quota-set tenant_42 1000 5000000

# 2. Watch GC work through it (ratio per disk, runs, emergency state).
#    The bandwidth tiers are compile-time constants (200 MB/s at 50-65 %
#    garbage, uncapped at >= 65 %) — there is no runtime knob to raise them.
kubectl veltrix gc-status

# 3. Long-term: add disks (scale volumeClaimTemplates, re-roll)
```

The emergency-mode GC (Invariant 23) keeps the system upright automatically — your job is to reduce write rate before disk fills.

---

## 5. Backup & restore

```bash
# Full backup of a RUNNING server: through the admin API, written on the
# server's own filesystem (dest_dir is a path inside the pod)
kubectl port-forward -n veltrixdb POD 2112:2112 &
curl -s -X POST localhost:2112/admin/backup \
  -d '{"type":"full","dest_dir":"/mnt/nvme0/backup/'$(date +%F)'"}'
# incremental: {"type":"incremental","dest_dir":"...","base_dir":"<previous backup dir>"}
# (equivalent for a full backup: veltrix --addr 127.0.0.1:2112 backup DEST_DIR)

# veltrixdb-backup (cmd/backup) opens the data dirs with its OWN engine:
# run it only while the server is STOPPED. Pointed at a live server's
# directories it races the server and can truncate wal.log / vlog_active.dat.
# It is not in the server image (the Dockerfile builds only cmd/server).
veltrixdb-backup full --data-dirs=/mnt/nvme0,...,/mnt/nvme7 --dest=/backup/$(date +%F)

# Upload to S3 (prints the cloud path: <prefix>/<backup_id>/, default
# prefix veltrixdb-backups, backup_id from manifest.json, e.g. full-<unix-ns>)
veltrixdb-backup upload --src=/backup/$(date +%F) \
  --provider=s3 --bucket=my-bucket --region=us-east-1

# Download and restore (stop the engine first)
veltrixdb-backup download --provider=s3 --bucket=my-bucket --region=us-east-1 \
  --cloud-path=veltrixdb-backups/full-1748001600000000000/ --dest=/tmp/restore
veltrixdb-backup restore --chain=/tmp/restore --data-dirs=/data-new
```

Backups and PITR restores write a binary WAL; restoring into a build that predates the binary WAL needs the §8 procedure first. See [backup-restore.md](backup-restore.md).

Or via Kubernetes volume snapshots, if your CSI driver supports them (a
`VolumeSnapshot` manifest per PVC; there is no `kubectl create volumesnapshot`
subcommand). This repository ships no snapshot manifests. Run
`POST /admin/checkpoint` first so `wal.log` is a compacted checkpoint.

---

## 6. Encryption key rotation

Key rotation is **not implemented**. The engine loads exactly one key at
startup (`storage/encrypt.go`) and has no per-record key version, so values
written under the old key cannot be decrypted once the key is replaced, and
there is no command that re-encrypts in place. `kubectl veltrix migrate`
(`POST /admin/migrate`) only rewrites records whose schema version is older
than the current one; it does not re-encrypt.

The only way to change the key is to move the data through a client: read
every key from a node running with the old key and write it into a fresh
node (empty data dirs) started with the new key, then cut over. Do **not**
swap the Secret under existing data.

---

## 7. Value-transform metadata damage from a pre-fix build (SEV-1)

**Symptom.** Reads return wrong bytes, or fail with `vlog CRC32C mismatch`.
Affects nodes that ran a build predating the value-transform WAL fix, with
compression enabled (zstd is the **default**) or `--encrypt-at-rest`, and that
have restarted since writing.

Two distinct presentations, depending on how the node last shut down:

| Last shutdown | Presentation |
|---------------|--------------|
| **Clean** | Reads **succeed** and silently return the raw compressed or encrypted blob. No error, no metric, no alert. A 1040-byte value comes back as ~51 bytes starting `02 28 b5 2f fd` (zstd magic). |
| **Crash** | Reads **fail** with `vlog CRC32C mismatch`. |

The clean-shutdown case is the dangerous one — nothing surfaces it. If a value
over 256 bytes ever came back looking like binary garbage, this is why.

**Upgrading does not fix it.** The metadata was never written to the WAL, so a
new binary reconstructs the same wrong index. The data itself is fine: only
the metadata describing it was lost, which is why this repairs in place rather
than needing re-ingestion.

### Step 1 — assess (read-only, safe)

```bash
# whole fleet, one line per node
./scripts/repair-scan-fleet.sh --hosts hosts.txt --data-dirs /mnt/nvme0,/mnt/nvme1

# encrypted deployments need the key, or those records report as unresolved
VELTRIXDB_ENCRYPTION_KEY=... ./scripts/repair-scan-fleet.sh \
    --hosts hosts.txt --data-dirs /mnt/nvme0 --encrypt
```

Changes nothing. Exits non-zero if any node needs attention, so it can gate a
pipeline. It refuses to scan a host whose server is still listening.

### Step 2 — repair, per node, server STOPPED

```bash
kubectl scale statefulset veltrixdb --replicas=0     # or stop the unit
veltrix-repair --data-dirs /mnt/nvme0,/mnt/nvme1 --repair
# encrypted deployments: add --encrypt-at-rest (key from VELTRIXDB_ENCRYPTION_KEY
# or --encryption-key-path); non-zstd data: --compression=none|flate
```

Each `wal.log` is copied to `wal.log.prerepair.<timestamp>` before anything is
written. The repair is exact, not heuristic: `IndexEntry.CRC32C` is the CRC of
the **plaintext** and survived the bug, so the tool tries the four possible
interpretations of each on-disk blob and accepts only the one reproducing that
CRC. Anything else is reported and left untouched. It is idempotent. The
repaired `wal.log` is written in the binary WAL format (§8).

### Step 3 — verify

Restart and read back several values **larger than 256 bytes** (smaller ones
were never compressed and are unaffected). Re-run the scan; it should report
all healthy.

### If entries come back UNRESOLVED

No interpretation matched the stored CRC. Either the scan ran without the
encryption key — re-run with `--encrypt` and the correct key — or those
records are genuinely corrupt and need §5 restore-from-backup.

## 8. WAL format: upgrade and rollback

WAL records are binary by default. Replay reads binary and legacy text records
in any mix, so **upgrading needs no step**: the new build replays the existing
text WAL and appends binary records after it.

A build that predates the binary WAL cannot read it. To roll back:

```bash
# 1. On the CURRENT build, restart once writing the legacy text WAL
#    (logs "[wal] WARNING: --wal-format=text ...")
veltrixdb --wal-format=text ...        # add to the StatefulSet args, re-roll
# 2. Stop it CLEANLY (SIGTERM, not SIGKILL) — the shutdown checkpoint
#    rewrites wal.log as text
kubectl scale statefulset/veltrixdb --replicas=0 -n veltrixdb
# 3. Deploy the older build
```

If the node crashed instead of stopping cleanly, repeat steps 1–2. While
running with `--wal-format=text`, keys containing `|` or a newline are not
crash-safe: a crash can drop every acknowledged write after such a key.

## Contact

- On-call: `@veltrixdb-oncall` in PagerDuty
- Slack: `#veltrixdb-incidents`
- Runbook owner: SRE team — review quarterly
