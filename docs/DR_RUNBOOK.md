# VeltrixDB Disaster Recovery Runbook

> Assumes `kubectl` admin rights and pods labelled `app.kubernetes.io/name=veltrixdb` in `-n veltrixdb`.

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
| `WAL replay corruption at offset N` | Partial write on crash | Restart pod once; if recurring: `kubectl veltrix sync-from REPLICA_POD` |
| `cannot allocate memory` (vlog open) | Hugepages missing | `kubectl exec POD -- sysctl vm.nr_hugepages` — must be ≥ 512 |
| `bad magic at offset O` | Silent disk corruption | Drain node, replace disk, re-join |
| `encryption: no key in VELTRIXDB_ENCRYPTION_KEY` | Missing secret | `kubectl create secret generic veltrixdb-enc --from-literal=key=BASE64_32B` |

---

## 2. Data corruption (SEV-0)

```bash
# Confirm
kubectl exec -n veltrixdb POD -- curl -s localhost:2112/metrics | \
  grep -E 'scrub_corruption_total|vlog_gc_read_errors_total'

# Find the bad disk
kubectl logs -n veltrixdb POD | grep '\[scrub\] disk='

# Quarantine the pod
kubectl label pod -n veltrixdb POD veltrixdb.io/quarantine=true --overwrite

# Wipe and restart (replication refills automatically)
kubectl exec -n veltrixdb POD -- rm -rf /mnt/nvme*/vlog_active.dat /mnt/nvme*/wal_*
kubectl delete pod -n veltrixdb POD

# After replay completes
kubectl label pod -n veltrixdb POD veltrixdb.io/quarantine- --overwrite

# Root-cause
kubectl exec -n veltrixdb POD -- smartctl -a /dev/nvme0n1
```

---

## 3. Write outage (SEV-0)

```bash
kubectl get pods -n veltrixdb -o wide
kubectl exec -n veltrixdb POD -- df -h /mnt/nvme*
kubectl exec -n veltrixdb POD -- curl -s localhost:2112/metrics | grep wal_flushes_total
```

| Cause | Fix |
|-------|-----|
| Disk 100% full + GC paused | `kubectl veltrix checkpoint` |
| GC can't keep up | `kubectl veltrix quota-set NS 1000 5000000` — throttle writes |
| Network partition | `kubectl exec POD -- nc -zv OTHER_POD 9000` — check CNI/NetworkPolicies |

---

## 4. GC death-spiral / garbage ratio > 65% (SEV-2)

```bash
# 1. Throttle writes
kubectl veltrix quota-set tenant_42 1000 5000000

# 2. Raise GC budget if hardware has headroom
kubectl set env statefulset/veltrixdb -n veltrixdb \
  VELTRIXDB_GC_CRITICAL_BPS=300000000   # 300 MB/s

# 3. Long-term: add disks (scale volumeClaimTemplates, re-roll)
```

The emergency-mode GC (Invariant 23) keeps the system upright automatically — your job is to reduce write rate before disk fills.

---

## 5. Backup & restore

```bash
# Full backup (engine can be running)
veltrixdb-backup full --data-dirs=/mnt/nvme0,...,/mnt/nvme7 --dest=/backup/$(date +%F)

# Upload to S3
veltrixdb-backup upload --src=/backup/$(date +%F) \
  --provider=s3 --bucket=my-bucket --region=us-east-1

# Download and restore (stop the engine first)
veltrixdb-backup download --provider=s3 --bucket=my-bucket \
  --cloud-path=veltrixdb-backups/$(date +%F) --dest=/tmp/restore
veltrixdb-backup restore --chain=/tmp/restore --data-dirs=/data-new
```

Or via Kubernetes volume snapshots:
```bash
kubectl create volumesnapshot veltrixdb-disk-0 -n veltrixdb \
  --persistentvolumeclaim=data-veltrixdb-0
```

---

## 6. Encryption key rotation

Online rotation is not supported. Use the offline sequence:

1. Stop all pods: `kubectl scale statefulset/veltrixdb --replicas=0 -n veltrixdb`
2. Generate new key: `openssl rand 32 | base64`
3. Update secret: `kubectl create secret generic veltrixdb-enc --from-literal=key=NEW_KEY --dry-run=client -o yaml | kubectl apply -f -`
4. Start pods and run: `kubectl veltrix migrate` (forces full rewrite at new key)

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
```

Each `wal.log` is copied to `wal.log.prerepair.<timestamp>` before anything is
written. The repair is exact, not heuristic: `IndexEntry.CRC32C` is the CRC of
the **plaintext** and survived the bug, so the tool tries the four possible
interpretations of each on-disk blob and accepts only the one reproducing that
CRC. Anything else is reported and left untouched. It is idempotent.

### Step 3 — verify

Restart and read back several values **larger than 256 bytes** (smaller ones
were never compressed and are unaffected). Re-run the scan; it should report
all healthy.

### If entries come back UNRESOLVED

No interpretation matched the stored CRC. Either the scan ran without the
encryption key — re-run with `--encrypt` and the correct key — or those
records are genuinely corrupt and need §5 restore-from-backup.

## Contact

- On-call: `@veltrixdb-oncall` in PagerDuty
- Slack: `#veltrixdb-incidents`
- Runbook owner: SRE team — review quarterly
