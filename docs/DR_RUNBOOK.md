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

## Contact

- On-call: `@veltrixdb-oncall` in PagerDuty
- Slack: `#veltrixdb-incidents`
- Runbook owner: SRE team — review quarterly
