# VeltrixDB — SOC 2 Trust Service Criteria Mapping

This document maps VeltrixDB's built-in capabilities to the SOC 2 Trust Service Criteria (TSC). Provided as a starting point for an auditor's evidence-gathering; **a SOC 2 attestation is a property of your operating environment, not of VeltrixDB alone**. The table below states what the database provides and what the operator must implement to satisfy each criterion.

| TSC | Criterion summary | VeltrixDB capability | Operator obligation |
|-----|---|---|---|
| CC6.1 | Logical access controls | RBAC (`-auth-config`), per-namespace tenancy, mTLS (`-tls-cert`/`-tls-key`/`-tls-ca`) | Provision unique users per service, rotate credentials, deny network ingress to admin port |
| CC6.6 | Restrict data transmission to authorized parties | TLS 1.3 listener (`-tls-addr`), mTLS client cert verification | Use mTLS in production; load only signed certs |
| CC6.7 | Encrypted data at rest | AES-256-GCM at rest (`EncryptionEnabled=true`); key sourced from `VELTRIXDB_ENCRYPTION_KEY` env or `EncryptionKeyPath` file | Manage the master key in KMS / sealed secret; never commit key material to source |
| CC6.8 | Anti-malware / image hygiene | Single-binary distribution; `Dockerfile` is FROM scratch + minimal CA bundle | Scan published images (Trivy / Snyk); pin SHA256 in Helm chart |
| CC7.1 | Detection of vulnerabilities | `govulncheck` runs in CI (`.github/workflows/ci.yml`) | Triage vulncheck output weekly; subscribe to Go security mailing list |
| CC7.2 | Monitoring of components | Prometheus metrics (130+ series), `/healthz` and `/readyz` endpoints, `PrometheusRule` for 22 default alerts (write/read latency, GC, scrub corruption, slow disk, replication, cluster) | Run a Prometheus / Alertmanager stack with paging integration; review alert noise quarterly |
| CC7.3 | Anomaly identification | Slow-op trace ring buffer at `/traces` (50 ms threshold); per-disk latency EWMA; scrub corruption counter | Define normal-baseline; alert on deviations |
| CC7.4 | Security incident response | DR runbook (`docs/DR_RUNBOOK.md`) | Rehearse the runbook quarterly; capture post-mortems |
| CC7.5 | Recovery from incidents | Backup via VolumeSnapshot; replica resync; raw-mode crash recovery (Invariant 27); WAL replay | Test restore at least twice a year; maintain quorum across availability zones |
| CC8.1 | Authorization of changes | Helm chart values are versioned; `cluster.replicationFactor` is enforced via PDB | Require PR review on all `values.yaml` changes; block deploys outside change windows |
| A1.1 | Process capacity | Per-disk throughput / latency / file-bytes metrics; admission control with documented thresholds (Invariant 22) | Capacity plan against `vlog_file_bytes` growth; pre-provision before crossing 70 % |
| A1.2 | Backup of data | VLog + WAL durably fdatasync'd; WAL group commit (Invariant 19, 20); checkpoint endpoint at `/admin/checkpoint` | Schedule snapshot job; verify by occasional restore-to-staging |
| A1.3 | Disaster recovery testing | Soak script at `scripts/soak.sh`; bench harness `scripts/bench.sh` regression-gates packing density and GC emergency | Run soak monthly to detect drift; gate releases on bench pass |
| C1.1 | Confidentiality of information | At-rest encryption + audit log of mutating ops (`AuditLogPath` config) | Ship audit log to immutable retention (Loki / S3 Object Lock); restrict who can read it |
| C1.2 | Disposal of confidential information | `BLKDISCARD` on raw NVMe (Invariant 26); `fallocate(PUNCH_HOLE)` on file-backed disks during VLog GC | Document key destruction in tenant offboarding |
| PI1.1 | Processing integrity inputs | Binary protocol size caps; per-namespace quotas (`SetNamespaceLimit`); rate-limit on writes | Enable quotas per tenant from day one |
| PI1.4 | Output completeness/accuracy | CRC32C on every VLog record (write + read verification); background scrubber re-validates (50 MB/s default) | Alert on `scrub_corruption_total > 0` (already in chart) |
| PI1.5 | Processing integrity errors | Admission control + GC emergency logs; scrubber emits `[scrub] disk=N offset=O ...` for every detection | Forward error logs to SIEM; on-call must be paged on corruption |

## Audit evidence checklist

For an auditor in the room, the operator should be able to produce:

1. **Encryption proof** — `kubectl exec POD -- env | grep VELTRIXDB_ENCRYPTION_KEY` (key value redacted in output) and a `kubectl describe secret veltrixdb-encryption` showing the secret exists.
2. **Access control proof** — `cat /var/lib/veltrixdb/auth.json` showing roles + users, and a sample successful + failed login traces from server logs.
3. **TLS proof** — `openssl s_client -connect veltrixdb.svc:9443 -showcerts` showing the chain. mTLS proof: an attempt without a client cert is rejected.
4. **Backup proof** — `kubectl get volumesnapshot -n veltrixdb` listing snapshots within the last 7 days.
5. **Audit log proof** — `tail -100 /var/log/veltrixdb/audit.jsonl` (NDJSON), and a query showing the audit log has been shipped to Loki / Splunk for the last 30 days.
6. **Monitoring proof** — Grafana dashboard JSON (`grafana/`) and a screenshot of an alert firing in the past 90 days.
7. **DR proof** — minutes of the most recent DR drill (run quarterly per the runbook).
8. **Vulnerability scanning proof** — most recent `govulncheck` output (CI artifact) and the resolution status of any findings.
9. **Change management proof** — recent PR reviews on `VeltrixDB-Helm-Chart/values.yaml` showing two-person sign-off.
10. **Capacity proof** — Prometheus query for `veltrixdb_vlog_file_bytes` over 30 days showing < 70 % of allocated PVC.

## What VeltrixDB does NOT yet provide (gaps to disclose)

- **Tamper-evident audit log**: the audit JSONL is appended locally and shipped externally; it is not cryptographically chained. If your control framework requires hash-chained audit, ship the log to an immutable backend (S3 Object Lock, Worm-mounted volume).
- **Field-level encryption**: at-rest encryption applies to whole values. If you need per-field encryption (column-level), implement at the application layer.
- **HSM / KMS integration**: the master key is loaded from an environment variable. Use Kubernetes Secrets backed by a CSI driver (e.g. AWS Secrets Manager CSI) to source the key from a KMS at pod startup; do not store the raw key in the cluster.
- **PII detection / redaction**: no built-in scanner. If applicable, run an external DLP scanner over the audit log.
