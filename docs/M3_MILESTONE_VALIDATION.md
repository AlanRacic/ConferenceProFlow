# ConferenceProFlow — M3 Milestone Validation and Limitations

**Milestone:** M3 — Kafka & Event Analytics  
**Work item:** M3.6F — Final Enterprise Validation and Documentation  
**Date:** 25 November 2026  
**Status:** M3.1–M3.5 implemented/merged; M3.6 implementation and local validation completed; final M3.6 Pull Request and independent GitHub Actions CI **pending**.

## 1. Scope of this record

This document summarizes user-reported execution evidence and audited code/documentation from M3.6. It is **not** a claim that the repository is production-ready, publicly secured or externally independently audited. The latest source changes exist on branch `chore/enterprise-integration-hardening` until merged.

## 2. Implemented architecture

```text
Registration API  →  RabbitMQ  →  Allocation Worker
       ↑                 ← allocation.decided.v1 ←  |
       |                                         |
       └── Registration reconciliation  ←────────┘
                  |
                  +── Registration Analytics Outbox (SQL transaction)
                            |
                            v
                       Kafka Publisher
                            |
                            v
                     Apache Kafka (3 partitions locally)
                            |
                 +──────────+─────────────+
                 |                        |
                 v                        v
            Live consumer          Bounded replay runner
                 |                        |
                 v                        v
         ConferenceProFlow        ConferenceProFlow
             Analytics              AnalyticsReplay
         (live SQL database)    (isolated SQL database)
```

RabbitMQ handles the operational registration/allocation workflow. Kafka publishes finalized registration events for asynchronous projections. Registration, Allocation, Live Analytics and Replay each use distinct **logical** SQL databases on the same Docker-hosted SQL Server instance. No distributed SQL transaction spans these databases.

## 3. Evidence matrix (reported local results)

| Check | Result reported | Evidence boundary |
|---|---|---|
| M3.3 Kafka publisher Testcontainers | 4 passed | Includes ACK-before-failed-SQL-state update and redelivery |
| M3.4 Analytics integration tests | 30 passed during M3.4 | SQL processing, duplicate handling, quarantine, manual offsets |
| M3.5 Replay Runner Testcontainers | 6 passed after clean rebuild | Multi-partition bounds, SQL failure/resume, retention gap |
| M3.5 manual replay | Passed | Fresh session 2 confirmed / 1 rejected, three matching events, unchanged live group offsets |
| M3.6A architecture audit | Documented | Static review, not independent security certification |
| M3.6B resilience and Analytics Outbox SQL checks | Documented; no pending events at point of inspection | A clean point-in-time query is not a future-availability guarantee |
| M3.6C configuration/secret and Compose checks | Passed locally | Does not constitute a penetration test |
| M3.6D clean-clone Restore/Build | Passed locally | Did not re-provision all infrastructure in isolation |
| M3.6E1 snapshot path tests + Replay Runner 6-case regression | Passed locally | LocalApplicationData override behavior, replay safeguards preserved |
| M3.6E2 Outbox alert policy unit + publisher 4-case regression | Passed locally | Logging only; delivery semantics preserved |
| M3.6F solution Release build + full regression | Passed locally | Total test count not claimed; final GitHub Actions CI pending |

Test runs were performed by the project developer in the local Windows/.NET 10/Docker Desktop environment. The named CI checkpoint is intentionally left open until the final PR checks complete.

## 4. Targeted changes in M3.6

### 4.1. Replay snapshot storage

Previous path: OS temp directory (`%TEMP%`). New Windows default: `%LOCALAPPDATA%\ConferenceProFlow\ReplaySnapshots`, or an explicit absolute `AnalyticsReplay:SnapshotDirectory`. No automatic migration of legacy snapshots or modifications to SQL databases/Kafka offsets. This reduces accidental deletion but is not redundant storage, backup or high availability. See [Replay Snapshot Storage](REPLAY_SNAPSHOT_STORAGE.md).

### 4.2. Kafka Analytics Outbox operational visibility

Existing retry/backoff and Kafka-ACK-then-SQL-publication-state rules remain unchanged. Added diagnostic `PayloadValidation`, `KafkaDelivery`, and `Unexpected` categories; warning-level structured logs and error-level alerts starting at the configured failure threshold (default 5, then every 10 additional failures). Pending events are **not** deleted, auto-quarantined or marked published on failure. Alerts are logs, **not** notifications. See [Kafka Outbox Operational Visibility](KAFKA_OUTBOX_OPERATIONAL_VISIBILITY.md).

### 4.3. Reproducibility and document hygiene

Correct local first-time order: set up `.env`/User Secrets → start SQL Server/RabbitMQ via Docker → apply existing migrations → start apps → run guarded demo. The optional Kafka profile and Analytics components require their documented additional steps. Do not run destructive `docker compose down -v` or reset Kafka live group offsets as a routine setup step.

## 5. Residual risks and accepted scope boundaries

| Area | Limitation | Scope decision |
|---|---|---|
| Kafka delivery | At least once across SQL/Kafka; duplicates possible | Mitigated by stable MessageId and idempotent SQL consumers; no exactly-once claim |
| Outbox concurrency | Single active Kafka publisher assumed | Multi-instance claiming/leasing deferred |
| Replay retention | Kafka topic retains data for a limited period (seven days in initial local config) | Only retained records are replayable; cannot prove complete historical rebuild |
| Replay snapshot | Per-partition boundaries taken at different moments | Not a globally atomic, synchronized Kafka-wide snapshot |
| Replay storage | Per-user local directory; no backup/replication | Suitable for local demonstrations only |
| Quarantine and alerting | Diagnostic log alerts, read-only SQL inspection, no external monitoring | Operational integrations deferred |
| Security | Development Kafka PLAINTEXT, local secrets, no public API auth | Never expose stack as a public production service |
| Provisioning | Development-only guarded Allocation inventory SQL seed | Automated business provisioning deferred |
| Local setup | Requires manual secrets, migrations and selected startup services | Not a one-command production bootstrap |
| Testing | Tests cover documented scenarios, not all possible failures | No comprehensive load/chaos/security certification |

## 6. Release gate for M3.6 PR

1. Verify all new M3.6 documents are under one `docs/` directory; optional Solution Items links use repository-relative paths.
2. Review staged changes and ensure no `.env`, User Secrets, replay manifests, local DB data, or `bin/obj` outputs are tracked.
3. Confirm local `dotnet build ConferenceProFlow.slnx --configuration Release` and `dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal` passed.
4. Stage changes and run `git diff --cached --check` plus `git diff --cached --stat`.
5. Create PR from `chore/enterprise-integration-hardening` into `master`, reference the **actual** M3.6 issue number, and assign milestone M3.
6. Inspect GitHub Actions **test execution**, not only green build summary; merge only after green checks.
7. Confirm issue closure, update local `master`, and close milestone M3 **only after** all its issues are complete.

## Related documentation

- [Enterprise Integration Review](ENTERPRISE_INTEGRATION_REVIEW.md)
- [Enterprise Resilience Review](ENTERPRISE_RESILIENCE_REVIEW.md)
- [Configuration and Security Review](CONFIGURATION_SECURITY_REVIEW.md)
- [Local Reproducibility Review](LOCAL_REPRODUCIBILITY_REVIEW.md)
- [Replay Snapshot Storage](REPLAY_SNAPSHOT_STORAGE.md)
- [Kafka Outbox Operational Visibility](KAFKA_OUTBOX_OPERATIONAL_VISIBILITY.md)
- [Local Development](LOCAL_DEVELOPMENT.md)
