# ConferenceProFlow — M3.6B Correctness & Resilience Review

**Milestone:** M3 — Kafka & Event Analytics  
**Stage:** M3.6B — Correctness & Resilience Review  
**Reviewed:** 2026-11-24  
**Branch:** `chore/enterprise-integration-hardening`  
**Status:** Static code and test-evidence review; not a new local test run

> **Evidence boundary:** This document is a targeted follow-up to `docs/ENTERPRISE_INTEGRATION_REVIEW.md` (M3.6A). It reviews currently available repository implementations for Kafka publishing, live Analytics consumption, replay, Registration reconciliation, Allocation decisions, and both RabbitMQ consumers, plus available Analytics test sources. It does **not** assert that new faults were reproduced locally, that every RabbitMQ publisher/test was exhaustively inspected, or that a green CI run proves every failure mode. Existing M2–M3.5 test/E2E results were reported by the developer.

## 1. Executive disposition

**No verified critical correctness bug requiring an architectural rewrite emerged from the inspected paths.** In the main Kafka processing paths, SQL persistence precedes offset commit, and duplicates are explicitly allowed at the transport layer but suppressed at the projection layer. RabbitMQ consumers ACK after durable inbox/reconciliation or after successful confirmed parking-queue publishing. Replay uses an isolated database, an independent consumer group and per-partition bounded windows.

**Two concrete operational risks merit narrow follow-up in M3.6E:**

- **R-01 — Replay manifest durability (P2 for this local tool).** The snapshot is stored under the operating-system temporary directory. If it disappears, `Resume=true` cannot reconstruct the original capture boundaries, and the runner correctly refuses to resume. The absence of fallback is safe but limits recoverability. **Disposition:** target for a small, tested improvement *only if* it can preserve the existing per-group lock, manifest validation and fresh-run safeguards. An explicit stable directory and clear backup/recovery instructions may suffice; SQL storage is not required.
- **R-02 — Poison Analytics outbox event visibility (P2).** The Kafka publisher retries validation failures together with transient broker failures using a 60-second capped backoff. `AnalyticsOutboxMessage` has a `QuarantinedAtUtc` field, but no quarantine transition is used by the inspected publisher. A permanently invalid row therefore remains pending and repeatedly fails, without a distinct terminal/escalated state. **Disposition:** a narrowly scoped alert/operator-visible classification or tested terminal handling policy; **never silently mark the event as published or delete it**.

**Do not expand M3.6 into a new distributed leasing implementation, Kafka high-availability cluster, automatic inventory provisioning or public API authentication project.** Record those limits as accepted scope boundaries.

## 2. Failure-mode traceability matrix

| ID | Failure window / invariant | Inspected implementation | Existing evidence | Disposition |
| --- | --- | --- | --- | --- |
| F-01 | Registration transaction commits but RabbitMQ is down: event must remain durable for eventual publish. | Registration-owned outbox architecture; exact RabbitMQ publisher failure branch not fully re-audited here. | Prior M2 local/CI validation reported. | **Verify later / no change yet**. Inspect outbox publisher confirms and failure path before declaring fully covered. |
| F-02 | Registration-request AMQP delivery duplicated: do not create duplicate allocation work. | `RegistrationRequestConsumerService` ACKs only for stored/recognized duplicates; conflicting/invalid messages use a confirmed parking publish before ACK. | Current consumer source. | **Reasonable code behavior;** retain existing tests; no change now. |
| F-03 | Allocation inventory missing: do not misclassify as full session. | `AllocationProcessingService` returns `WaitingForInventory` without final allocation. | Current processor source and reported M2 tests. | **Covered in implementation;** manual provisioning remains known scope limit. |
| F-04 | Two concurrent seat allocations: do not overbook. | Allocation decision retries entire operation after recognized row-version/unique/deadlock collisions; inventory, inbox and allocation are saved together. | Current processor source and previously reported tests. | **Covered in design and existing tests;** no new feature. |
| F-05 | Allocation result duplicated or contradictory: keep one final registration decision. | `AllocationResultReconciliationService` compares MessageId and business identifiers, commits final status + Result Inbox + Analytics Outbox together; result AMQP ACK follows reconciliation or confirmed parking. | Current reconciliation and consumer source. | **Covered in inspected code;** maintain regression test. |
| F-06 | Kafka ACK succeeds but SQL `PublishedAtUtc` save fails: a duplicate can be published. | Kafka publisher awaits `ProduceAsync` before saving publication state; original payload and MessageId reused. | Existing M3.3 Kafka integration test reported. | **Accepted at-least-once window;** downstream idempotency required. |
| F-07 | Kafka unavailable temporarily: Registration finalization must remain committed. | Kafka publisher leaves outbox pending and schedules bounded retries. | Current publisher; M3.3 manual E2E and tests reported. | **Covered for transient outages;** differentiate permanent payload errors (R-02). |
| F-08 | Analytics SQL transaction fails: Kafka offset must not advance. | Live consumer handles record in fresh DI scope and calls `Commit(record)` only after successful handler; exceptions restart consumer. | `SqlFailure_PreventsOffsetCommit_UntilDatabaseRecovers` integration test. | **Covered by code and named test.** |
| F-09 | SQL commits but Kafka offset commit fails: duplicate must not change statistics. | Processed-event ledger and projection are committed atomically; duplicate MessageId/RegistrationId classified without new increment. | `ExactDuplicateMessage_DoesNotIncreaseCounter`; `ValidDuplicateAndMalformedRecords_UpdateSqlExactlyOnce_ThenCommitOffsets`. | **Covered by code and named tests**, acknowledging no cross-system exactly-once guarantee. |
| F-10 | Malformed or conflicting Kafka event: do not silently lose or count it. | Handler routes validation/business conflicts to SQL quarantine; Kafka offset commit only follows persisted quarantine. | `MalformedOrIncompleteJson_IsQuarantinedOnlyOnceAtKafkaPosition`; `ConflictingFinalization_IsQuarantinedButOriginalProjectionIsUntouched`. | **Covered by named tests;** monitor quarantine as an operator concern. |
| F-11 | Replay interrupted after snapshot: resume same bounded windows with independent group. | Runner persists manifest, reads broker-committed offsets on resume and checks captured boundaries; SQL idempotency handles ambiguous commit. | `SqlFailure_PreventsCommit_ExplicitResumeKeepsOriginalSnapshotAndDoesNotCountTwice`. | **Covered when manifest survives;** R-01 remains. |
| F-12 | Kafka retention removes an offset required by replay: do not claim completion. | Runner compares current low watermark with required next offset and throws on a gap. | `MissingRetainedRecord_ResumeRefusesToDeclareCompleteOrSkipTheGap`. | **Covered by code and named test.** |
| F-13 | Events published after replay capture: do not include them in this replay. | Runner validates per-partition `[low, highExclusive)` and pauses completed partitions. | `ThreePartitions_DuplicatesQuarantineAndLateRecord_RespectCapturedHighWatermarksAndLiveIsolation`. | **Covered;** windows are not one globally atomic time cut. |
| F-14 | Accidental replay into live database/group: refuse operation. | Replay safety checks database names, same SQL server, distinct group prefix and disabled live consumer. | Replay safety and database isolation integration tests. | **Covered for intended local topology;** connection-string checks are not a substitute for least-privilege SQL credentials. |

## 3. Confirmed code properties versus untested assumptions

### Durable handling

**RabbitMQ operational consumers:** The inspected Registration Request consumer uses `autoAck: false`; a stored or recognized duplicate inbox record leads to `BasicAckAsync`, while invalid/conflicting deliveries go through publisher-confirm-enabled parking channels before ACK. The Allocation Result consumer follows the analogous pattern, ACKing applied/duplicate reconciliation only after it returns. A failure before ACK can lead to redelivery, which the inbox/reconciliation design must tolerate. This is *at-least-once processing*, not an assertion of exactly-once AMQP delivery.

**Kafka Analytics publisher:** `ProduceAsync` success precedes `MarkPublished()` and SQL `SaveChangesAsync()`. If the latter fails, the original outbox row can be republished. This is intentional and already covered by M3.3. However, serialization, type mismatch and contract-validation failures are caught in the same retry branch as broker outages. `QuarantinedAtUtc` exists as state but is not transitioned in that path. This is a concrete **observability/triage gap**, not demonstrated event loss.

**Live Analytics consumer:** Both Kafka auto-commit and auto-offset-store are off; a record is processed via `KafkaAnalyticsRecordHandler` and the consumer subsequently calls `Commit(record)`. On SQL or Kafka errors it restarts from broker-committed progress. SQL transaction and unique constraints protect counters against replay. A durable quarantine is also an acceptable processing outcome. Tests explicitly cover SQL failures, duplicates and malformed records.

**Replay:** A new run captures watermarks *per partition* and stores a manifest before consuming. Resume verifies the original capture and trusts broker-committed offsets, not optimistic manifest progress. A missing file fails closed. Retention gaps fail closed. The current OS-TEMP path is a durability limitation; **do not delete/reset an existing replay manifest or offset group to work around it**.

### What this audit does *not* prove

- The complete RabbitMQ outbox publisher confirmation/unroutable-return behavior was not revalidated against every current test in this stage.
- Broker/SQL failover, multi-replica Kafka consumer rebalancing and simultaneous publisher replicas are not demonstrated production guarantees.
- Kafka replay watermarks are captured sequentially per partition: this is a bounded offset **vector**, not a globally atomic snapshot of the topic.
- Local error logs and tests do not replace durable metrics/alerts for production operations.

## 4. Prioritized actions

| Priority | Item | Proposed handling | Acceptance evidence |
| --- | --- | --- | --- |
| P2 | R-01 Manifest storage | Consider configurable stable local replay manifest directory with explicit no-overwrite/lock semantics; retain temp fallback only if documented. | Test that path is stable/configurable, resume uses same manifest and unsafe group reuse is refused. |
| P2 | R-02 Poison outbox visibility | Add bounded explicit classification/escalation or structured high-severity logging/inspection query for permanent contract failures; distinguish transient Kafka failures. | Test unchanged payload stays pending or explicitly quarantined with a reason; no false `PublishedAtUtc`. |
| P2 / accepted | Single-active-instance publisher and consumer baseline | Document instead of adding leasing/rebalance infrastructure. | Architecture README clearly states limitation. |
| P2 / accepted | Manual Allocation inventory provisioning | Keep guarded local seed and state limitation. | Fresh-start guide clearly distinguishes Registration session and Allocation inventory. |
| P1 only if Internet-deployed | Public API authentication/authorization | Do not expose to public Internet; future milestone if productization is desired. | Document scope warning, no misleading production-ready claim. |
| P3 | Replay snapshot semantics | Clarify per-partition snapshot in docs. | README and Replay doc use accurate wording. |

## 5. Read-only local verification (optional)

Run the accompanying `docs/ENTERPRISE_RESILIENCE_CHECKS.sql` from **Visual Studio → SQL Server Object Explorer**, after choosing your **Registration database**. These queries do not modify SQL rows, Kafka offsets or replay manifests. They highlight unpublished Analytics Outbox events, repeated failures and how many attempts were made; they are **observations**, not automated fault injection.

Do not start destructive outage scenarios or reset consumer groups during M3.6B. We already have focused Testcontainers tests for the high-value scenarios; new tests will be targeted only after deciding on a concrete hardening change.

## 6. M3.6B outcome / next gate

**Decision:** keep existing app behavior and schema unchanged in M3.6B. The immediate next step is **M3.6C Configuration & Security Hygiene**, followed by M3.6D reproducibility and **M3.6E targeted changes only for approved risks**. Do not treat R-01/R-02 as proof that current demos are broken; they are scope-conscious operational shortcomings worth improving or documenting.

### Source anchors

- Registration Kafka publisher: `src/ConferenceProFlow.Registration.Api/Infrastructure/Outbox/KafkaAnalyticsOutboxPublisherService.cs`
- Analytics outbox state: `src/ConferenceProFlow.Registration.Api/Infrastructure/Outbox/AnalyticsOutboxMessage.cs`
- Registration reconciliation: `src/ConferenceProFlow.Registration.Api/Infrastructure/Inbox/AllocationResultReconciliationService.cs`
- Registration Request RabbitMQ consumer: `src/ConferenceProFlow.Allocation.Worker/Messaging/Consumers/RegistrationRequestConsumerService.cs`
- Allocation Result RabbitMQ consumer: `src/ConferenceProFlow.Registration.Api/Messaging/Consumers/AllocationResultConsumerService.cs`
- Allocation decision engine: `src/ConferenceProFlow.Allocation.Worker/Processing/AllocationProcessingService.cs`
- Live Kafka consumer: `src/ConferenceProFlow.Analytics.Worker/Messaging/Consumers/KafkaAnalyticsConsumerService.cs`
- Analytics processing + quarantine: `src/ConferenceProFlow.Analytics.Worker/Application/Processing/`
- Replay runner + snapshot: `src/ConferenceProFlow.Analytics.Worker/Replay/`
- Test evidence: `tests/ConferenceProFlow.Analytics.IntegrationTests/{Messaging,Processing,Replay}/`

**No application code, EF migration or Kafka configuration changes are required to adopt this review.**
