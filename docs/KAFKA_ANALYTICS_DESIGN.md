# ConferenceProFlow — Kafka Event Analytics Design

**Milestone:** M3 — Kafka & Event Analytics  
**Work item:** M3.1 — Kafka Analytics Architecture  
**Status:** Reviewed architecture proposal for M3.1 (implementation pending; no Kafka runtime or analytics service implemented yet)  
**Baseline:** M2.6 Registration ↔ Allocation messaging via RabbitMQ, independent SQL Server databases, Transactional Outbox / Inbox, final registration reconciliation.

## 1. Purpose and business requirement

The Analytics component will independently build operational statistics from **committed business events**. It must not query Registration or Allocation operational databases. An initial, deliberately narrow outcome is a queryable count of **confirmed and rejected registrations by session**, with a traceable event-processing history.

This follows Project Charter FR-007 (Business Events), FR-008 (Analytics), and the local, no-paid-cloud reproducibility constraint. The Analytics component must not decide whether a seat is available, change a registration status, or participate in the live registration transaction.

## 2. Existing system and proposed extension

**Implemented in M2.6:**

```text
Registration API → Registration SQL Outbox → RabbitMQ → Allocation Worker
Allocation Worker → Allocation SQL Outbox → RabbitMQ → Registration API
Registration API → Allocation Result Inbox + Registration final status (local atomic commit)
```

**Proposed for M3:**

```text
Allocation result received by Registration API
  → local Registration SQL transaction:
       update Registration status
       + insert Allocation Result Inbox receipt
       + insert *Analytics Kafka Outbox* record (only for a new finalization)
  → dedicated Kafka publisher (retries independently of RabbitMQ)
  → Kafka topic: conferenceproflow.registration-finalized.v1
  → Analytics Worker (consumer group)
  → single Analytics SQL transaction:
       insert ProcessedAnalyticsEvent
       + update SessionRegistrationStats
  → commit Kafka offset only AFTER SQL commit
```

**Architecture decision AD-01:** the source of analytics is the **final registration state**, not an intermediate Allocation decision. An event is inserted only when a Registration transitions from `Pending` to `Confirmed` or `Rejected` in the same SQL transaction as the status update and Registration-side result Inbox receipt. Duplicate reconciliation of an already-finalized Registration must not create a second event. If reconciliation is delayed, analytics is correspondingly delayed (eventual consistency).

## 3. Responsibility and ownership boundaries

| Component | Owns | Must not do |
|---|---|---|
| Registration API | Authoritative Registration status; finalization event creation; dedicated Kafka outbox | Write analytics database or make capacity decisions |
| Allocation Worker | Capacity, allocation decision and its RabbitMQ publication | Write Registration or Analytics databases |
| Kafka | Retained, partitioned transport of analytics events | Act as the source of truth for Registration status |
| Analytics Worker | Kafka consumption, event deduplication, per-session projections | Change Registration or Allocation records |
| Analytics SQL database | Processed event IDs, projection counts | Replace the operational databases |

Analytics can use an independent SQL Server database on the **existing local SQL Server container**. Separate database ownership does not require a new SQL Server container.

## 4. Proposed integration contract

**Event:** `RegistrationFinalizedV1` (new; proposed for M3)  
**Kafka topic:** `conferenceproflow.registration-finalized.v1`  
**Kafka message key:** `SessionId` serialized consistently as a canonical GUID string.  
**Value:** JSON, camelCase; contract and schema version defined independently of entity classes.

| Field | Type | Meaning |
|---|---|---|
| `messageId` | Guid | Stable event identity, generated once when analytics outbox row is created |
| `registrationId` | Guid | Business registration identity |
| `sessionId` | Guid | Session for grouping analytics |
| `status` | string | Exactly `confirmed` or `rejected` |
| `occurredAtUtc` | DateTimeOffset | UTC timestamp assigned when the status transition and Outbox message are recorded; not an exact database commit timestamp |

**Illustrative payload (fictional identifiers):**

```json
{
  "messageId": "84f99e64-c60c-45aa-929b-1a267a33911f",
  "registrationId": "7be88de0-21e7-485b-9283-8eb2603b3112",
  "sessionId": "d7263cac-475f-40a3-b149-a8c4db117c69",
  "status": "confirmed",
  "occurredAtUtc": "2026-11-20T10:00:00+00:00"
}
```

**Privacy:** no participant name, email address or other personal information is required for the initial aggregate counts. The initial analytics contract intentionally excludes such fields. Rejection reason is also excluded from the initial contract to minimize data and scope.

**Semantics:** exactly one *logical finalization* event per business registration, with a database uniqueness guard on the Registration-owned analytics outbox business key (e.g., `RegistrationId` for this event type). Transport delivery may still duplicate the event. Existing duplicate reconciliation decisions **must not** generate another analytics event. If future requirements allow cancellations or state changes after finalization, this contract and projection rules require explicit evolution; this version does not model them.

## 5. Why a separate Kafka publication state?

The M2 Outbox records already track **RabbitMQ** publication status. Marking the same message `PublishedAtUtc` after RabbitMQ confirmation would be unsafe if that status were also used to mean "published to Kafka". An outage of Kafka could then silently lose analytics publication.

**Architecture decision AD-02:** use a Registration-owned `AnalyticsOutboxMessages` table with its **own** `PublishedAtUtc`, retry scheduling and failure metadata. Its row is inserted via the existing Registration `DbContext` and committed in the **same Registration SQL transaction** as the final status and result Inbox receipt; do not introduce another database or a separate SQL write for event creation. The event payload and stable `messageId` are persisted together, so retries publish the same logical event.

This is deliberately a new event (`RegistrationFinalizedV1`) with different semantics from the existing `AllocationDecidedV1` and `RegistrationRequestedV1` contracts. We do **not** copy the Allocation broker result directly into Kafka and call it a committed registration status.

The Kafka publisher sends each outbox payload using a stable `messageId` and the session key. Require acknowledgement of successful broker publication (producer configured for reliable acknowledgements; implementation details and package version are selected in M3.3). Only then is the *Kafka-specific* publication state updated in Registration SQL. If Kafka accepts the event but the SQL update fails, publication may repeat: the Analytics consumer must deduplicate it. No cross-system two-phase transaction or exactly-once delivery is claimed.

**Constraints:** begin with one active Kafka publisher instance; avoid claiming multi-replica outbox dispatch safety until a claim/lease strategy is implemented and tested. No two-phase commit between SQL and Kafka is assumed.

## 6. Kafka topic, partitioning, retention and consumer group

| Setting | Initial proposal | Reason |
|---|---|---|
| Topic | `conferenceproflow.registration-finalized.v1` | Explicit purpose and contract version |
| Partition key | `SessionId` | Related events for one session route to one partition; ordering exists only *within* a partition |
| Partitions | 3 for local exercises, configurable | Demonstrate partitioning and consumer assignment without overcomplicating the demo |
| Replication factor | 1 locally | Single-broker developer topology; not a fault-tolerant production deployment |
| Retention | 7 days initially, configurable | Bounded local replay/testing; events beyond retention cannot be reconstructed from Kafka alone |
| Consumer group | `conferenceproflow.analytics.v1` | Analytics workers share work inside one group |
| Consumer client | `Confluent.Kafka` for .NET (version to be selected in M3.4) | Standard .NET Kafka integration |
| Cluster | Single-node Kafka in KRaft mode via Docker Compose | No ZooKeeper or paid cloud service in initial demo |

**Architecture decision AD-03:** use `SessionId` as the key, three partitions locally, and seven-day retention. This gives per-session ordering *within one Kafka partition* but no total order across partitions. Very large sessions could create a hot partition; for the initial per-session analytics this is an acceptable, measurable assumption. The single-node KRaft broker is for learning and reproducibility, not broker fault tolerance.

## 7. Analytics persistence and idempotency

**Database:** `ConferenceProFlowAnalytics` (independent owner; same local SQL Server infrastructure is acceptable).

**Proposed tables:**

1. `ProcessedAnalyticsEvents`: `MessageId` (PK), `RegistrationId` (unique), `SessionId`, `Status`, `ProcessedAtUtc`. Durable deduplication, per-registration business identity and traceability.
2. `SessionRegistrationStats`: `SessionId` (PK), `ConfirmedCount`, `RejectedCount`, `UpdatedAtUtc` (optional row version or equivalent concurrency strategy).
3. `QuarantinedAnalyticsEvents`: `(Topic, Partition, Offset)` unique, `MessageKey`, bounded raw `Payload`, `Reason`, `ReceivedAtUtc`. Durable record of malformed, unsupported or conflicting events that are deliberately not applied to projections. Apply safe payload size limits and avoid logging unbounded message content.

For each valid new message, one Analytics SQL transaction must **insert the processed event and update the aggregate together**. Atomic counter increments and unique database constraints (not application-level read/modify/write alone) must protect against concurrent consumers. On duplicated `MessageId` or the same compatible final decision for `RegistrationId`, return success without incrementing counters. A conflicting finalization for the same `RegistrationId` is **not** a second count: persist it in `QuarantinedAnalyticsEvents` for investigation. Do not modify the original processed decision.

The initial aggregate counts **finalized registrations**; it does not claim to count all requests still in `Pending`. Statistics over time can initially be produced from the persisted `ProcessedAnalyticsEvents` records; precomputed time buckets can be added only if justified by a measured query need.

**Architecture decision AD-05 (replay demo):** run a replay worker with an **independent consumer group** (e.g., `conferenceproflow.analytics.replay.<run-id>`) and an **isolated replay SQL database** on the same local SQL Server instance. Process retained events into new empty projections and compare with the normal Analytics database; do not reset or delete live projection tables. This reconstructs only events still covered by Kafka retention (seven days by default). If the required events have expired, the replay cannot be presented as a complete rebuild. Implement and test this controlled demo in M3.5.

## 8. Delivery, offset and failure policy

- **At-least-once intent:** a valid message is processed durably before its offset is committed. Kafka offsets are not assumed to be in the same SQL transaction; duplicates after a crash are possible.
- **Architecture decision AD-04 (offsets):** for the first implementation, use `Confluent.Kafka` with `EnableAutoCommit = false`, `EnableAutoOffsetStore = false`, and an explicit `Commit(consumeResult)` **only after** the Analytics SQL transaction succeeds or a duplicate is safely recognized. A failed offset commit is allowed to cause redelivery; SQL deduplication makes it safe. Do not commit beyond unprocessed records; handle rebalances and partition assignment loss conservatively. This per-message commit approach favors clarity over throughput; consider batched offset management only after measuring performance.
- **Invalid/unknown/conflicting events:** do not count them and do not silently skip them. Store the Kafka topic, partition, offset, bounded payload and reason in the durable SQL `QuarantinedAnalyticsEvents` table. Commit the Kafka offset **only after** that quarantine write succeeds. If quarantine persistence fails, do not commit; retry when infrastructure recovers. An error/parking Kafka topic is optional future work, not needed in M3.4.
- **Transient SQL errors:** leave the Kafka offset uncommitted, log and retry/back off without processing later records from the failed partition as though the earlier record were complete.
- **Kafka unavailable:** finalized Registration changes still commit with Analytics Outbox pending; retry publication after Kafka recovery.
- **Analytics worker unavailable:** Kafka retains events within the configured retention window; consumer catches up after restart.
- **Exactly-once:** no claim of exactly-once transport or end-to-end SQL/Kafka transactions. The practical guarantee sought is *effectively once for supported analytics projections* through SQL deduplication.
- **Reconciliation remains independent:** analytics failure never changes the final Registration/Allocation business outcome.

## 9. Testing and local demonstration acceptance

The implemented M2.6 workflow stays functional when Kafka is disabled. A full M3 demonstration will use the same test conference pattern (two seats, three registrations), then verify:

- Registration and Allocation produce **2 Confirmed / 1 Rejected** as before.
- Analytics projection reaches **ConfirmedCount = 2, RejectedCount = 1** for that session.
- Duplicate publication/delivery does **not** increment counters twice.
- An Analytics Worker restart catches up with retained Kafka events.
- A Kafka outage does not undo committed Registration statuses; Analytics Outbox retries after recovery.
- A controlled replay using an isolated SQL database and new consumer group yields the same counts for the *retained* test dataset, without changing live projections.
- A conflicting finalization is quarantined and does not change counts; malformed or unsupported records are durably quarantined before their offsets are committed.
- Normal M2.6 Registration ↔ Allocation processing continues when Kafka is stopped or Kafka publishing is disabled (pending Analytics Outbox rows are expected while unavailable).
- Automated integration tests cover SQL transaction boundaries, Kafka event delivery, offset/deduplication behavior and restart cases (Docker/Testcontainers).

Do not assume that these future M3 verifications have already passed; they are acceptance targets.

## 10. Iterative delivery plan

| Phase | Deliverable |
|---|---|
| M3.1 | Review and accept this design document; record architecture trade-offs |
| M3.2 | Local Kafka Compose configuration + health check and smoke test |
| M3.3 | `RegistrationFinalizedV1` and dedicated Registration Analytics Outbox/publisher + tests |
| M3.4 | Analytics Worker, SQL migrations, idempotent projection and Kafka consumer + tests |
| M3.5 | Consumer groups, controlled offset/replay demonstration, restart and fault tests |
| M3.6 | End-to-end demo, documentation, regression testing and PR closure |

**Architecture gate before M3.2:** the reviewed decisions AD-01 through AD-05 below define the implementation baseline. Avoid introducing new operational coupling, paid/cloud services, Kubernetes or a dashboard during this scope. M3.1 is documentation-only; passing M3.2–M3.6 acceptance criteria is not yet claimed.

## 11. Architecture review — decisions and boundaries

This review settles the initial implementation scope while leaving normal implementation details (library versions, migration names, class structure and specific retry parameters) for the relevant M3 issues.

| Decision | Selected baseline | Rationale |
|---|---|---|
| AD-01 — Event boundary | Emit `RegistrationFinalizedV1` only on a new `Pending → Confirmed/Rejected` transition, in the same Registration SQL transaction as reconciliation | Analytics reflects an authoritative persisted final state, not a preliminary allocation decision |
| AD-02 — Publication reliability | Dedicated Registration `AnalyticsOutboxMessages` table and Kafka-specific publisher state; use the existing Registration SQL transaction | Prevent RabbitMQ publication success from masking a Kafka failure |
| AD-03 — Kafka topology | `SessionId` key, three local partitions, seven-day retention, single-node KRaft | Teach session-local ordering, consumer group assignment and bounded replay without paid infrastructure |
| AD-04 — Processing safety | Analytics SQL transactional deduplication and projection, explicit Kafka offset commit after durable handling; durable SQL quarantine for invalid/conflicting records | Prevent double-counting and accidental message loss when processing or acknowledgements fail |
| AD-05 — Replay | Separate replay consumer group and isolated SQL database, using only events still retained by Kafka | Demonstrate meaningful replay without resetting production-like live projections |

**Additional scope decisions:**

- Initial metrics are **confirmed and rejected registration counts by session**. Pending requests and registration-to-finalization latency are deferred; the initial event needs neither participant PII nor original creation timestamp.
- Readable verification will initially be via documented SQL queries. No new UI/dashboard or extra analytics HTTP API is needed to satisfy M3.
- Reuse existing generic logging, backoff, health check and test helpers when practical, but **never** reuse a single `PublishedAtUtc` flag across RabbitMQ and Kafka destinations.
- Analytics failures do not roll back a completed Registration business decision. Starting the Registration API while Kafka is unavailable must remain possible when the Kafka publisher is disabled or operates with retrying background startup rather than a hard broker dependency.
- The initial `RegistrationFinalizedV1` contract assumes a Registration finalizes once. Future cancellation or amendment events require an explicitly versioned change and different projection semantics.

**Review checkpoints before the M3.1 PR:** validate no cross-database writes, no hidden SQL/Kafka dual-write, documented event-to-projection semantics, and a clear local demonstration path. These are design-level checks, not claims of passing Kafka integration tests.

## 12. References and scope boundaries

- `docs/PROJECT_CHARTER.md` — FR-007/FR-008, nonfunctional and portfolio constraints.
- `docs/MESSAGING_DESIGN.md` — implemented M2.6 RabbitMQ/SQL Outbox/Inbox behavior.
- `docs/LOCAL_DEVELOPMENT.md` — existing local Docker, database and service setup.
- Apache Kafka architecture and design documentation: https://kafka.apache.org/design/
- Confluent Kafka .NET client documentation: https://docs.confluent.io/kafka-clients/dotnet/current/overview.html

**Design status:** This document records the reviewed M3.1 **target architecture**. Kafka-specific components, schema, consumer, publisher, recovery tests, and replay procedure remain to be implemented in M3.2–M3.6. Architecture review does not mean production readiness.