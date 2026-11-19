# ConferenceProFlow — Messaging Design

## 1. Overview

ConferenceProFlow uses RabbitMQ for asynchronous communication between the Registration API and the Allocation Worker.

The messaging architecture is designed around independent service ownership, reliable publication of integration events, eventual consistency, and idempotent message processing.

The Registration service implements the Transactional Outbox pattern. Registration data and the corresponding outgoing event are committed to the same SQL Server database transaction. A background publisher subsequently sends pending Outbox messages to RabbitMQ.

**Implementation status (M2.6):** Both directions of the event flow are implemented: Registration publishes registration requests and Allocation publishes allocation decisions via their respective transactional Outbox pipelines; both services consume events through durable RabbitMQ queues and SQL Inbox/idempotency handling. Allocation makes capacity-safe decisions, and Registration reconciles final statuses to `Confirmed` or `Rejected`. The workflow was verified manually with two seats and three registrations, and with a Worker stop/restart recovery scenario. These checks complement automated unit and integration tests; they do not establish exactly-once delivery or production readiness.

**Known boundary:** Session inventory is not created automatically when Registration creates a session. The local demo uses a development-only SQL seed against AllocationDb.

## 2. Service Responsibilities

### Registration Service

The Registration API owns:

- Conference and session information.
- Participant records.
- Registration requests and lifecycle state.
- Registration Outbox messages and their publication state.
- Consumption of `AllocationDecidedV1` through a Registration-owned RabbitMQ results queue.
- Durable `AllocationResultInboxMessages` receipts and reconciliation of registrations to final `Confirmed` / `Rejected` status.

### Allocation Service

The Allocation Worker owns:

- Session inventory and capacity.
- Allocation records and capacity decisions.
- Consumption and validation of `RegistrationRequestedV1` events.
- Durable SQL Inbox storage, duplicate detection, and conflict handling.
- Parking of invalid or conflicting RabbitMQ deliveries for investigation.
- Capacity-safe processing of pending SQL Inbox messages into Confirmed or Rejected allocations, with a background processor and SQL transaction consistency.
- Durable `AllocationOutboxMessages` and publication of `AllocationDecidedV1` outcomes after local persistence.

Each service maintains its own SQL Server database. Neither service directly modifies the other service's database.

## 3. Versioned Integration Events

### 3.1. RegistrationRequestedV1 (Registration → Allocation)

The request integration event is `RegistrationRequestedV1`, with message type `registration.requested.v1`.

The event represents a newly created registration request awaiting allocation.

### Request Message Contract

| Field | Type | Description |
|---|---|---|
| `MessageId` | `Guid` | Unique integration message identifier |
| `RegistrationId` | `Guid` | Business identifier of the registration |
| `SessionId` | `Guid` | Target conference session |
| `OccurredAtUtc` | `DateTimeOffset` | UTC timestamp of the event |

The event deliberately excludes participant personal information because the Allocation Worker does not require it to make an allocation decision.

### Request JSON Representation

```json
{
  "messageId": "d76314db-f2c8-4d84-b40c-b801e2f6be3a",
  "registrationId": "6ed94d51-f3db-434f-9923-d9e27cf7813f",
  "sessionId": "765f56ae-0f5f-4c76-a52c-a4195e1d344c",
  "occurredAtUtc": "2026-11-09T10:00:00+00:00"
}
```

The JSON contract uses camelCase property names and UTC timestamps. Integration contracts are versioned independently of internal C# namespaces and domain entities.

### 3.2. AllocationDecidedV1 (Allocation → Registration)

After a capacity decision has been persisted, Allocation publishes an `AllocationDecidedV1` event with message type and routing key `allocation.decided.v1`.

| Field | Type | Description |
|---|---|---|
| `MessageId` | `Guid` | Identity of this result event; the Allocation Outbox row `Id` equals this ID |
| `AllocationId` | `Guid` | Identity of the persisted Allocation decision |
| `RegistrationId` | `Guid` | Registration to reconcile |
| `SessionId` | `Guid` | Session referenced by the allocation |
| `Status` | `string` | `confirmed` or `rejected` in the event payload |
| `RejectionReason` | `string?` | `null` when confirmed; populated when rejected |
| `OccurredAtUtc` | `DateTimeOffset` | UTC event timestamp |

Example rejected decision (illustrative test identifiers):

```json
{
  "messageId": "a02d33df-9f66-40c8-9a3f-a8c955c2cc8a",
  "allocationId": "00d94fb3-ecb0-4863-a580-e01e23d43f4c",
  "registrationId": "6ed94d51-f3db-434f-9923-d9e27cf7813f",
  "sessionId": "765f56ae-0f5f-4c76-a52c-a4195e1d344c",
  "status": "rejected",
  "rejectionReason": "Session capacity reached.",
  "occurredAtUtc": "2026-11-19T16:50:00+00:00"
}
```

Both services keep their own event contract types. The receiving side validates the wire contract and never requires direct access to the other service's database.

## 4. Message Identity and Delivery Semantics

`MessageId` identifies an integration message. `RegistrationId` identifies the underlying business registration. The Outbox row identifier equals the event's `MessageId`.

The publisher provides **at-least-once publication behavior**, subject to continued retries and recovery of its dependencies; it does not provide exactly-once delivery or exactly-once business execution.

A message can reach RabbitMQ successfully even if the publisher crashes before persisting `PublishedAtUtc`. A subsequent attempt can publish the same `MessageId` again.

RabbitMQ publisher confirmation indicates broker acceptance of the publication, **not** that the Allocation Worker has processed the registration.

The implemented Allocation consumer is **idempotent at the durable-receipt (Inbox) boundary**. SQL constraints prevent multiple Inbox rows for the same `MessageId` or `RegistrationId`. Business-level idempotency is implemented separately by `AllocationProcessingService`, SQL uniqueness on `Allocation.RegistrationId`, optimistic concurrency, and re-evaluation after conflicts.

The Allocation request consumer uses manual RabbitMQ acknowledgements: it acknowledges a valid request **after** persisting it or verifying an existing identical/compatible Inbox record. This ACK means durable intake succeeded; it **does not mean a seat has been allocated**.

The Registration result consumer separately acknowledges a valid allocation result only after reconciliation has committed or an existing compatible decision is recognized. SQL uniqueness and business comparisons defend against redelivery of the same `MessageId`, repetition of the same decision with another message ID, and conflicting updates. Neither consumer provides exactly-once transport delivery.

## 5. Transactional Outboxes

The Registration service stores Outbox messages in its own SQL Server database.

### Registration Outbox Schema

The implemented `OutboxMessages` entity contains:

| Field | Purpose |
|---|---|
| `Id` | Unique Outbox identifier; matches the event `MessageId` |
| `MessageType` | Versioned event type and RabbitMQ routing key |
| `Payload` | Serialized event JSON |
| `OccurredAtUtc` | Event creation timestamp |
| `PublishedAtUtc` | Set after confirmed publication; otherwise `NULL` |
| `PublishAttempts` | Count of recorded processing attempts, including successful publication, failed publication, and quarantine validation |
| `LastError` | Most recently recorded failure or quarantine reason |
| `NextAttemptAtUtc` | Earliest eligible retry time; `NULL` means no scheduled delay |
| `QuarantinedAtUtc` | Set when a permanently invalid payload is excluded from automatic publishing |

An index named `IX_OutboxMessages_PublishStatus` supports queries over publication, quarantine, and retry status.

### Atomic Registration Write

Creating a registration writes both the new registration and its `RegistrationRequestedV1` Outbox message within one EF Core `SaveChangesAsync` transaction.

- If the local database transaction fails, neither record is committed.
- If it succeeds, the registration initially remains `Pending` while allocation has not yet occurred.
- RabbitMQ availability is **not required** for the HTTP registration transaction to succeed.

### Background Publication

`OutboxPublisherService` runs as a `BackgroundService` within the Registration API process.

It polls for eligible messages approximately every five seconds, processes batches of up to 20, publishes persistent messages to RabbitMQ, and waits for publisher confirmation before recording `PublishedAtUtc`.

Messages are eligible when they are unpublished, not quarantined, and have no future `NextAttemptAtUtc` value. An unsuccessful message does not automatically stop processing the other messages already selected in the batch.

The current implementation assumes **one active publisher instance**. It does not claim or lease Outbox rows for coordination among multiple application replicas.

### Allocation Result Outbox (M2.6B–M2.6C)

The Allocation database has a separate `AllocationOutboxMessages` table. Its rows represent durable result events, not Registration-side requests. Each stores `Id` (= result `MessageId`), `AllocationId`, `MessageType`, serialized `Payload`, `OccurredAtUtc`, `PublishedAtUtc`, `PublishAttempts`, `LastError`, `NextAttemptAtUtc`, and `QuarantinedAtUtc`.

- `AllocationId` has a **unique index**, so a given Allocation cannot create multiple logical result-Outbox rows.
- The result payload and message identity are generated once and reused on publication retries.
- The Allocation processing flow is designed to persist the allocation decision, relevant inventory update, Inbox completion, and result Outbox together within the **Allocation** database consistency boundary. This is separate from the Registration database transaction; confirm the final write path and the related integration coverage during the pre-merge review.
- `AllocationOutboxPublisherService` polls up to 20 eligible rows approximately every five seconds, validates the stored result payload, publishes persistent `allocation.decided.v1` messages with publisher confirmations and `mandatory: true`, and then records `PublishedAtUtc`.
- Failure metadata and retry scheduling are durable; invalid known payloads are quarantined in SQL instead of being sent. The default publish-attempt timeout is 15 seconds, configurable through `AllocationOutboxPublisher:PublishTimeoutSeconds` (1–120 seconds).
- **Queue ownership:** the Allocation publisher declares the shared event exchange, but does **not** create the Registration-owned result queue. The Registration consumer declares/binds that queue. An absent binding makes mandatory publication fail rather than silently mark the event as published.

As in the Registration Outbox, a broker confirmation does **not** imply consumer processing. A broker confirmation followed by a SQL update failure can lead to the same result event being published again. The Registration result Inbox makes that redelivery safe at the business boundary.

## 6. Reliability and Failure Handling

### 6.1. Failure Scenarios (both directions)

| Scenario | Expected behavior |
|---|---|
| Database transaction fails | Neither registration nor Outbox message is committed |
| Application stops after database commit | Unpublished Outbox message remains in SQL Server |
| RabbitMQ is unavailable | Failure is recorded; message remains eligible for a later retry |
| RabbitMQ publication times out | Outcome is treated as uncertain; message is retained for retry |
| Routing key has no matching binding | Publishing failure is recorded; message remains unpublished |
| Known event payload is permanently invalid | Message is quarantined in SQL Server |
| RabbitMQ accepts a message but SQL status update fails | Duplicate publication is possible on retry |

### 6.2. Persistent Retry Scheduling (Registration publisher)

Failed publication attempts are recorded in SQL Server. `NextAttemptAtUtc` determines when an unpublished message may be retried.

The retry policy uses capped exponential backoff:

| Failed attempt | Delay until next attempt |
|---|---:|
| 1 | 10 seconds |
| 2 | 20 seconds |
| 3 | 40 seconds |
| 4 | 80 seconds |
| 5 | 160 seconds |
| 6 and later | 300 seconds |

Retry delays are capped at five minutes; **the number of transient-failure retries is not capped**. A prolonged infrastructure outage should not automatically quarantine otherwise valid messages.

The polling loop can continue publishing other eligible messages while an unsuccessful message is deferred. This mitigates head-of-line blocking, but it does not replace a full queue-scheduling or distributed-work coordination mechanism.

### 6.3. Publish Timeout (Registration publisher)

RabbitMQ connection and publishing attempts use a configurable linked cancellation token. The default value of `OutboxPublisher:PublishTimeoutSeconds` is **15 seconds**.

The timeout limits the intended attempt, but does not guarantee that every RabbitMQ.Client operation, including connection cleanup, finishes within that exact duration.

A timeout does not prove that the broker rejected the message. Consequently, retrying after a timeout can produce duplicates.

### 6.4. Payload Validation and Quarantine (Registration publisher)

Before publication, `OutboxPayloadValidator` validates the known `RegistrationRequestedV1` contract, including:

- Valid JSON that can be deserialized into the event contract.
- Matching payload `MessageId` and Outbox `Id`.
- Nonempty `RegistrationId` and `SessionId`.
- A valid UTC `OccurredAtUtc` timestamp.

A permanently invalid known payload is **quarantined**, rather than repeatedly sent to RabbitMQ. Its Outbox row is retained, `QuarantinedAtUtc` and `LastError` are recorded, and automatic publishing excludes it.

Quarantine is a **SQL Outbox state**, not a RabbitMQ dead-letter queue. An audited administrative repair/requeue workflow has not yet been implemented.

Unknown future message types do not yet have dedicated payload validators. An unknown routing key may remain unpublished and be retried with backoff if RabbitMQ cannot route it.

The Allocation result publisher has equivalent durable publish-state fields and its own `AllocationOutboxPayloadValidator` and retry policy. Its current polling loop stops the current batch after a publish failure and tries again later; therefore failure-isolation details are not identical to the Registration publisher.

## 7. RabbitMQ Topology

Both message directions share the durable topic exchange `conferenceproflow.events`, but each receiving service owns its **own durable queue and parking queue**.

| Direction | Routing key | Consumer-owned queue | Parking queue |
|---|---|---|---|
| Registration → Allocation | `registration.requested.v1` | `allocation.registration-requests.v1` | `allocation.failed-registration-requests.v1` |
| Allocation → Registration | `allocation.decided.v1` | `registration.allocation-results.v1` | `registration.failed-allocation-results.v1` |

The local-development virtual host is `conferenceproflow`. Both publishers use JSON messages with AMQP `MessageId`, `Type`, and `ContentType`, persistence, publisher confirmations, and mandatory routing. Both consumers use manual ACK, `prefetchCount = 1`, and asynchronous single-dispatch processing. The source for each durable queue and binding is its **consumer service**; each side declares the shared exchange compatibly.

For malformed metadata/JSON and conflicting identities or decisions, a consumer publishes the original delivery to its own parking queue using a **separate publisher-confirm channel** and the default RabbitMQ exchange. The original delivery is ACKed **only after** successful confirmed parking publication. The Registration result consumer also parks an `UnknownRegistration` outcome for investigation, since the Registration-side request was committed before its original event was published.

If database persistence, broker operations, or parking confirmation fails, the consumer must not ACK an incompletely handled delivery. The session is restarted and unacknowledged deliveries are eligible for redelivery. The Registration result consumer includes a bounded 15-second parking-publish timeout.

**Caveat:** Parking and the original ACK are not one atomic operation. A crash between the two can duplicate parked deliveries. Parking queues are not an automated dead-letter recovery or audited replay workflow. Tests use isolated RabbitMQ Testcontainers and may use their own virtual hosts.

## 8. SQL Inbox, Idempotent Processing, and Eventual Consistency

Registration and Allocation maintain separate databases and do not share a distributed SQL transaction.

### Durable Intake

The Allocation Worker deserializes and validates the consumer-side `RegistrationRequestedV1` contract. The consumer also verifies AMQP routing key, message type, content type, and header/payload message ID agreement before accepting a delivery.

The `InboxReceiveService` persists an `InboxMessage` in the Allocation SQL Server database. Its schema contains:

| Field | Purpose |
|---|---|
| `Id` | Inbox primary key; equals the incoming event's `MessageId` |
| `RegistrationId` | Business registration identifier; protected by a unique index |
| `SessionId` | Requested session identifier |
| `OccurredAtUtc` | Timestamp recorded by the Registration service |
| `ReceivedAtUtc` | Timestamp when the request is durably accepted by Allocation |
| `ProcessedAtUtc` | `NULL` after durable intake; set after the allocation processor commits a business result |

The database enforces uniqueness on `Id` and `RegistrationId`. `InboxReceiveService` first attempts an insert, then handles SQL Server unique-constraint violations (`2601`/`2627`) by reading the existing record. The results are:

| Result | Interpretation | Consumer action |
|---|---|---|
| `Stored` | New registration request persisted | ACK |
| `DuplicateMessage` | Same message identity and matching data already persisted | ACK |
| `DuplicateRegistration` | Another message ID for the same registration and session | ACK |
| `Conflict` | Inconsistent existing message or registration identity | Park, then ACK if parking is confirmed |
| SQL or other unexpected error | Durable acceptance not established | No ACK; close/restart session and permit redelivery |

The unique Inbox registration constraint prevents duplicate **receipt**. The RabbitMQ request consumer itself does not update `ReservedCount` or create allocations. A **separate** allocation processor handles those writes in the Allocation database. Registration status is then updated independently when the result event reaches the Registration consumer.

### Failure Handling and Recovery

`RegistrationRequestConsumerService` runs as a .NET `BackgroundService` using `RabbitMQ.Client` 7.x. It creates a new dependency-injection scope and `AllocationDbContext` for each delivery. The consumer uses `prefetchCount = 1`, manual acknowledgements, and an application-controlled reconnect loop with a five-second delay. Client automatic recovery is disabled to avoid competing recovery strategies.

If SQL persistence fails, the delivery is not acknowledged. Closing the RabbitMQ channel makes unacknowledged deliveries eligible for redelivery. A duplicate after SQL commit but before ACK is recognized by Inbox idempotency constraints.

Invalid JSON, invalid message metadata, and conflicting identities are preserved in the durable parking queue after successful publication confirmation. This is **not** a replacement for production-grade dead-letter handling, audited recovery, or monitoring.

### End-to-End Eventual Consistency (M2.6)

1. Registration API commits a `Pending` registration and `RegistrationRequestedV1` row in the Registration Outbox.
2. The Registration publisher sends `registration.requested.v1` to RabbitMQ.
3. The Allocation request consumer validates the event, persists an Allocation Inbox row and ACKs it (or recognizes a safe duplicate).
4. `AllocationInboxProcessorService` selects pending Inbox rows with an existing `SessionInventory` and delegates to `AllocationProcessingService`.
5. Allocation decides Confirmed/Rejected while respecting capacity; the result event is stored in `AllocationOutboxMessages` for reliable publication.
6. `AllocationOutboxPublisherService` publishes `allocation.decided.v1` to the Registration-owned results queue and marks the Outbox row published after broker confirmation.
7. `AllocationResultConsumerService` validates the event and invokes `AllocationResultReconciliationService` in a fresh DI scope.
8. Reconciliation stores `AllocationResultInboxMessage` and updates `Registration.Status` from `Pending` to `Confirmed` or `Rejected` within one Registration DB `SaveChangesAsync` transaction. The consumer ACKs only after a successful/compatible outcome.

The Registration API may return `201 Created` with `Pending` before the asynchronous decision is available. `GET /api/registrations/{id}` later returns the final status. **No distributed transaction** spans both databases. A request-consumer ACK proves durable request intake; an Allocation result-consumer ACK proves durable reconciliation or a safe duplicate; a broker publisher confirm proves acceptance by the broker, not final business completion.

### Registration Result Inbox and Reconciliation (M2.6D)

The Registration database table `AllocationResultInboxMessages` stores `Id` (= result `MessageId`), `AllocationId`, `RegistrationId`, `SessionId`, decision `Status`, `RejectionReason`, `OccurredAtUtc`, and `ReceivedAtUtc`. Its primary key is `Id`; unique indexes on `AllocationId` and `RegistrationId` defend against conflicting decisions. The Registration database owns this receipt and the related status transition.

`AllocationResultReconciliationService.ApplyAsync` handles:

| Outcome | Meaning | RabbitMQ result-consumer action |
|---|---|---|
| `Applied` | Decision and durable receipt committed together | ACK |
| `DuplicateMessage` | The same event ID and identical contents have already been recorded | ACK |
| `DuplicateDecision` | Same compatible business decision arrived under another message ID | ACK |
| `Conflict` | Different/contradictory decision, registration/session mapping, or message identity | Confirmed parking publication, then ACK |
| `UnknownRegistration` | Result references a Registration that does not exist | Confirmed parking publication, then ACK; manual investigation |
| Unexpected SQL / infrastructure failure | Durable reconciliation is not established | No ACK; restart session and permit redelivery |

Known statuses use the integration-event strings `confirmed` / `rejected`, while Registration API responses expose `Confirmed` / `Rejected`. If a Registration already has an incompatible final status, it is not overwritten. On recognized SQL unique-key races, reconciliation clears stale EF tracking state and re-evaluates against committed data; it does not blindly save an old state.

### M2.5 — Automated, Capacity-Safe Allocation Processing

**This is a separate stage from RabbitMQ consumption.** The Allocation Worker can host both `RegistrationRequestConsumerService` (message intake) and `AllocationInboxProcessorService` (SQL Inbox business processing) at the same time. Both are registered conditionally through `RabbitMqConsumer:Enabled` and `AllocationInboxProcessor:Enabled`.

`AllocationInboxProcessorService` polls eligible Inbox rows (`ProcessedAtUtc IS NULL` and an existing `SessionInventory` for the same SessionId), orders by receipt time and message ID, and processes up to a configured batch size per poll. Each request uses a dedicated DI scope / EF Core DbContext. A missing inventory does not block other, ready sessions; the pending request becomes eligible once inventory is provisioned.

`AllocationProcessingService` evaluates each Inbox request:

- If the Inbox record does not exist, return `NotFound`; if already processed, return `AlreadyProcessed`.
- If an Allocation already exists for the same registration/session, do **not** reserve another seat; complete the pending Inbox record if necessary (`ExistingAllocation`). A conflicting session mapping is an error.
- If no `SessionInventory` exists, return `WaitingForInventory` and keep the Inbox pending. No default capacity is invented and the registration is not rejected solely for lacking inventory.
- If `TryReserveSeat()` succeeds, persist a **Confirmed** Allocation and increment `ReservedCount`.
- If capacity is exhausted, persist a **Rejected** Allocation with reason `Session capacity reached.` without incrementing `ReservedCount`.
- Set `InboxMessage.ProcessedAtUtc` only as part of successful persistence of that business decision.

The inventory update, Allocation insert, and Inbox completion are tracked in **one AllocationDbContext** and written in a single EF Core `SaveChangesAsync` transaction. The result Outbox write belongs in the same consistency boundary so a persisted decision cannot lose its outgoing result event; confirm this invariant in the source and test suite before merging M2.6. SQL Server constraints enforce `0 <= ReservedCount <= Capacity`, and `Allocation.RegistrationId` is unique. `SessionInventory.RowVersion` detects stale concurrent inventory updates. A bounded retry clears tracked state and reevaluates the whole decision after recognized concurrency conflicts, duplicate-key races, or SQL Server deadlocks; it does not blindly repeat a stale update.

The polling worker runs sequentially within **one instance**. Its configurable batch size is not parallel worker concurrency. Separate SQL concurrency tests exercise contention across independent DbContexts, including 100 competing registrations for 10 seats. Those tests demonstrate tested safety conditions, not a guarantee of scale-out orchestration under every workload.

The BackgroundService's `FailureRetrySeconds` cooldown is **in memory**, so it resets after Worker restart. Failures leave the Inbox record durable and eligible for later retry. Its query excludes rows without inventory rather than repeatedly failing them.

Configuration defaults (in Worker `appsettings.json`):

```json
"AllocationInboxProcessor": {
  "Enabled": false,
  "PollIntervalSeconds": 5,
  "BatchSize": 20,
  "FailureRetrySeconds": 30
}
```

For the local end-to-end demonstration, enable the processor and provision inventory using `docs/ALLOCATION_E2E_SEED.sql` for a newly created Registration session. This is **development-only setup**, not automated cross-service inventory synchronization. Also enable the result publisher (`AllocationOutboxPublisher:Enabled=true`) and Registration-side result consumer (`AllocationResultConsumer:Enabled=true`) alongside the original two publishers/consumers. See `docs/ALLOCATION_E2E_DEMO.md` and `docs/ALLOCATION_E2E.http`.

## 9. Automated Testing

Unit tests cover Outbox creation and state changes, retry policy, payload validation, Inbox entity invariants/processing completion, SessionInventory capacity rules, and consumer-side JSON parsing and validation.

### Registration Integration Tests

Registration integration tests use SQL Server and isolated RabbitMQ Testcontainers to verify:

- Atomic registration and Outbox persistence.
- Publisher confirmations and persisted publication state.
- Temporary broker unavailability, recovery, unroutable messages, and failure isolation.
- Duplicate publication scenarios and deferred retries.
- Quarantine of permanently invalid known payloads.

### M2.6 Allocation Result Outbox and Registration Reconciliation Tests

Dedicated automated tests cover result-event contract validation, Allocation-side result Outbox records and publication behavior, and Registration-side reconciliation. The Registration SQL integration tests exercise final status transitions, duplicate-message and duplicate-decision idempotency, conflicting decisions, unknown registrations, and durable Inbox persistence. The Registration RabbitMQ consumer integration tests exercise genuine RabbitMQ and SQL Server behavior for normal Confirmed/Rejected results, duplicate handling, malformed/contradictory results, parking, restart recovery, and transient database/broker failures.

The original M2.5 tests continue to cover request intake and capacity safety. Successful component and integration tests do **not**, by themselves, demonstrate a single automated full cross-service HTTP test.

### Allocation SQL Integration Tests

Allocation SQL Server integration tests verify Inbox persistence, unique `MessageId` and `RegistrationId` constraints, duplicate-vs-conflict handling, and concurrent attempts to accept the same registration.

### Allocation RabbitMQ Consumer Integration Tests

The consumer integration test suite uses the real `RegistrationRequestConsumerService`, SQL Server Testcontainers, and isolated RabbitMQ test brokers. Current scenarios include:

- Valid message intake into SQL Inbox.
- Duplicate `MessageId` and repeated `RegistrationId` deliveries.
- Invalid JSON and identity conflicts moved to the parking queue.
- Redelivery of an unacknowledged message.
- Processing after a consumer restart and idempotency after a repeated delivery.
- Inaccessible SQL endpoint, absence of premature ACK, and recovery with a valid connection.
- RabbitMQ application restart inside the same container, reconnect, and continued processing.
- Restoration of a missing queue binding at consumer startup.

The broker application restart test keeps the container and published endpoint stable; it does not simulate a deployment with a changing broker address. The test suite does not establish exactly-once business execution or cover every network timing/fault combination.

### Allocation Business and Background Processing Tests

SQL Server Testcontainers tests additionally verify:

- Confirmed and Rejected allocation decisions, matching inventory updates, and atomic Inbox completion.
- Pending requests awaiting inventory, and automatic processing after inventory is created.
- Duplicate/sequential business processing and existing-allocation recovery without reserving a second seat.
- SQL Server `RowVersion` conflicts, rollback of failed transactional writes, and fresh-state retry.
- Concurrent attempts to process the same Inbox message.
- 100 requests competing for 10 seats with an eight-operation concurrency cap: 10 confirmed, 90 rejected, with capacity intact.
- The Inbox polling BackgroundService: automatic allocation, batch processing, delayed inventory provisioning, restart recovery, and per-message failure isolation.

A separate **manual cross-service** demonstration is documented in `docs/ALLOCATION_E2E_DEMO.md`. During M2.6E the user verified the two-seat/three-registration scenario end to end (`Confirmed`, `Confirmed`, `Rejected`), corresponding results in both databases, and a later Worker-stop/restart scenario. The current automated suites do not claim a single full-system HTTP → RabbitMQ → SQL business E2E test.

## 10. Operational Visibility

Both Outbox publishers emit structured logs for startup, broker connection/topology, publication, retries and quarantined messages. Both consumers log registration/reconnection, accepted results and parked/failed deliveries. The Allocation Inbox processor logs batch-processing results. These are development-time observability signals, not a substitute for metrics and production alerting.

Trusted developers and operators can inspect the `OutboxMessages` table to monitor:

- Pending and deferred messages.
- Quarantined messages requiring investigation.
- Messages with many recorded attempts.
- The age of the oldest unpublished message.
- Recent error details and scheduled retry times.

Example SQL query:

```sql
DECLARE @UtcNow datetimeoffset = TODATETIMEOFFSET(SYSUTCDATETIME(), '+00:00');

SELECT
    COUNT(*) AS TotalMessages,
    COALESCE(SUM(CASE WHEN PublishedAtUtc IS NOT NULL THEN 1 ELSE 0 END), 0) AS PublishedMessages,
    COALESCE(SUM(CASE WHEN PublishedAtUtc IS NULL AND QuarantinedAtUtc IS NULL THEN 1 ELSE 0 END), 0) AS PendingMessages,
    COALESCE(SUM(CASE WHEN QuarantinedAtUtc IS NOT NULL THEN 1 ELSE 0 END), 0) AS QuarantinedMessages,
    COALESCE(SUM(CASE WHEN PublishedAtUtc IS NULL AND QuarantinedAtUtc IS NULL
                      AND NextAttemptAtUtc > @UtcNow THEN 1 ELSE 0 END), 0) AS DeferredMessages
FROM dbo.OutboxMessages;
```

Operational SQL queries are intended for trusted development and administration. They must not be exposed through an unauthenticated public API.

Operators can also inspect pending Inbox work (queries run against `ConferenceProFlowAllocation`):

```sql
SELECT TOP (25)
    Id, RegistrationId, SessionId,
    OccurredAtUtc, ReceivedAtUtc, ProcessedAtUtc
FROM dbo.InboxMessages
WHERE ProcessedAtUtc IS NULL
ORDER BY ReceivedAtUtc ASC;
```

To inspect completed allocation decisions in the Allocation database:

```sql
SELECT a.RegistrationId, a.SessionId, a.Status, a.RejectionReason,
       i.Capacity, i.ReservedCount, m.ProcessedAtUtc
FROM dbo.Allocations AS a
INNER JOIN dbo.SessionInventories AS i ON i.SessionId = a.SessionId
LEFT JOIN dbo.InboxMessages AS m ON m.RegistrationId = a.RegistrationId
ORDER BY a.CreatedAtUtc DESC;
```

`Allocation.Status = 1` means Confirmed and `2` means Rejected. Registration initially uses `Pending = 0`, and after result reconciliation its status becomes `Confirmed = 1` or `Rejected = 2`. The RabbitMQ Management UI exposes request/result/parking queue counts and active consumers. A `Ready = 0` queue only reflects messaging state, not proof of successful business reconciliation.

There is not yet a dedicated production monitoring dashboard, alerting policy, audited quarantine/parking recovery operation, or authenticated administrative replay tooling.

## 11. Current Limitations

- The Registration Outbox publisher assumes **one active instance**; concurrent publishers need row claiming/leasing or equivalent coordination.
- Publisher confirms only establish broker acceptance. Consumer ACK establishes durable receipt, **not business allocation**.
- Delivery is at least once; exact-once publication, delivery, or business execution is not guaranteed.
- Timeout cancellation is not a hard end-to-end guarantee for all RabbitMQ client operations and cleanup.
- Unknown message contracts do not yet have dedicated publisher-side validators; unsupported routing may trigger repeated retry attempts.
- SQL Outbox quarantine and RabbitMQ parking queue require manual investigation; audited repair/requeue/replay is not implemented.
- Parking publication followed by original ACK is not atomic; duplicate parked messages may occur on crashes.
- Consumer uses prefetch 1 and application-managed reconnect. The current tests do not cover every mid-delivery network partition or multi-instance load scenario.
- There is no dedicated production metrics/alerting setup or administrative monitoring dashboard.
- Pending Inbox messages **are automatically processed into capacity-safe decisions** only when the Allocation Inbox processor is enabled **and** a matching SessionInventory exists. Inventory provisioning/synchronization is not yet automated; a development-only SQL seed is available for the demo.
- Polling and in-memory error cooldowns are scoped to one Worker instance; no durable claims/leases coordinate multiple allocation processor replicas.
- The local end-to-end demo is manual; no single automated cross-service HTTP → RabbitMQ → allocation regression scenario is currently claimed.
- Allocation-result publication and Registration reconciliation are implemented, but runtime recovery and all failure modes are not comprehensively proven by the manual demo alone.
- The Registration API has additional development-stage access-control/session-publication limitations documented elsewhere.

The project is **production-aware**, with selected production-ready engineering practices; it is not yet a fully production-ready distributed system.

## 12. Next Steps

1. Finish M2.6 documentation, inspect the final Allocation decision → result Outbox atomic-write path, run the full Release regression suite and merge the feature PR after CI passes.
2. Introduce explicit, service-owned session inventory provisioning synchronized from Registration without one service writing into the other service's database.
3. Add an automated full-system HTTP → Outbox → RabbitMQ → Inbox → Allocation → result Outbox → RabbitMQ → Registration reconciliation regression test when its maintenance cost is justified.
4. Enforce session-publication/eligibility rules, authentication, authorization, and production-grade secret management.
5. Consider durable work claims, audited replay tooling, metrics/alerting, and multi-replica coordination for real deployment requirements.

The architecture prioritizes consistency, explicit failure handling, testability, and reproducible local development without paid infrastructure. Its scope remains **production-aware**, not a claim of comprehensive production readiness.