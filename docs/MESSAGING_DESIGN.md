# ConferenceProFlow — Messaging Design

## 1. Overview

ConferenceProFlow uses RabbitMQ for asynchronous communication between the Registration API and the Allocation Worker.

The messaging architecture is designed around independent service ownership, reliable publication of integration events, eventual consistency, and idempotent message processing.

The Registration service implements the Transactional Outbox pattern. Registration data and the corresponding outgoing event are committed to the same SQL Server database transaction. A background publisher subsequently sends pending Outbox messages to RabbitMQ.

**Implementation status:** Registration-side Transactional Outbox publishing and Allocation-side RabbitMQ consumption with durable SQL Inbox persistence are implemented and tested. **Capacity-safe allocation, allocation-result events, and registration status reconciliation are not yet implemented.**

## 2. Service Responsibilities

### Registration Service

The Registration API owns:

- Conference and session information.
- Participant records.
- Registration requests and lifecycle state.
- Registration Outbox messages and their publication state.
- Future processing of allocation-result events.

### Allocation Service

The Allocation Worker owns:

- Session inventory and capacity.
- Allocation records and capacity decisions.
- Consumption and validation of `RegistrationRequestedV1` events.
- Durable SQL Inbox storage, duplicate detection, and conflict handling.
- Parking of invalid or conflicting RabbitMQ deliveries for investigation.
- Future processing of received requests into allocation decisions.

Each service maintains its own SQL Server database. Neither service directly modifies the other service's database.

## 3. Initial Integration Event

The first integration event is `RegistrationRequestedV1`, with message type `registration.requested.v1`.

The event represents a newly created registration request awaiting allocation.

### Message Contract

| Field | Type | Description |
|---|---|---|
| `MessageId` | `Guid` | Unique integration message identifier |
| `RegistrationId` | `Guid` | Business identifier of the registration |
| `SessionId` | `Guid` | Target conference session |
| `OccurredAtUtc` | `DateTimeOffset` | UTC timestamp of the event |

The event deliberately excludes participant personal information because the Allocation Worker does not require it to make an allocation decision.

### JSON Representation

```json
{
  "messageId": "d76314db-f2c8-4d84-b40c-b801e2f6be3a",
  "registrationId": "6ed94d51-f3db-434f-9923-d9e27cf7813f",
  "sessionId": "765f56ae-0f5f-4c76-a52c-a4195e1d344c",
  "occurredAtUtc": "2026-11-09T10:00:00+00:00"
}
```

The JSON contract uses camelCase property names and UTC timestamps. Integration contracts are versioned independently of internal C# namespaces and domain entities.

## 4. Message Identity and Delivery Semantics

`MessageId` identifies an integration message. `RegistrationId` identifies the underlying business registration. The Outbox row identifier equals the event's `MessageId`.

The publisher provides **at-least-once publication behavior**, subject to continued retries and recovery of its dependencies; it does not provide exactly-once delivery or exactly-once business execution.

A message can reach RabbitMQ successfully even if the publisher crashes before persisting `PublishedAtUtc`. A subsequent attempt can publish the same `MessageId` again.

RabbitMQ publisher confirmation indicates broker acceptance of the publication, **not** that the Allocation Worker has processed the registration.

The implemented Allocation consumer is **idempotent at the durable-receipt (Inbox) boundary**. SQL constraints prevent multiple Inbox rows for the same `MessageId` or `RegistrationId`. Business-level idempotency of capacity allocation remains a separate requirement for M2.5.

The consumer uses manual RabbitMQ acknowledgements: it acknowledges a valid request **after** persisting it or verifying an existing identical/compatible Inbox record. Acknowledging a message means that durable intake succeeded; it **does not mean a seat has been allocated**.

## 5. Transactional Outbox

The Registration service stores Outbox messages in its own SQL Server database.

### Outbox Schema

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

## 6. Reliability and Failure Handling

### 6.1. Failure Scenarios

| Scenario | Expected behavior |
|---|---|
| Database transaction fails | Neither registration nor Outbox message is committed |
| Application stops after database commit | Unpublished Outbox message remains in SQL Server |
| RabbitMQ is unavailable | Failure is recorded; message remains eligible for a later retry |
| RabbitMQ publication times out | Outcome is treated as uncertain; message is retained for retry |
| Routing key has no matching binding | Publishing failure is recorded; message remains unpublished |
| Known event payload is permanently invalid | Message is quarantined in SQL Server |
| RabbitMQ accepts a message but SQL status update fails | Duplicate publication is possible on retry |

### 6.2. Persistent Retry Scheduling

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

### 6.3. Publish Timeout

RabbitMQ connection and publishing attempts use a configurable linked cancellation token. The default value of `OutboxPublisher:PublishTimeoutSeconds` is **15 seconds**.

The timeout limits the intended attempt, but does not guarantee that every RabbitMQ.Client operation, including connection cleanup, finishes within that exact duration.

A timeout does not prove that the broker rejected the message. Consequently, retrying after a timeout can produce duplicates.

### 6.4. Payload Validation and Quarantine

Before publication, `OutboxPayloadValidator` validates the known `RegistrationRequestedV1` contract, including:

- Valid JSON that can be deserialized into the event contract.
- Matching payload `MessageId` and Outbox `Id`.
- Nonempty `RegistrationId` and `SessionId`.
- A valid UTC `OccurredAtUtc` timestamp.

A permanently invalid known payload is **quarantined**, rather than repeatedly sent to RabbitMQ. Its Outbox row is retained, `QuarantinedAtUtc` and `LastError` are recorded, and automatic publishing excludes it.

Quarantine is a **SQL Outbox state**, not a RabbitMQ dead-letter queue. An audited administrative repair/requeue workflow has not yet been implemented.

Unknown future message types do not yet have dedicated payload validators. An unknown routing key may remain unpublished and be retried with backoff if RabbitMQ cannot route it.

## 7. RabbitMQ Topology

The implemented local-development topology is:

| Component | Name or behavior |
|---|---|
| Virtual host | `conferenceproflow` |
| Exchange | `conferenceproflow.events` |
| Exchange type | Durable `topic` exchange |
| Routing key | `registration.requested.v1` |
| Registration-request queue | `allocation.registration-requests.v1` (durable) |
| Failure parking queue | `allocation.failed-registration-requests.v1` (durable) |
| Event delivery | Persistent JSON messages with `MessageId`, `Type`, and `ContentType` properties |
| Publisher | Publisher confirms and `mandatory` routing enabled |
| Consumer | Manual ACK; `prefetchCount = 1`; asynchronous dispatch concurrency of one |

The Registration publisher declares the event exchange, registration-request queue, and binding when establishing its broker connection. The Allocation consumer also declares the same compatible exchange, queue, and binding when starting a consumer session. This reduces dependence on the order in which the services start. Both declarations must remain compatible.

The Allocation consumer declares its own parking queue. It republishes malformed or conflicting deliveries to that queue via the default exchange on a separate publisher-confirm channel. The original delivery is acknowledged **only after** the parking publication is confirmed.

Parking is not an atomic move. A crash between confirmed parking publication and original ACK can cause duplicate parked deliveries. There is no automated replay, deduplication, or operator workflow for parked messages yet. The parking queue should be restricted to trusted operators.

Integration tests use isolated RabbitMQ Testcontainers and may use the default test virtual host instead of the local-development virtual host.

## 8. SQL Inbox, Idempotent Intake, and Eventual Consistency

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
| `ProcessedAtUtc` | Remains `NULL` until future business allocation processing completes |

The database enforces uniqueness on `Id` and `RegistrationId`. `InboxReceiveService` first attempts an insert, then handles SQL Server unique-constraint violations (`2601`/`2627`) by reading the existing record. The results are:

| Result | Interpretation | Consumer action |
|---|---|---|
| `Stored` | New registration request persisted | ACK |
| `DuplicateMessage` | Same message identity and matching data already persisted | ACK |
| `DuplicateRegistration` | Another message ID for the same registration and session | ACK |
| `Conflict` | Inconsistent existing message or registration identity | Park, then ACK if parking is confirmed |
| SQL or other unexpected error | Durable acceptance not established | No ACK; close/restart session and permit redelivery |

A unique registration constraint prevents duplicate **Inbox receipt**, not duplicate **seat reservation**. The Inbox does not directly update `ReservedCount`, create allocations, or change Registration status.

### Failure Handling and Recovery

`RegistrationRequestConsumerService` runs as a .NET `BackgroundService` using `RabbitMQ.Client` 7.x. It creates a new dependency-injection scope and `AllocationDbContext` for each delivery. The consumer uses `prefetchCount = 1`, manual acknowledgements, and an application-controlled reconnect loop with a five-second delay. Client automatic recovery is disabled to avoid competing recovery strategies.

If SQL persistence fails, the delivery is not acknowledged. Closing the RabbitMQ channel makes unacknowledged deliveries eligible for redelivery. A duplicate after SQL commit but before ACK is recognized by Inbox idempotency constraints.

Invalid JSON, invalid message metadata, and conflicting identities are preserved in the durable parking queue after successful publication confirmation. This is **not** a replacement for production-grade dead-letter handling, audited recovery, or monitoring.

### Eventual Consistency

A new registration remains `Pending` after its Outbox event is published **and after its Inbox request is received**. The currently implemented path is:

1. Registration API atomically persists a `Pending` registration and Outbox message.
2. The Outbox publisher sends `registration.requested.v1` to RabbitMQ.
3. Allocation consumer validates and durably records the request in `InboxMessages` and then ACKs it (or safely handles a duplicate).
4. **Planned M2.5:** Allocation processes pending Inbox rows and makes a capacity-safe, idempotent allocation decision in its own database transaction.
5. **Planned:** Allocation publishes an allocation-result integration event through an appropriate reliable mechanism.
6. **Planned:** Registration consumes the result and changes the registration status to `Confirmed` or `Rejected`.

No step in the implemented intake flow confirms or rejects a seat allocation.

## 9. Automated Testing

Unit tests cover Outbox creation and state changes, retry policy, payload validation, Inbox entity invariants, and consumer-side JSON parsing and validation.

### Registration Integration Tests

Registration integration tests use SQL Server and isolated RabbitMQ Testcontainers to verify:

- Atomic registration and Outbox persistence.
- Publisher confirmations and persisted publication state.
- Temporary broker unavailability, recovery, unroutable messages, and failure isolation.
- Duplicate publication scenarios and deferred retries.
- Quarantine of permanently invalid known payloads.

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

## 10. Operational Visibility

The publisher emits structured logs for startup, RabbitMQ connection/topology initialization, publication success, retryable failures, and quarantined messages. The Allocation consumer logs consumer registration/reconnection, accepted requests, and parked or failed deliveries.

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

The RabbitMQ Management UI exposes the request and parking queue message counts and active consumer information. These views are diagnostic; a `Ready = 0` queue does not by itself prove a business allocation has occurred.

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
- Incoming requests are stored but **not yet processed into capacity-safe allocations**. `InboxMessage.ProcessedAtUtc` remains `NULL`.
- Allocation-result event publishing and Registration status reconciliation are not implemented.
- The Registration API has additional development-stage access-control/session-publication limitations documented elsewhere.

The project is **production-aware**, with selected production-ready engineering practices; it is not yet a fully production-ready distributed system.

## 12. Next Steps

1. Implement **M2.5 concurrency-safe Inbox processing**: provision/resolve session inventory, enforce capacity, and atomically persist the allocation decision and Inbox processing state.
2. Preserve idempotent business results across redelivery and application restarts.
3. Publish versioned allocation-result events using reliable delivery and process them in Registration to update `Pending` registrations.
4. Add end-to-end business workflow tests and document inventory provisioning and session-publication rules.
5. Revisit operating procedures for parked and quarantined messages, metrics, alerting, and scale-out coordination when justified by deployment requirements.

The architecture prioritizes data consistency, explicit failure handling, testability, and reproducible local development without paid infrastructure.
