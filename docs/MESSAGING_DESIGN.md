# ConferenceProFlow — Messaging Design

## 1. Overview

ConferenceProFlow uses RabbitMQ for asynchronous communication between the Registration API and the Allocation Worker.

The messaging architecture is designed around independent service ownership, reliable publication of integration events, eventual consistency, and idempotent message processing.

The Registration service implements the Transactional Outbox pattern. Registration data and the corresponding outgoing event are committed to the same SQL Server database transaction. A background publisher subsequently sends pending Outbox messages to RabbitMQ.

**Implementation status:** Registration-side Outbox persistence and RabbitMQ publishing are implemented and tested. RabbitMQ consumption and allocation processing are planned for subsequent milestones.

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
- Future consumption of registration-request events.

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

The future Allocation consumer must be idempotent: repeated delivery of a message, or repeated requests concerning the same `RegistrationId`, must not create duplicate allocations or reserve additional seats.

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
| Queue | `allocation.registration-requests.v1` |
| Queue configuration | Durable |
| Message configuration | Persistent; JSON content type; stable message ID |
| Publication | Publisher confirmations enabled; mandatory routing enabled |

The publisher currently declares the exchange, queue, and binding when establishing its RabbitMQ connection for publication. In this project phase, that lets the Registration API publish before a consumer has been implemented.

In a more independently deployed system, queue declaration would generally belong to the consuming service or to infrastructure provisioning. This ownership boundary should be revisited when the Allocation consumer is added.

Integration tests use an isolated RabbitMQ Testcontainer and may use its default virtual host instead of the development virtual host.

## 8. Eventual Consistency

Registration and Allocation maintain separate databases and do not share a distributed SQL transaction.

A new registration remains `Pending` while its allocation decision is outstanding. Successful RabbitMQ publication does **not** change the registration to `Confirmed` or `Rejected`.

The planned flow is:

1. Registration API persists a `Pending` registration and its Outbox event atomically.
2. Outbox Publisher sends `registration.requested.v1` to RabbitMQ.
3. Allocation Worker consumes the message and makes a capacity-safe, idempotent allocation decision. **Not yet implemented.**
4. Allocation Service publishes an allocation-result event. **Not yet implemented.**
5. Registration Service consumes the result and updates registration status. **Not yet implemented.**

Temporary service unavailability, retries, and repeated delivery are expected conditions in this design.

## 9. Automated Testing

Registration-side messaging behavior is covered by unit and integration tests.

### Unit Tests

- Outbox message creation and state transitions.
- Failure recording, retry scheduling, and capped exponential backoff.
- Publication state cleanup after success.
- Payload validation and quarantine rules.

### Integration Tests

- Atomic registration and Outbox persistence in SQL Server.
- Publishing to a real RabbitMQ Testcontainer and recording the confirmed result.
- Unavailable-broker failure recording and recovery.
- Unroutable-message handling and failure isolation.
- Demonstration that duplicate publication can occur.
- Deferred-message processing without blocking an eligible message.
- Quarantine of an invalid payload without blocking a valid message.

Messaging integration tests use isolated databases and RabbitMQ test brokers to avoid dependence on execution order or messages left by other tests.

These tests validate the implemented scenarios; they do not establish exactly-once delivery, multi-instance safety, or all possible broker/network fault conditions.

## 10. Operational Visibility

The publisher emits structured logs for startup, RabbitMQ connection/topology initialization, publication success, retryable failures, and quarantined messages.

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

There is not yet a dedicated production monitoring dashboard, alerting policy, or audited quarantine-recovery operation.

## 11. Current Limitations

- The publisher assumes one active instance; concurrent publishers need database claiming/leasing or equivalent coordination.
- RabbitMQ publisher confirmations do not establish downstream consumer processing.
- Publication is at-least-once, not exactly-once; duplicate deliveries remain possible.
- The configured cancellation timeout is not a hard end-to-end bound on all client and cleanup operations.
- Unknown message contracts do not have dedicated validators; unsupported routing may cause repeated retries.
- Quarantined messages require manual investigation; safe administrative repair/requeue is not implemented.
- Retry behavior has no dedicated production alerting or automated intervention workflow.
- The Allocation Worker has not yet implemented RabbitMQ consumption, idempotency, or capacity-safe allocation.
- Allocation-result messaging and Registration status reconciliation are not yet implemented.

The current implementation is **production-aware**, with selected production-ready engineering practices, rather than a fully production-ready distributed system.

## 12. Next Steps

1. Complete the final M2.3 architecture review, documentation review, and pull request.
2. Implement an idempotent RabbitMQ consumer in the Allocation Worker, including manual acknowledgements and transactional deduplication.
3. Implement concurrency-safe seat allocation against the Allocation database.
4. Publish allocation-result integration events through an appropriate reliable mechanism.
5. Consume allocation results in Registration and transition registrations from `Pending` to `Confirmed` or `Rejected`.
6. Revisit topology ownership, operational monitoring, and multi-instance publisher coordination based on deployment requirements.

The architecture prioritizes data consistency, explicit failure handling, testability, and reproducible local development without paid infrastructure.
