# ConferenceProFlow — Messaging Design

## 1. Overview

ConferenceProFlow will use RabbitMQ for asynchronous communication between the Registration API and the Allocation Worker.

The messaging architecture is designed to support reliable event publication, independent service ownership, idempotent processing, and eventual consistency.

The Registration service will use the Transactional Outbox pattern to connect registration persistence with outgoing integration messages.

## 2. Service Responsibilities

### Registration Service

The Registration API owns:

- Conference and session information.
- Participant records.
- Registration requests.
- Registration lifecycle state.
- Registration Outbox messages.
- Processing of future allocation results.

### Allocation Service

The Allocation Worker owns:

- Session inventory.
- Session capacity.
- Allocation records.
- Capacity allocation decisions.
- Processing of incoming registration requests.

Each service maintains independent database ownership.

Neither service may directly modify the other service's database.

## 3. Initial Integration Event

The first integration event is:

`RegistrationRequestedV1`

Message type:

`registration.requested.v1`

The event represents a newly created registration request awaiting allocation.

### Message Contract

| Field | Type | Description |
|---|---|---|
| MessageId | Guid | Unique integration message identifier |
| RegistrationId | Guid | Business identifier of the registration |
| SessionId | Guid | Target conference session |
| OccurredAtUtc | DateTimeOffset | UTC timestamp of the event |

The event does not contain participant personal information because the Allocation Worker does not require it.

### JSON Representation

```json
{
  "messageId": "d76314db-f2c8-4d84-b40c-b801e2f6be3a",
  "registrationId": "6ed94d51-f3db-434f-9923-d9e27cf7813f",
  "sessionId": "765f56ae-0f5f-4c76-a52c-a4195e1d344c",
  "occurredAtUtc": "2026-11-09T10:00:00+00:00"
}
```

The JSON contract uses camelCase property names and UTC timestamps.

Message contracts are versioned independently of internal C# namespaces and domain entities.

## 4. Message Identity

`MessageId` identifies a particular integration message.

`RegistrationId` identifies the underlying business registration.

A message may be delivered more than once.

Consumers must therefore distinguish message delivery from business execution.

For allocation processing, repeated requests for the same RegistrationId must not create additional seat reservations.

## 5. Transactional Outbox

The Registration service stores outgoing messages in its own database.

The planned Outbox schema contains:

- Id
- MessageType
- Payload
- OccurredAtUtc
- PublishedAtUtc
- PublishAttempts
- LastError

A newly created message is unpublished until successful publication has been recorded.

Registration creation and the corresponding Outbox record must be persisted atomically.

The Outbox Publisher will asynchronously read unpublished messages and publish them to RabbitMQ.

The Registration API must not require RabbitMQ availability to complete the local registration transaction.

## 6. Reliability Requirements

### Database Transaction Failure

If the local transaction fails, neither the registration nor the corresponding Outbox message may be committed.

### Application Failure Before Publication

If the Registration API stops after committing the transaction, the Outbox message remains available for later publishing.

### RabbitMQ Unavailability

Unpublished messages remain in SQL Server and can be retried after RabbitMQ becomes available.

### Duplicate Publication

A message may be published successfully even if the publisher fails before recording the publication result.

The system must therefore tolerate repeated publication and delivery.

### Consumer Idempotency

The Allocation Worker must prevent repeated messages from creating duplicate allocations or reserving additional seats.

## 7. RabbitMQ Topology

The initial planned topology is:

| Component | Name |
|---|---|
| Virtual host | `conferenceproflow` |
| Exchange | `conferenceproflow.events` |
| Exchange type | `topic` |
| Routing key | `registration.requested.v1` |
| Consumer queue | `allocation.registration-requests.v1` |

These names define the intended topology.

Publisher confirmations, persistent messages, durable queues, and routing-failure handling will be considered together.

## 8. Eventual Consistency

Registration and Allocation maintain independent databases.

They do not share a distributed SQL transaction.

A registration may temporarily remain Pending while allocation is being processed.

The final registration status will be updated after an allocation result is received and safely processed.

The system must tolerate temporary service unavailability, retries, and duplicate deliveries.

## 9. Current Implementation and Limitations

The Registration service implements Transactional Outbox persistence and an initial RabbitMQ Outbox Publisher.

### Implemented

- Atomic persistence of registration requests and corresponding Outbox messages.
- Versioned `RegistrationRequestedV1` integration events.
- JSON serialization of outgoing messages.
- SQL Server Outbox persistence.
- BackgroundService-based Outbox publishing.
- RabbitMQ topic exchange, durable queue, and routing configuration.
- Persistent RabbitMQ messages.
- Publisher confirmation tracking.
- Recording of successful publication timestamps.
- Recording of failed publication attempts.
- Automatic retry through periodic polling.
- Recovery from temporary RabbitMQ unavailability.
- Cancellation-aware background processing.

### Current Limitations

The initial implementation is intended for local development with a single active publisher instance.

Additional reliability mechanisms are required before production use, including:

- Automated RabbitMQ integration and failure-recovery tests.
- Safe coordination between multiple publisher instances.
- Improved retry scheduling and exponential backoff.
- Handling of permanently failing messages.
- Operational monitoring and alerting.
- Message retention and Outbox cleanup.
- Independent publisher deployment where operationally justified.

The Allocation Worker does not yet consume registration messages.

Session inventory provisioning, concurrency-safe allocation, idempotent consumers, and allocation-result processing remain planned development tasks.

The current HTTP API is not ready for public production deployment.

## 10. Next Steps

Upcoming implementation phases will introduce:

1. EF Core Outbox persistence and atomic registration writes.
2. RabbitMQ Outbox publishing.
3. Publisher confirmation and retry handling.
4. Automated messaging integration tests.
5. Allocation consumers and idempotency.
6. Concurrency-safe seat allocation.
7. Allocation-result events and Registration status updates.

The architecture will prioritize data consistency, explicit failure handling, and reproducible local development.