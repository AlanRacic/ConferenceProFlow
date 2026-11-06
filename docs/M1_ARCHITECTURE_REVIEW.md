# ConferenceProFlow — M1 Architecture Review

**Review Date:** 2026-11-06  
**Phase:** M1 — Core Registration API  
**Status:** Architecture review completed; follow-up work identified

## 1. Executive Summary

The first development phase established the core Registration API using .NET 10, ASP.NET Core Minimal APIs, Entity Framework Core, and SQL Server.

The implementation supports conference and session management, participant creation, and registration requests.

Registration requests are initially persisted with Pending status. Automated tests validate domain behavior, persistence, HTTP responses, and database constraints.

The architecture provides a foundation for introducing asynchronous messaging and independent capacity allocation.

However, the application is not yet production-ready. Security, session publication, distributed consistency, and reliable asynchronous processing remain outside the implemented scope.

## 2. Architecture Assessment

### 2.1 Domain Model

The domain model defines conferences, sessions, participants, and registrations.

Registration lifecycle transitions are encapsulated within the Registration entity.

Supported transitions:

- Pending → Confirmed
- Pending → Rejected

Confirmed and Rejected are terminal states in the initial model.

**Assessment:** The existing domain model provides an appropriate foundation.

**Decision:** Retain the current model and introduce additional behavior only when required by upcoming workflows.

### 2.2 Application Organization

The Registration API uses feature-based endpoint organization.

Business entities are separated from HTTP request and response contracts.

Entity Framework Core configuration is located in the Infrastructure/Persistence directory.

**Assessment:** The current structure is sufficiently clear for the implemented functionality.

**Decision:** Do not introduce additional architectural projects or repository abstractions without a demonstrated requirement.

Application services or handlers may be introduced when messaging and transaction coordination increase workflow complexity.

### 2.3 Persistence

The Registration service owns its SQL Server database.

Entity Framework Core manages persistence and version-controlled schema migrations.

Database constraints include foreign keys, unique indexes, and registration status validation.

**Assessment:** The initial persistence implementation is appropriate.

**Decision:** Maintain database-level constraints as the authoritative protection against duplicate business records.

### 2.4 Testing

The project includes:

- Domain unit tests.
- Persistence integration tests using SQL Server.
- HTTP integration tests using WebApplicationFactory.
- Testcontainers-based database infrastructure.
- Automated GitHub Actions execution.

**Assessment:** The current test suite covers the implemented core workflows.

**Decision:** Extend testing with concurrency, duplicate message delivery, and failure recovery scenarios during M2.

## 3. Identified Risks and Limitations

| Area | Current Limitation | Planned Action | Priority |
|---|---|---|---|
| Registration concurrency | Concurrent status updates are not yet protected by a dedicated persistence concurrency strategy | Introduce and test concurrency-safe result processing | High — before allocation-result consumers |
| Reliable messaging | Registration creation does not persist outgoing messages | Implement Transactional Outbox | High — M2 |
| Message idempotency | Incoming allocation results are not yet processed | Implement durable deduplication and idempotent consumers | High — M2 |
| Capacity allocation | No authoritative Allocation database or worker exists | Implement independent Allocation service | High — M2 |
| Session publication | Development API permits registration for unpublished sessions | Introduce publication and eligibility controls | High — before public registration |
| Authentication and authorization | HTTP endpoints are currently unauthenticated | Protect sensitive and administrative operations | High — before public deployment |
| API idempotency | No Idempotency-Key contract or persistence exists | Implement request idempotency | High — before reliable distributed workflow |
| API error consistency | Error responses are handled within individual endpoint groups | Evaluate centralized error conventions when complexity increases | Medium |
| Query scalability | Collection endpoints do not yet expose pagination | Add pagination when larger datasets are introduced | Medium |
| Operational readiness | Basic health checks do not verify complete service readiness | Introduce readiness checks and monitoring | Medium |

## 4. Distributed Systems Readiness

### 4.1 Service Boundaries

The system will maintain two independent business data owners.

**Registration Service**

Responsible for:

- Conference and session catalog.
- Participant information.
- Registration records and lifecycle.
- Registration-related outgoing messages.
- Processing allocation results.

**Allocation Service**

Responsible for:

- Session inventory.
- Available capacity.
- Atomic seat allocation.
- Allocation records.
- Processing registration requests.
- Publishing allocation results.

Services must not directly modify each other's databases.

### 4.2 Transactional Outbox

Registration creation and recording the corresponding outgoing business message must occur within the same local database transaction.

This prevents the system from accepting a registration without durably recording the intent to request allocation.

A separate publisher will deliver stored messages to RabbitMQ.

Message delivery must tolerate retries and duplicate publication.

### 4.3 Idempotent Message Processing

The Allocation service must safely handle repeated delivery of the same registration request.

Duplicate messages must not allocate additional seats.

Durable message-processing records and database constraints will protect against duplicate business effects.

### 4.4 Capacity Consistency

The Allocation service must guarantee:

`0 <= ReservedCount <= Capacity`

Concurrent registration requests must not cause overbooking.

Capacity allocation will use atomic database operations or an equivalent concurrency-safe strategy.

The selected implementation must be verified using concurrent integration tests.

### 4.5 Registration Status Consistency

Allocation results may arrive more than once.

Concurrent or conflicting allocation results must not silently overwrite an existing terminal registration state.

Registration result processing requires an explicit concurrency control strategy.

## 5. Testing Strategy for M2

The next development phase should introduce tests covering:

- Successful asynchronous registration processing.
- Rejected allocation when capacity is exhausted.
- Concurrent allocation requests.
- Duplicate message delivery.
- Duplicate allocation results.
- Recovery after publisher or consumer failures.
- Registration status convergence.
- Prevention of overbooking.

Tests should use real infrastructure where database or messaging behavior is relevant.

## 6. Architectural Decisions

The following decisions are retained:

1. Minimal APIs remain the HTTP interface for the Registration service.
2. The Registration and Allocation services own separate databases.
3. SQL Server remains the authoritative source for relational consistency.
4. RabbitMQ will provide initial asynchronous communication.
5. Transactional Outbox will protect reliable outgoing message publication.
6. Consumers must be idempotent.
7. The Allocation service will own seat capacity.
8. Kafka will be introduced later for event streaming and analytics.
9. Infrastructure must remain reproducible using free local tools.
10. Additional abstractions will be introduced only when justified by actual requirements.

## 7. M2 Readiness

The Registration API provides a suitable foundation for beginning distributed messaging development.

The next phase must implement the missing reliability and allocation mechanisms rather than assuming they already exist.

The following conditions must be satisfied before the distributed registration workflow can be considered complete:

- Registration requests are durably connected to outgoing messages.
- The Allocation service maintains authoritative capacity.
- Concurrent allocation requests cannot cause overbooking.
- Duplicate messages cannot produce duplicate business effects.
- Allocation results update registrations safely.
- Failures and retries are covered by automated tests.

## 8. Review Outcome

**Decision: Proceed to M2 — Messaging & Allocation.**

The M1 architecture is accepted as a foundation for the next development phase.

Identified limitations remain tracked development requirements and must be resolved before the corresponding functionality is presented as complete or production-ready.

The architecture will continue to evolve through focused, measurable improvements.