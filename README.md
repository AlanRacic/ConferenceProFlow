# ConferenceProFlow

[![.NET CI](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml/badge.svg?branch=master)](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml)

### Enterprise-style distributed conference management in .NET 10

ConferenceProFlow is an incremental, production-aware backend portfolio project demonstrating **asynchronous messaging, data consistency, concurrency-safe resource allocation, local infrastructure and automated integration tests**.

It is intentionally designed to run on a developer machine with Docker and .NET **without paid cloud services**. The platform is **not yet a production-ready public service**; implementation boundaries and remaining work are documented explicitly.

## Implemented Architecture (M2.5)

```text
Registration API (.NET 10)
  |-- RegistrationDb (SQL Server)
  |     |-- Registrations (initially Pending)
  |     +-- OutboxMessages (same local transaction)
  |-- OutboxPublisherService
  v
RabbitMQ (durable topic exchange, registration-request queue)
  v
Allocation Worker (.NET 10 Generic Host)
  |-- RegistrationRequestConsumerService
  |     +-- SQL InboxMessage + manual RabbitMQ ACK after durable receipt
  |-- AllocationInboxProcessorService
  |     +-- AllocationProcessingService
  |           |-- SessionInventory capacity / RowVersion
  |           |-- Allocation Confirmed or Rejected
  |           +-- atomic inventory + allocation + Inbox completion
  v
AllocationDb (independently owned SQL Server database)
```

The Registration and Allocation services **do not access each other's databases**. RabbitMQ messages follow at-least-once delivery semantics, so both Inbox intake and business allocation are designed to be idempotent. Publisher confirmation and consumer ACK are distinct from the eventual business decision.

### Currently implemented

- ASP.NET Core Minimal API for conferences, sessions, participants and registration requests.
- SQL Server / EF Core with version-controlled migrations and constraints.
- Transactional Outbox with publisher confirms, retry scheduling and payload quarantine.
- RabbitMQ direct client integration, durable topology and consumer-side recovery.
- Durable SQL Inbox, validation, duplicate/conflict handling, manual ACK and a parking queue.
- Capacity-aware Allocation decisions using domain rules, SQL transactions, `RowVersion` optimistic concurrency, unique indexes and bounded retries.
- A background SQL Inbox processor for eligible pending requests; restart recovery and failure isolation.
- Automated xUnit v3 unit and Testcontainers SQL Server/RabbitMQ integration tests, including concurrent seat allocation scenarios.
- Local Docker Compose for SQL Server 2022 and RabbitMQ; GitHub Actions build and test workflow.

**Currently important distinction:** Allocation can confirm/reject **in its own database**, but the corresponding Registration record remains **Pending**. Allocation-result events and Registration-side status reconciliation are not yet implemented.

## Technology Stack

| Area | Implemented |
|---|---|
| Backend | C#, .NET 10 LTS, ASP.NET Core Minimal API, .NET Generic Host |
| Persistence | SQL Server 2022, EF Core 10, migrations and relational constraints |
| Messaging | RabbitMQ and `RabbitMQ.Client` 7.x, Transactional Outbox and SQL Inbox |
| Consistency | Local SQL transactions, idempotent processing, optimistic concurrency (`RowVersion`) |
| Testing | xUnit v3, Testcontainers for .NET, SQL Server / RabbitMQ integration tests |
| Development | Docker Compose, User Secrets, Visual Studio, GitHub Actions CI |

Kafka, Kubernetes, distributed observability and performance-testing tools are **possible later phases**, not required components of the current solution. MassTransit is **not currently used** by the implemented RabbitMQ pipeline.

## Quick Start (Local)

**Prerequisites:** .NET 10 SDK, Docker Desktop (Linux containers), Git and a compatible development environment. See the detailed [Local Development Guide](docs/LOCAL_DEVELOPMENT.md) for safe configuration and migrations.

1. Copy `.env.example` to `.env`, setting local SQL Server and RabbitMQ credentials. The private `.env` must never be committed.
2. Configure separate Registration and Allocation SQL connection strings and RabbitMQ settings through each project's **User Secrets**.
3. Apply the existing EF Core migrations for **both** databases if not already applied.
4. From the repository root, start the local infrastructure:

   ```powershell
   docker compose --env-file .env -f infrastructure/compose.yaml up -d
   ```

5. Run the Registration API (HTTPS profile) and Allocation Worker (Development profile) in two terminals or through Visual Studio. Enable `OutboxPublisher:Enabled`, `RabbitMqConsumer:Enabled` and `AllocationInboxProcessor:Enabled` in the appropriate development User Secrets.
6. Follow the [end-to-end allocation demo](docs/ALLOCATION_E2E_DEMO.md) to create a conference session, provision **development-only** inventory, submit three registrations and verify capacity-safe Allocation results.

The Registration API HTTPS development endpoint is `https://localhost:7217`. Its example requests are in [`docs/REGISTRATION_API.http`](docs/REGISTRATION_API.http). The OpenAPI JSON is exposed in Development at `/openapi/v1.json`; interactive Swagger UI is not configured.

### End-to-End Demo

The [M2.5 demo guide](docs/ALLOCATION_E2E_DEMO.md) contains a reproducible path with two seats and three registration requests, plus SQL queries verifying **two Confirmed and one Rejected Allocation** without overbooking. The companion [HTTP requests](docs/ALLOCATION_E2E.http) and [guarded SQL seed](docs/ALLOCATION_E2E_SEED.sql) avoid hand-assembling HTTP requests or guessing SQL table structure.

The SQL seed is **for local development only**. Automated production synchronization of session inventory does not yet exist.

## Build and Test

From the repository root, with Docker Desktop running for Testcontainers integration tests:

```powershell
dotnet restore ConferenceProFlow.slnx
```

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release --no-restore
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

The current GitHub Actions workflow runs restore, Release build, Docker availability check and the solution test command on PRs targeting `master` and pushes to `master`. A green workflow means the configured tests succeeded; it does not prove every imaginable infrastructure-failure scenario.

## API Endpoints

| Method | Path | Function |
|---|---|---|
| POST | `/api/conferences` | Create a conference |
| GET | `/api/conferences` | List conferences |
| GET | `/api/conferences/{id}` | Get conference |
| POST | `/api/conferences/{id}/sessions` | Create a session |
| GET | `/api/conferences/{id}/sessions` | List sessions |
| GET | `/api/conferences/{id}/sessions/{sessionId}` | Get session |
| POST | `/api/participants` | Create a participant |
| GET | `/api/participants/{id}` | Get participant |
| POST | `/api/registrations` | Create a Pending registration |
| GET | `/api/registrations/{id}` | Get the Registration-side status |

## Documentation

- [Project Charter](docs/PROJECT_CHARTER.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Domain Model](docs/DOMAIN_MODEL.md)
- [Messaging Design and Reliability](docs/MESSAGING_DESIGN.md)
- [Local Development Guide](docs/LOCAL_DEVELOPMENT.md)
- [M2.5 End-to-End Allocation Demo](docs/ALLOCATION_E2E_DEMO.md)
- [Registration API HTTP Examples](docs/REGISTRATION_API.http)

## Project Status and Next Steps

**M1 — Registration API: Complete. M2.3–M2.4 — Outbox, RabbitMQ and SQL Inbox: Complete. M2.5 — Concurrency-safe business allocation and background processing: implemented, pending final review and PR merge.**

Planned improvements include reliable Allocation-result events and Registration-side status updates; automatic inventory provisioning; session-publication/registration eligibility rules; access control; operational tooling, instrumentation, and eventual additional infrastructure only where justified.

### Explicit Current Limitations

- No automated provisioning of `SessionInventory` from a newly created Registration session (the demo uses a guarded local SQL script).
- No Allocation-result message and no Registration status reconciliation; the Registration database still reports Pending.
- Authentication and authorization are not implemented; Registration currently accepts some unpublished-session requests for development/testing.
- RabbitMQ delivery is at least once, **not exactly once**. Parking queue requires manual investigation; there is no audited replay interface.
- The Outbox publisher and SQL Inbox processor do not yet coordinate multiple active replicas through durable row claims or leases. Transient Inbox failure cooldown is in memory.
- Operational alerting, production secrets management, performance qualification, and full-system automated E2E tests remain future work.

The focus is production-aware engineering and clear tradeoffs—not claiming the entire distributed platform is production-ready before those gaps are addressed.