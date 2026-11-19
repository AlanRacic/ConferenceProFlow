# ConferenceProFlow

[![.NET CI](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml/badge.svg?branch=master)](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml)

### Enterprise-style distributed conference management in .NET 10

ConferenceProFlow is an incremental, production-aware backend portfolio project demonstrating **asynchronous messaging, data consistency, concurrency-safe resource allocation, local infrastructure and automated integration tests**.

It is intentionally designed to run on a developer machine with Docker and .NET **without paid cloud services**. The platform is **not yet a production-ready public service**; implementation boundaries and remaining work are documented explicitly.

## Implemented Architecture (M2.6)

```text
Registration API (.NET 10)
  |-- RegistrationDb (SQL Server)
  |     |-- Registrations (Pending -> Confirmed / Rejected)
  |     |-- OutboxMessages (registration-request events)
  |     +-- AllocationResultInboxMessages (result-event receipts)
  |-- OutboxPublisherService
  |-- AllocationResultConsumerService
  |     +-- AllocationResultReconciliationService
  |            +-- Registration status + result Inbox committed together
  |
  | registration.requested.v1             ^ allocation.decided.v1
  v                                       |
RabbitMQ (durable topic exchange and service-owned queues)
  |                                       ^
  v                                       |
Allocation Worker (.NET 10 Generic Host)  |
  |-- RegistrationRequestConsumerService  |
  |     +-- durable SQL Inbox + ACK        |
  |-- AllocationInboxProcessorService     |
  |     +-- AllocationProcessingService   |
  |           +-- capacity-safe Confirmed / Rejected decision
  |           +-- inventory + allocation + Inbox + result Outbox atomically
  |-- Allocation result Outbox publisher (BackgroundService)
  |     +-- publish allocation.decided.v1 after commit
  +---------------------------------------+

AllocationDb (separately owned SQL Server database)
  |-- SessionInventories (Capacity / ReservedCount / RowVersion)
  |-- InboxMessages
  |-- Allocations
  +-- Allocation result Outbox (durable events)
```

The two services **own separate databases** and communicate using versioned RabbitMQ integration events. No distributed SQL transaction spans both databases. Registrations are initially `Pending` and are later reconciled to `Confirmed` or `Rejected` (eventual consistency). Delivery is **at least once**, not exactly once; SQL constraints and idempotent handlers protect against repeated business processing. Publisher confirmation, consumer ACK and final business outcome mean different things.

### Currently implemented

- ASP.NET Core Minimal API for conferences, sessions, participants and registrations.
- SQL Server / EF Core 10 with migrations, relational constraints and unique indexes.
- Registration-side Transactional Outbox for `RegistrationRequestedV1` and Allocation-side Transactional Outbox for `AllocationDecidedV1`.
- RabbitMQ direct client integration (`RabbitMQ.Client` 7.x) with durable topology, persistent messages, publisher confirmations, manual consumer ACKs and recovery handling.
- Durable SQL Inbox processing on both sides, duplicate/conflict detection, and service-owned parking queues for invalid/conflicting deliveries.
- Capacity-aware Allocation decisions with SQL transaction consistency, `RowVersion` optimistic concurrency and bounded retries.
- A background Allocation Inbox processor and an Allocation result Outbox publisher.
- Registration-side result consumer and reconciliation service that atomically persist the final status and result Inbox receipt.
- Automated xUnit v3 unit tests and SQL Server/RabbitMQ Testcontainers integration tests, including recovery and concurrency scenarios.
- Local Docker Compose for SQL Server 2022 and RabbitMQ; GitHub Actions build/test workflow.

**M2.6 verified locally:** A session with capacity 2 received three registrations; two became `Confirmed`, one `Rejected`, and Registration/Allocation databases agreed on the results. A separate recovery exercise showed a registration remained `Pending` while the Allocation Worker was stopped and completed after the Worker restarted. This is a targeted manual demonstration, not exhaustive failure testing.

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

5. Run the Registration API (HTTPS profile) and Allocation Worker (Development profile) in two terminals or through Visual Studio. In the Registration API User Secrets enable `OutboxPublisher:Enabled` and `AllocationResultConsumer:Enabled`; in the Allocation Worker User Secrets enable `RabbitMqConsumer:Enabled`, `AllocationInboxProcessor:Enabled` and `AllocationOutboxPublisher:Enabled`. These services also require each application's existing database/RabbitMQ settings.
6. Follow the [end-to-end allocation demo](docs/ALLOCATION_E2E_DEMO.md): create a conference and session, provision **development-only** inventory, submit three registrations, then verify the final statuses in the Registration API and both databases.

The Registration API HTTPS development endpoint is `https://localhost:7217`. Its example requests are in [`docs/REGISTRATION_API.http`](docs/REGISTRATION_API.http). The OpenAPI JSON is exposed in Development at `/openapi/v1.json`; interactive Swagger UI is not configured.

### End-to-End Demo

The [end-to-end demo guide](docs/ALLOCATION_E2E_DEMO.md) uses two seats and three registration requests, then verifies **two Confirmed and one Rejected Registration**, corresponding Allocation decisions, and no overbooking. The [HTTP requests](docs/ALLOCATION_E2E.http) reference IDs from prior responses, reducing manual GUID entry; a [guarded SQL seed](docs/ALLOCATION_E2E_SEED.sql) provisions inventory for the demo. The final Registration GET responses and SQL verification illustrate eventual consistency.

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
- [M2.6 End-to-End Allocation and Reconciliation Demo](docs/ALLOCATION_E2E_DEMO.md)
- [Registration API HTTP Examples](docs/REGISTRATION_API.http)

## Project Status and Next Steps

**M1 — Registration API: Complete. M2.3–M2.4 — Registration Outbox, RabbitMQ and Allocation SQL Inbox: Complete. M2.5 — Capacity-safe allocation: implemented. M2.6 — Allocation result publishing and Registration reconciliation: implemented and locally verified; final documentation review, full Release regression and CI/PR merge remain part of milestone completion.**

Potential future work includes automatic session inventory provisioning; publication/registration eligibility rules; authentication/authorization; operational replay and monitoring; multi-replica coordination; and additional load/failure testing where justified.

### Explicit Current Limitations

- No automated provisioning of `SessionInventory` when a new Registration session is created; the local demo uses a guarded SQL script against AllocationDb. **This is for development only.**
- Authentication and authorization are not implemented; some unpublished-session requests are accepted for development/testing.
- RabbitMQ delivery is **at least once**, not exactly once; duplicate deliveries are handled by the current idempotency logic. Parking queues require trusted manual investigation; there is no audited replay interface or complete operational alerting.
- Outbox publishers and the SQL Inbox processor are not designed to coordinate arbitrary numbers of active replicas via durable claims or leases. The Allocation processor's transient failure cooldown is in memory.
- The current design does not provide global distributed transactions, guaranteed FIFO business decisions across services, or comprehensive automated full-system E2E and chaos tests.
- Production secrets management, operational monitoring/alerting, capacity/performance qualification and stronger security boundaries remain future work.

The focus is **production-aware engineering with explicit tradeoffs**, not a claim that this incremental portfolio platform is production-ready.