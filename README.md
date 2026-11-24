# ConferenceProFlow

[![.NET CI](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml/badge.svg?branch=master)](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml)

### Enterprise-style distributed conference management in .NET 10

ConferenceProFlow is an incremental, production-aware backend portfolio project demonstrating **asynchronous messaging, data consistency, concurrency-safe resource allocation, local infrastructure and automated integration tests**.

It is intentionally designed to run on a developer machine with Docker and .NET **without paid cloud services**. The platform is **not yet a production-ready public service**; implementation boundaries and remaining work are documented explicitly.

## Implemented Architecture (through M3.5)

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

**Additional M3.3 analytics publication path (independent of RabbitMQ):**

```text
Registration API: Allocation Result reconciliation (Pending -> final)
  +-- Registration SQL transaction
       |-- Registration status + AllocationResultInboxMessage
       +-- AnalyticsOutboxMessage (RegistrationFinalizedV1)
                |
                v
       KafkaAnalyticsOutboxPublisherService
                |
                v
       Apache Kafka (conferenceproflow.registration-finalized.v1)

Analytics Worker (.NET 10 Generic Host; implemented M3.4)
  |-- KafkaAnalyticsConsumerService (durable consumer group)
  |-- KafkaAnalyticsRecordHandler (validate event and session key)
  |-- AnalyticsEventProcessingService (SQL transaction + deduplication)
  |-- AnalyticsQuarantineService (invalid/conflicting records)
  +-- Analytics SQL database: ConferenceProFlowAnalytics
       |-- ProcessedAnalyticsEvents (MessageId PK; RegistrationId unique)
       |-- SessionRegistrationStats (confirmed/rejected by session)
       +-- QuarantinedAnalyticsEvents (Topic, Partition, Offset PK)

Analytics Kafka offsets are committed explicitly AFTER durable SQL handling;
SQL and Kafka offset commits are not one distributed transaction.
```

**Additional M3.5 replay path (separate mode of the Analytics Worker):**
```text
Apache Kafka: conferenceproflow.registration-finalized.v1
  |-- Live group: conferenceproflow.analytics.v1 -> ConferenceProFlowAnalytics
  +-- Isolated replay group: conferenceproflow.analytics.replay.<run-id>
        -> KafkaAnalyticsReplayRunner (bounded partition snapshot)
        -> existing durable record handler / SQL idempotency / quarantine
        -> ConferenceProFlowAnalyticsReplay (separate SQL database)
```
Replay does not reset live offsets or write to the live Analytics database. A fresh run requires an **empty migrated replay database** and an **unused replay group**. Replays reconstruct only events still retained by Kafka.

The Registration API and Allocation Worker **own separate operational databases** and communicate using versioned RabbitMQ integration events. Analytics Worker has a **third independently owned SQL database** and reads Kafka events; it does not query operational databases. No distributed SQL transaction spans these service-owned databases. Registrations are initially `Pending` and are later reconciled to `Confirmed` or `Rejected` (eventual consistency). Delivery is **at least once**, not exactly once; SQL constraints and idempotent handlers protect against repeated business processing. Publisher confirmation, consumer ACK and final business outcome mean different things.

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
- Local Docker Compose for SQL Server 2022, RabbitMQ, and optional Apache Kafka 4.4.0 (KRaft); reproducible Kafka topic initialization.
- Registration-owned Analytics Transactional Outbox with `RegistrationFinalizedV1`, independently enabled Kafka publisher, Kafka acknowledgement-based publication state, retries/recovery, and SQL/Kafka Testcontainers integration tests.
- Independent Analytics Worker (.NET 10 Generic Host) with `Confluent.Kafka` manual commits, `AnalyticsDbContext` and `InitialAnalyticsPersistence` EF Core migration.
- Durable SQL idempotency ledger keyed by `MessageId` and unique `RegistrationId`; SQL-atomic per-session `ConfirmedCount` / `RejectedCount` projections with transaction-scoped `sp_getapplock` coordination.
- SQL quarantine of malformed, invalid-key or conflicting events; offsets committed only after successful SQL processing, recognized duplicate or successful quarantine persistence.
- Analytics xUnit/SQL Server/Kafka Testcontainers integration tests and a manual end-to-end demonstration of stop/restart catch-up and duplicate redelivery.
- Guarded, one-shot, **bounded Kafka replay (M3.5)** using a dedicated consumer group, per-partition low/high watermarks, local snapshot manifest, explicit offset commits after durable replay SQL handling, and explicit resume.
- A fourth *logical* SQL database, `ConferenceProFlowAnalyticsReplay`, on the **same** local SQL Server container, populated using the existing `AnalyticsDbContext`/migration; the live Analytics database remains independent.
- SQL/Kafka Testcontainers replay integration tests for empty/multiple partitions, duplicates, quarantine, late records, SQL failure/resume, completed-run guard and loss of retained records.
- GitHub Actions build/test workflow.

**M2.6 verified locally:** A session with capacity 2 received three registrations; two became `Confirmed`, one `Rejected`, and Registration/Allocation databases agreed on the results. A separate recovery exercise showed a registration remained `Pending` while the Allocation Worker was stopped and completed after the Worker restarted. This is a targeted manual demonstration, not exhaustive failure testing.

**M3.3 locally verified:** Kafka publisher delivers actual finalization events (2 confirmed / 1 rejected in the capacity-2 demonstration), updates the dedicated SQL Analytics Outbox, and recovers from temporary Kafka unavailability. The Kafka publisher Testcontainers suite includes **four** scenarios, including a Kafka-ACK/SQL-update failure that deliberately produces an at-least-once redelivery with a stable logical event identity. M3.3 was merged after regression/CI validation; its publication behavior continues to be exercised by the M3.4 end-to-end demonstration.

**M3.4 locally verified:** A fresh capacity-2 session produces 2 Confirmed / 1 Rejected both in the authoritative operational workflow and in Analytics SQL; stopping Analytics Worker while a fourth registration is rejected leaves a temporary 2/1 projection, then restarting with the same consumer group catches up to 2/2. Re-publishing the original Kafka event (`MessageId` unchanged) is recognized as a duplicate without incrementing counts. Local Release build, regression tests, and the M3.4 Pull Request CI were reported successful.

**M3.5 locally validated (feature PR/CI pending):** A fresh capacity-2 session reached 2 Confirmed / 1 Rejected in both live and replay Analytics SQL; the three finalized events matched by identity, and live consumer-group offsets stayed unchanged across the bounded replay. Six new Kafka + SQL Server Testcontainers replay test cases also passed locally, including injected SQL failure and explicit resume, and retention-gap refusal. Full Analytics regression, the complete solution Release build and the full solution regression suite passed locally on 24 November 2026; Pull Request review and GitHub Actions CI are still pending.

## Technology Stack

| Area | Implemented |
|---|---|
| Backend | C#, .NET 10 LTS, ASP.NET Core Minimal API, .NET Generic Host |
| Persistence | SQL Server 2022, EF Core 10, migrations and relational constraints |
| Messaging | RabbitMQ / `RabbitMQ.Client` 7.x for operational work; Apache Kafka 4.4.0 and `Confluent.Kafka` for publishing and durable Analytics consumption |
| Consistency | Local SQL transactions, idempotent processing, optimistic concurrency (`RowVersion`); Analytics transaction-scoped SQL locks and explicit post-SQL Kafka offset commits |
| Testing | xUnit v3, Testcontainers for .NET, SQL Server / RabbitMQ / Kafka integration tests |
| Development | Docker Compose, User Secrets, Visual Studio, GitHub Actions CI |

Kafka infrastructure and **Registration-side Kafka publishing** are implemented through M3.3; **Analytics consumption, SQL projections and quarantine** are implemented through M3.4. Controlled isolated replay (M3.5) is implemented and locally verified; Kubernetes, distributed observability and performance qualification remain later work. MassTransit is **not used** by the implemented RabbitMQ pipeline.

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

### Optional Kafka Publishing, Analytics and Replay (M3.3–M3.5)

Follow [Kafka Local Development](docs/KAFKA_LOCAL_DEVELOPMENT.md) to start the optional Kafka Compose profile and initialize topics. Then follow the **[Kafka Publishing (M3.3) guide](docs/KAFKA_PUBLISHING.md)** to configure `KafkaAnalyticsPublisher:Enabled` and Kafka bootstrap/topic settings in Registration API User Secrets, run a finalization demonstration, inspect `AnalyticsOutboxMessages`, read broker events, and test Kafka-outage recovery.

To **consume** these finalized events and build SQL projections, configure the independent `ConferenceProFlow.Analytics.Worker` as documented in [Analytics Worker (M3.4)](docs/ANALYTICS_WORKER.md). Apply the existing `InitialAnalyticsPersistence` EF Core migration in a new development environment, keep its own `ConnectionStrings:AnalyticsDb` in User Secrets, then opt into durable consumption with `AnalyticsKafkaConsumer:Enabled=true`, `ObserveOnly=false` and `GroupId=conferenceproflow.analytics.v1`. The [Analytics E2E guide](docs/ANALYTICS_E2E_DEMO.md) demonstrates a fresh-session 2/1 projection, catch-up after restart to 2/2, and duplicate safety.

For a **separate, bounded projection rebuild**, see [Kafka Replay Design](docs/KAFKA_REPLAY_DESIGN.md), [Replay Setup](docs/KAFKA_REPLAY_SETUP.md), [Replay Runner](docs/KAFKA_REPLAY_RUNNER.md), and [Replay E2E Demonstration](docs/KAFKA_REPLAY_E2E_DEMO.md). Use a distinct `AnalyticsReplay:GroupId` prefixed `conferenceproflow.analytics.replay.`, `ConnectionStrings:AnalyticsReplayDb` targeting `ConferenceProFlowAnalyticsReplay`, and `AnalyticsKafkaConsumer:Enabled=false` while `AnalyticsReplay:Enabled=true`. Never run replay against the live Analytics database or reset the live group's offsets.

Kafka remains **optional for the M2 RabbitMQ registration/allocation workflow**. When an Analytics Worker is disabled, Registration finalization is unaffected and Kafka events are retained only for the configured topic retention period.

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
- [Kafka Analytics Architecture (M3.1)](docs/KAFKA_ANALYTICS_DESIGN.md)
- [Kafka Local Development (M3.2)](docs/KAFKA_LOCAL_DEVELOPMENT.md)
- [Kafka Publishing & Reliability Demo (M3.3)](docs/KAFKA_PUBLISHING.md)
- [Analytics Worker Architecture and Operations (M3.4)](docs/ANALYTICS_WORKER.md)
- [Analytics End-to-End and Restart/Duplicate Demo (M3.4F)](docs/ANALYTICS_E2E_DEMO.md)
- [Analytics SQL Verification (M3.4E)](docs/ANALYTICS_SQL_VERIFICATION.md)
- [Kafka Replay Design (M3.5)](docs/KAFKA_REPLAY_DESIGN.md)
- [Replay Setup and Safety (M3.5B)](docs/KAFKA_REPLAY_SETUP.md)
- [Bounded Kafka Replay Runner (M3.5C)](docs/KAFKA_REPLAY_RUNNER.md)
- [Replay E2E Demo and Cross-DB Comparison (M3.5D)](docs/KAFKA_REPLAY_E2E_DEMO.md)
- [Replay Automated Integration Tests (M3.5E)](docs/KAFKA_REPLAY_INTEGRATION_TESTS.md)

## Project Status and Next Steps

**M1 — Core Registration API: complete. M2 — RabbitMQ Messaging & Allocation: complete. M3.1–M3.4 — Kafka design, infrastructure, publication and durable Analytics Worker: complete and merged. M3.5 — isolated bounded Kafka replay: implemented, locally demonstrated and verified with full-solution regression tests; PR review and GitHub Actions CI pending.**

Next: **open the M3.5 Pull Request and verify GitHub Actions CI**, followed by the M3.6 milestone-wide review/hardening. Automatic session inventory provisioning, production authentication and operational monitoring remain separate future work.

### Explicit Current Limitations

- No automated provisioning of `SessionInventory` when a new Registration session is created; the local demo uses a guarded SQL script against AllocationDb. **This is for development only.**
- Authentication and authorization are not implemented; some unpublished-session requests are accepted for development/testing.
- RabbitMQ delivery is **at least once**, not exactly once; duplicate deliveries are handled by the current idempotency logic. Parking queues require trusted manual investigation; there is no audited replay interface or complete operational alerting.
- Kafka publication is also **at least once**. Kafka producer idempotence does not prevent SQL/Kafka acknowledgement-gap redelivery. The M3.4 Analytics Consumer uses SQL deduplication and durable quarantine, but SQL and Kafka offset acknowledgement are not atomic. Earlier M3.2 smoke records may still be retained and can be quarantined.
- Outbox publishers (including the M3.3 Kafka publisher) and the SQL Inbox processor are not designed to coordinate arbitrary numbers of active replicas via durable claims or leases. The Allocation processor's transient failure cooldown is in memory; Kafka publishing does not implement automatic poison-event quarantine.
- The Analytics Worker is locally validated as a **single active instance**. Consumer rebalance/rolling-deployment behavior and multi-replica operational safety are not comprehensively qualified. Poison/quarantined data requires manual investigation; there is no automated remediation/replay dashboard.
- M3.5 replay rebuilds an **isolated projection only from Kafka records retained at snapshot time**. It cannot reconstruct already-expired records or certify a complete historical rebuild of the live database. The snapshot is a local file and replay has one-process/local-lock semantics, not multi-node coordination.
- The current design does not provide global distributed transactions, guaranteed FIFO business decisions across services, or comprehensive automated full-system E2E and chaos tests.
- Production secrets management, operational monitoring/alerting, capacity/performance qualification and stronger security boundaries remain future work.

The focus is **production-aware engineering with explicit tradeoffs**, not a claim that this incremental portfolio platform is production-ready.
