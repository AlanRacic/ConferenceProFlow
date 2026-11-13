# ConferenceProFlow — Local Development

## Prerequisites

The local development environment requires:

* .NET 10 SDK
* Docker Desktop or another compatible Docker Engine with Docker Compose
* Git
* Visual Studio 2026 or another compatible .NET development environment

Docker must support Linux containers.

## SQL Server

The project uses SQL Server 2022 Developer Edition for local development and testing.

SQL Server runs in a Docker container and does not require a separate local SQL Server installation.

### Configuration

From the repository root, copy the example environment file:

```powershell
Copy-Item .env.example .env
```

Open `.env` and configure a strong local SQL Server administrator password.

The `.env` file is excluded from Git and must not be committed.

### Start SQL Server

Run the following command from the repository root:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d
```

### Check Container Status

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml ps
```

### View Logs

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml logs --tail=50 sqlserver
```

### Database Connection

Use the following connection settings:

* Server: `127.0.0.1,14333`
* Authentication: SQL Server Authentication
* Username: `sa`
* Password: Value configured in the local `.env` file

Trusting the local development server certificate may be necessary.

### Stop Infrastructure

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml down
```

Named Docker volumes retain database data when containers are stopped or removed normally.

Do not use `down -v` unless intentionally resetting the local database.

## Application Development

The Registration API is an ASP.NET Core Minimal API targeting .NET 10.

For local development, the Registration API runs through Visual Studio or `dotnet run`, while SQL Server and RabbitMQ run in Docker Compose.

### Start the Registration API

Before starting the application:

1. Ensure Docker Desktop is running.
2. Start the development SQL Server using the Docker Compose instructions in this guide.
3. Configure the Registration API connection string using ASP.NET Core User Secrets.
4. Apply the existing EF Core migrations if the database has not yet been initialized.

In Visual Studio:

1. Open `ConferenceProFlow.slnx`.
2. Set `ConferenceProFlow.Registration.Api` as the startup project.
3. Select the HTTPS development profile.
4. Press `F5` to start the application.

The default HTTPS development profile uses:

`https://localhost:7217`

The health endpoint is available at:

`https://localhost:7217/health`

A successful response returns `Healthy`.

The health endpoint currently verifies basic application availability. It does not establish that the SQL Server database is ready.

### OpenAPI Documentation

When running in the Development environment, the generated OpenAPI document is available at:

`https://localhost:7217/openapi/v1.json`

This endpoint returns an OpenAPI JSON document describing the HTTP API.

An interactive Swagger UI is not currently configured.

### Test HTTP Endpoints

The repository contains example HTTP requests in:

`docs/REGISTRATION_API.http`

Open this file using Visual Studio's HTTP editor.

Ensure that the Registration API and development SQL Server are running before sending requests.

The file contains examples for:

- Creating and retrieving conferences.
- Creating and retrieving conference sessions.
- Creating and retrieving participants.
- Creating and retrieving registrations.
- Testing duplicate participant and registration requests.

The HTTP file uses named requests and response variables for the creation workflow. Execute the create-conference, create-session, and create-participant requests before creating a registration. The registration request reads the previously returned `id` values. If Visual Studio reports that a response variable cannot be evaluated, run its prerequisite named request first.

A successful registration creation request returns HTTP `201 Created` with an initial status of `Pending`.

A Pending registration does not represent a confirmed seat allocation.

### Automated HTTP Integration Tests

Automated HTTP integration tests are implemented using ASP.NET Core WebApplicationFactory and xUnit v3.

The tests use Testcontainers to create an isolated SQL Server environment and apply the application's EF Core migrations.

They validate HTTP responses, input validation, database persistence, duplicate handling, and registration status.

Docker Desktop must be running before executing the integration tests.

Use the automated test commands documented in the database testing section of this guide.

### Current Development Limitations

The Registration API is intended for local development and testing. Authentication and authorization have not yet been implemented.

Registration requests can currently be created for unpublished sessions to support development before session-publication rules and allocation inventory provisioning are implemented.

**Registration-to-Allocation messaging intake is implemented** using Transactional Outbox publishing, RabbitMQ, and a durable SQL Inbox. Registration requests remain `Pending` because capacity-safe business allocation, allocation-result handling, and registration status reconciliation are not yet implemented.

The API is not suitable for public production deployment. These limitations are documented rather than hidden.

## Development Principles

The local environment is designed to be reproducible without paid cloud services.

The existing local infrastructure includes SQL Server and RabbitMQ. Additional technologies (such as Kafka or observability components) will be considered only when justified by a specific implementation goal; they are not required to run the current solution.

---

## Registration API — Database Configuration

The Registration API uses Entity Framework Core 10 with Microsoft SQL Server 2022.

The development database is hosted in Docker, while connection credentials are stored locally using ASP.NET Core User Secrets.

The database schema is managed through version-controlled EF Core migrations.

### 1. Start SQL Server

Ensure Docker Desktop is running with Linux containers enabled.

From the repository root, start the development database:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d
```

Verify the container status:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml ps
```

The database server is accessible at:

```text
127.0.0.1,14333
```

The SQL Server instance runs locally and does not require an Azure subscription or paid cloud services.

### 2. Configure ASP.NET Core User Secrets

The Registration API reads its database connection string from the following configuration key:

```text
ConnectionStrings:RegistrationDb
```

In Visual Studio:

1. Right-click `ConferenceProFlow.Registration.Api`.
2. Select **Manage User Secrets**.
3. Add the following development configuration, replacing placeholders with the matching credentials from the local `.env` file. Preserve any other settings already present in your User Secrets:

```json
{
  "ConnectionStrings": {
    "RegistrationDb": "Server=127.0.0.1,14333;Database=ConferenceProFlowRegistration;User Id=sa;Password=YOUR_LOCAL_SQL_PASSWORD;Encrypt=True;TrustServerCertificate=True;"
  },
  "RabbitMq": {
    "HostName": "127.0.0.1",
    "Port": 5672,
    "VirtualHost": "conferenceproflow",
    "UserName": "YOUR_RABBITMQ_USERNAME",
    "Password": "YOUR_RABBITMQ_PASSWORD"
  },
  "OutboxPublisher": {
    "Enabled": true,
    "PublishTimeoutSeconds": 15
  }
}
```

The Registration API needs the RabbitMQ settings when `OutboxPublisher:Enabled` is `true`. This enables background publication of pending SQL Outbox messages. Set `OutboxPublisher:Enabled` to `false` only when intentionally running the API without its publisher. `PublishTimeoutSeconds` is optional; the default is 15 seconds.

User Secrets are stored outside the repository and are intended for local development only.

They are not encrypted and must not be used as a production secrets-management solution.

Never commit real database credentials to Git.

### 3. Restore .NET Tools

The repository includes a local .NET tool manifest for the EF Core CLI.

From the repository root, run:

```powershell
dotnet tool restore
```

Verify that the EF Core CLI is available:

```powershell
dotnet ef --version
```

Using a local tool manifest helps ensure that contributors use the expected version of the EF Core CLI.

### 4. Apply Database Migrations

Ensure SQL Server is running and the Registration API User Secrets are configured.

When using PowerShell, set the environment to Development:

```powershell
$env:ASPNETCORE_ENVIRONMENT = "Development"
```

From the repository root, apply the existing migrations:

```powershell
dotnet ef database update --project src/ConferenceProFlow.Registration.Api --startup-project src/ConferenceProFlow.Registration.Api --context RegistrationDbContext
```

This creates or updates the `ConferenceProFlowRegistration` database.

The Registration database contains the following business and messaging tables after all current migrations are applied:

- `Conferences`
- `ConferenceSessions`
- `Participants`
- `Registrations`
- `OutboxMessages`

The Outbox table stores the serialized integration events, publication state, retry scheduling (`NextAttemptAtUtc`), and quarantine state (`QuarantinedAtUtc`).

EF Core also creates `__EFMigrationsHistory` to track applied migrations.

The database includes primary keys, foreign keys, unique indexes, and other configured constraints.

**Do not manually create these tables.** Their structure is maintained through EF Core migrations.

### 5. Verify the Database

Connect to the local SQL Server using Visual Studio SQL Server Object Explorer or another compatible SQL client.

Use the following connection information:

| Setting | Value |
|---|---|
| Server | `127.0.0.1,14333` |
| Authentication | SQL Server Authentication |
| User | `sa` |
| Password | Password from the local `.env` file |
| Database | `ConferenceProFlowRegistration` |

For local development, enable **Trust Server Certificate** if necessary.

Execute the following SQL query:

```sql
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
```

The result should include the five Registration business/messaging tables and the EF Core migrations history table.

### 6. Run Automated Tests

ConferenceProFlow includes unit tests, HTTP integration tests, SQL Server persistence integration tests, and RabbitMQ integration tests for the publisher and consumer.

The persistence integration tests use **Testcontainers for .NET**.

Testcontainers automatically starts a temporary SQL Server container, applies the application's EF Core migrations, executes the database tests, and removes the container afterward.

**Docker Desktop must be running before executing the database integration tests.**

The existing development SQL Server container does not need to be running.

The database tests do not use or modify the `ConferenceProFlowRegistration` development database.

From the repository root, restore and build the solution:

```powershell
dotnet restore ConferenceProFlow.slnx
```

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release --no-restore
```

Run all automated tests:

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

The persistence integration tests verify:

- Registration persistence across separate DbContext instances.
- Enforcement of the participant-session unique index.
- Enforcement of foreign key constraints.
- Persistence of registration lifecycle changes.
- Atomic creation of a registration and its corresponding SQL Outbox message.

RabbitMQ Outbox-specific tests are documented later in this guide.

The tests use a real SQL Server instance rather than the EF Core InMemory provider.

### 7. Continuous Integration

The GitHub Actions workflow is located at:

```text
.github/workflows/dotnet-ci.yml
```

The workflow restores dependencies, builds the solution, verifies Docker availability, and executes automated tests.

SQL Server and RabbitMQ integration tests use Testcontainers on the GitHub-hosted Ubuntu runner.

No external database credentials or paid cloud services are required.

### 8. Stop the Development Database

To stop the local SQL Server container without deleting its persistent data:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml down
```

The named Docker volume remains available for subsequent development sessions.

Avoid using `down -v` unless you intentionally want to delete the database volume and its contents.

---

## Database Architecture Notes

ConferenceProFlow maintains separate service-owned databases on the same local SQL Server 2022 instance:

| Service | Database | Main data |
|---|---|---|
| Registration API | `ConferenceProFlowRegistration` | Conferences, sessions, participants, registrations, outgoing `OutboxMessages` |
| Allocation Worker | `ConferenceProFlowAllocation` | Session inventory, allocation models, incoming `InboxMessages` |

Each service has its own EF Core DbContext and version-controlled migrations. Neither service directly reads or modifies the other service's database.

Registration creates a `Pending` registration and its outgoing Outbox event atomically. RabbitMQ transfers the versioned integration event to Allocation, which validates it and durably stores its receipt in SQL Inbox. Neither service uses a distributed transaction spanning both databases.

**Receiving an Inbox message does not reserve a seat or confirm a registration.** Concurrency-safe allocation business processing is planned for M2.5, followed by allocation-result events and Registration status reconciliation. See `docs/MESSAGING_DESIGN.md` for the design, reliability semantics, and known limitations.

---

## RabbitMQ — Local Messaging Infrastructure

ConferenceProFlow uses RabbitMQ for asynchronous communication between the Registration API and the Allocation Worker. The official RabbitMQ image with the Management plugin runs in Docker Compose.

### Configuration

The root `.env.example` contains the environment variables needed for local infrastructure. On first setup, copy it and supply strong local credentials:

```powershell
Copy-Item .env.example .env
```

In the local `.env` file, configure RabbitMQ credentials, for example:

```dotenv
RABBITMQ_USER=conferenceproflow
RABBITMQ_PASSWORD=YOUR_STRONG_LOCAL_PASSWORD
```

The Docker Compose file uses these credentials to initialize the `conferenceproflow` virtual host. The SQL Server password (`MSSQL_SA_PASSWORD`) also belongs in `.env`. **Do not commit `.env` or any real credentials.**

### Start RabbitMQ

Ensure Docker Desktop is running. From the repository root:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d rabbitmq
```

Check status:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml ps
```

Verify RabbitMQ application readiness:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml exec rabbitmq rabbitmq-diagnostics -q check_running
```

To start **both** SQL Server and RabbitMQ:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d
```

### Connection Details

| Setting | Value |
|---|---|
| AMQP host | `127.0.0.1` |
| AMQP port | `5672` |
| Management UI | `http://127.0.0.1:15672` |
| Username | Value of `RABBITMQ_USER` |
| Password | Value of `RABBITMQ_PASSWORD` |
| Virtual host | `conferenceproflow` |

Sign in to the Management UI with the locally configured credentials. Both applications run on the host during local Visual Studio development and use the host-mapped AMQP port.

### Persistence

RabbitMQ state is stored in the named Docker volume `rabbitmq_data`. Normal container restarts preserve this volume. Removing the volume also removes persisted broker definitions and messages.

**Important:** Default RabbitMQ credentials and virtual host settings are applied when an empty RabbitMQ data directory is initialized. Changing `.env` later does not automatically update users/passwords in an existing RabbitMQ data volume.

### Stop RabbitMQ

To stop only the RabbitMQ container:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml stop rabbitmq
```

To start the existing container again:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml start rabbitmq
```

To stop and remove the Docker Compose services while preserving named data volumes:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml down
```

Avoid `down -v` unless intentionally deleting both persistent SQL Server and RabbitMQ development data.

### Current Scope

RabbitMQ messaging is implemented between the Registration API and the Allocation Worker:

- Registration publishes versioned `registration.requested.v1` events using a SQL Transactional Outbox.
- The publisher declares a compatible durable topic exchange, request queue, and binding; it uses persistent messages, mandatory routing, and publisher confirmations.
- The Allocation consumer also declares the same compatible exchange, durable request queue, and binding during startup. This reduces dependence on service startup order.
- Allocation consumes from `allocation.registration-requests.v1` with manual acknowledgements and `prefetchCount = 1`.
- Valid incoming requests are saved into a durable SQL Inbox, with message and registration uniqueness enforced at the database level.
- Malformed and conflicting deliveries are moved to `allocation.failed-registration-requests.v1`, a durable parking queue. The original delivery is acknowledged only **after** the parking publication is confirmed.

The current topology is:

| Component | Value |
|---|---|
| Exchange | `conferenceproflow.events` |
| Exchange type | Durable `topic` |
| Routing key | `registration.requested.v1` |
| Main request queue | `allocation.registration-requests.v1` |
| Failure parking queue | `allocation.failed-registration-requests.v1` |

These mechanisms do not guarantee exactly-once delivery or completed seat allocation. For complete failure-handling rules and limitations, see `docs/MESSAGING_DESIGN.md`.

---

## Allocation Worker — Local Development

The Allocation Worker is a standalone .NET 10 Generic Host application responsible for consuming registration requests and, in a later phase, making capacity-safe seat allocation decisions.

The project is located at:

`src/ConferenceProFlow.Allocation.Worker`

### Current Implementation

The Worker includes:

- .NET Generic Host, dependency injection, structured logging, and a cancellation-aware background service.
- EF Core 10, SQL Server 2022, a separate Allocation database, and version-controlled migrations.
- `SessionInventory` and `Allocation` domain entities; capacity-safe business processing is planned for M2.5.
- Consumer-side `RegistrationRequestedV1` contract and JSON validation parser.
- Durable SQL `InboxMessages` table and `InboxReceiveService`.
- Primary-key uniqueness on incoming `MessageId` (`InboxMessage.Id`) and a unique index on `RegistrationId`.
- RabbitMQ.Client `RegistrationRequestConsumerService` using manual acknowledgements, single-message prefetch, and application-managed reconnection.
- Durable parking queue for malformed/conflicting messages.
- Unit tests, SQL Server integration tests, and isolated RabbitMQ Testcontainers consumer integration tests.

`ReceivedAtUtc` means that the valid request was durably accepted by the Allocation database. `ProcessedAtUtc` remains `NULL` until a later phase implements business processing. **No seat is allocated by the current consumer.**

### Configure the Allocation Database and RabbitMQ

The Allocation Worker owns:

`ConferenceProFlowAllocation`

The database runs on the existing local SQL Server instance. The Worker reads its database connection string from:

`ConnectionStrings:AllocationDb`

In Visual Studio, right-click `ConferenceProFlow.Allocation.Worker` and select **Manage User Secrets**. Add the following example configuration, replacing placeholders with the matching credentials from `.env` and preserving any other existing settings:

```json
{
  "ConnectionStrings": {
    "AllocationDb": "Server=127.0.0.1,14333;Database=ConferenceProFlowAllocation;User Id=sa;Password=YOUR_LOCAL_SQL_PASSWORD;Encrypt=True;TrustServerCertificate=True;"
  },
  "RabbitMq": {
    "HostName": "127.0.0.1",
    "Port": 5672,
    "VirtualHost": "conferenceproflow",
    "UserName": "YOUR_RABBITMQ_USERNAME",
    "Password": "YOUR_RABBITMQ_PASSWORD"
  },
  "RabbitMqConsumer": {
    "Enabled": true
  }
}
```

`RabbitMqConsumer:Enabled = true` activates the real `RegistrationRequestConsumerService`. When disabled, the application runs its original placeholder background loop rather than consuming RabbitMQ messages.

The Registration API separately needs its own `RabbitMq` configuration and `OutboxPublisher:Enabled = true` to publish events. See the Registration API configuration section above.

User Secrets are for local development and are stored outside Git. They are not encrypted production-grade secret storage. **Never commit credentials or paste secret values into logs or issues.**

### Apply Database Migrations

Start the development SQL Server:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d sqlserver
```

Restore the local EF Core CLI tool:

```powershell
dotnet tool restore
```

If running EF Core commands outside Visual Studio, set the Worker environment to Development so its User Secrets are loaded:

```powershell
$env:DOTNET_ENVIRONMENT = "Development"
```

Apply the current Allocation migrations:

```powershell
dotnet ef database update --project src/ConferenceProFlow.Allocation.Worker --startup-project src/ConferenceProFlow.Allocation.Worker --context AllocationDbContext
```

After all migrations have been applied, the database contains:

- `SessionInventories`
- `Allocations`
- `InboxMessages`
- `__EFMigrationsHistory`

`InboxMessages` uses `Id` (incoming `MessageId`) as its primary key, a unique index on `RegistrationId`, and a processing index on `ProcessedAtUtc` and `ReceivedAtUtc`.

The schema is managed by EF Core migrations. Do not manually create application tables.

### Run the Allocation Worker

Start both SQL Server and RabbitMQ from the repository root:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d
```

Verify the containers:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml ps
```

Ensure the Worker's User Secrets contain `ConnectionStrings:AllocationDb`, all required `RabbitMq` keys, and `RabbitMqConsumer:Enabled = true`.

When running from a PowerShell terminal outside Visual Studio, set the environment to Development:

```powershell
$env:DOTNET_ENVIRONMENT = "Development"
```

Start the Worker:

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj
```

Alternatively, set `ConferenceProFlow.Allocation.Worker` as the startup project in Visual Studio and press `F5` using its Development environment.

Expected log messages include `Registration Request Consumer started` and `RabbitMQ consumer registered`.

Open RabbitMQ Management at `http://127.0.0.1:15672`, select the `conferenceproflow` virtual host, and inspect **Queues and Streams**. The `allocation.registration-requests.v1` queue should have an active consumer. The `allocation.failed-registration-requests.v1` parking queue is created by the consumer.

For a full local message-intake check:

1. Also run the Registration API with `OutboxPublisher:Enabled = true` and its local RabbitMQ User Secrets configured.
2. Open `docs/REGISTRATION_API.http` in Visual Studio.
3. Send the named **create conference**, **create session**, and **create participant** requests before the **create registration** request, because its request body references the IDs returned by those requests.
4. Confirm that creating a registration returns HTTP `201 Created` and the new status is `Pending`.
5. Inspect `ConferenceProFlowAllocation` with the SQL query below:

```sql
SELECT TOP (10)
    Id,
    RegistrationId,
    SessionId,
    ReceivedAtUtc,
    ProcessedAtUtc
FROM dbo.InboxMessages
ORDER BY ReceivedAtUtc DESC;
```

A successfully received event has a non-`NULL` `ReceivedAtUtc` and a `NULL` `ProcessedAtUtc`. The Worker's ACK means the request was durably received, **not** that a seat was reserved. `ReservedCount` is not changed by this intake stage, and Registration stays `Pending`.

### Stop the Worker

Press `Ctrl + C` when running from the terminal, or stop the Visual Studio debugging session. The `BackgroundService` handles cancellation and attempts graceful shutdown. Unacknowledged RabbitMQ deliveries are eligible for later redelivery when their channel/connection closes.

### Service Ownership

The Allocation Worker exclusively owns `ConferenceProFlowAllocation`. The Registration API exclusively owns `ConferenceProFlowRegistration`. Each service has its own migrations and persistence model; neither service directly accesses or modifies the other's database.

They communicate through versioned RabbitMQ integration events and do not share a distributed SQL transaction.

### Current Limitations

- SQL Inbox persistence establishes **durable intake**, not a completed seat allocation.
- `ProcessedAtUtc` remains `NULL` until the planned Inbox business processor is implemented in M2.5.
- Capacity-safe allocation, session inventory provisioning, allocation-result events, and Registration status reconciliation are future work.
- Consumer dispatch is configured for one unacknowledged delivery at a time; high-throughput and multi-instance load testing are not claimed.
- Malformed/conflicting messages are parked for trusted manual review; there is no audited administrative replay workflow or automated alerting.
- Confirmed parking publication followed by ACK is **not an atomic move**. Duplicate parked copies are possible if a process stops between those two operations.
- A successful RabbitMQ ACK proves that intake or confirmed parking was handled, not exactly-once business execution.

---

## Allocation Worker — Automated Testing

The Allocation Worker has separate unit and integration test projects. Tests use xUnit v3 and .NET 10.

### Unit Tests

The unit-test project is located at:

`tests/ConferenceProFlow.Allocation.UnitTests`

The tests cover:

- Session inventory creation, capacity validation, and initial reserved count.
- UTC timestamp validation.
- Confirmed and Rejected allocation entity creation, required identifiers, and rejection reason validation.
- Inbox entity invariants and initialization.
- `RegistrationRequestedV1` consumer-side JSON parsing and validation.

Run the Allocation unit tests:

```powershell
dotnet test tests/ConferenceProFlow.Allocation.UnitTests/ConferenceProFlow.Allocation.UnitTests.csproj --configuration Release
```

### SQL Server Integration Tests

The integration-test project is located at:

`tests/ConferenceProFlow.Allocation.IntegrationTests`

Allocation integration tests use EF Core and Testcontainers to start an isolated SQL Server instance and apply migrations. They validate:

- Session inventory and allocation persistence.
- Unique registration and foreign-key constraints.
- SQL Server CHECK constraints.
- `RowVersion` generation and optimistic concurrency conflicts.
- SQL Inbox persistence and primary-key/unique-registration constraints.
- Duplicate message identities, conflicting registration identity, and concurrent inserts.

Run Allocation integration tests:

```powershell
dotnet test tests/ConferenceProFlow.Allocation.IntegrationTests/ConferenceProFlow.Allocation.IntegrationTests.csproj --configuration Release
```

Docker Desktop must be running before these tests. Integration tests do not use or modify the local `ConferenceProFlowAllocation` development database.

### RabbitMQ Consumer Integration Tests

The consumer suite exercises the actual `RegistrationRequestConsumerService` with SQL Server Testcontainers and an **isolated RabbitMQ Testcontainer for each consumer test**. Its scenarios include:

- Successful message intake and persistence in SQL Inbox.
- Duplicate `MessageId` delivery and repeated `RegistrationId` handling.
- Malformed JSON and conflicting-registration parking.
- Unacknowledged redelivery and consumer restart.
- Idempotent intake after repeated delivery across restarts.
- Inaccessible SQL endpoint: no premature ACK, redelivery, and recovery with a valid database connection.
- RabbitMQ application restart inside the same container: reconnect and continued consumption.
- Restoration of a missing RabbitMQ queue binding on consumer startup.

Run only RabbitMQ consumer integration tests:

```powershell
dotnet test tests/ConferenceProFlow.Allocation.IntegrationTests/ConferenceProFlow.Allocation.IntegrationTests.csproj --configuration Release --filter "FullyQualifiedName~RegistrationRequestConsumerIntegrationTests" --verbosity normal
```

The RabbitMQ restart integration test restarts the **RabbitMQ application inside the existing container**, keeping its mapped endpoint stable. It does not prove recovery when a replaced broker uses a new hostname or port.

### Run All Tests

Run the full solution from the repository root:

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --verbosity normal
```

The solution contains independent Registration and Allocation test suites. GitHub Actions runs automated tests through the existing CI workflow.

### Current Testing Scope

SQL Inbox idempotency, RabbitMQ intake, manual-ACK-related scenarios, redelivery, and selected infrastructure recovery paths are covered by automated tests.

Capacity-safe **business allocation**, resulting allocation events, and registration status reconciliation have not yet been implemented or tested. Neither the current SQL tests nor the RabbitMQ intake tests imply exactly-once seat allocation.

---

## RabbitMQ Outbox Integration Tests

The Registration integration-test suite includes automated Outbox Publisher tests using:

- xUnit v3.
- SQL Server Testcontainers.
- RabbitMQ Testcontainers.
- The actual `OutboxPublisherService`.
- Isolated SQL databases and RabbitMQ test brokers.
- RabbitMQ.Client for message publication verification.

### Test Scenarios

The current publisher integration tests verify:

1. Successful publishing with publisher confirmation and recorded SQL publication state.
2. Unavailable RabbitMQ broker and successful subsequent recovery.
3. Unroutable-message failure handling and isolation from other eligible Outbox messages.
4. Duplicate publication risk when broker acceptance and SQL publication state diverge.
5. Persistent exponential backoff and deferred retry scheduling.
6. Quarantine of invalid known event payloads without blocking a valid Outbox message.

### Run RabbitMQ Tests

Start Docker Desktop. From the repository root:

```powershell
dotnet test tests/ConferenceProFlow.Registration.IntegrationTests/ConferenceProFlow.Registration.IntegrationTests.csproj --configuration Release --filter "FullyQualifiedName~OutboxPublisherIntegrationTests"
```

Testcontainers starts and removes the test infrastructure. This test suite does not use the development RabbitMQ broker or local development databases.

### Reliability Guarantees and Limitations

The Registration Outbox publisher provides **at-least-once publication behavior** with persistent retry state. Publisher confirmations establish broker acceptance, not consumer receipt or completed business processing.

The Allocation SQL Inbox provides durably recorded, deduplicated intake based on `MessageId` and `RegistrationId`. It does **not** provide exactly-once capacity allocation or Registration status updates.

The Registration publisher currently assumes one active instance. The tests cover selected publishing, failure, duplicate, and recovery scenarios, but not every network partition or cancellation timing.

See `docs/MESSAGING_DESIGN.md` for complete topology, failure semantics, and remaining design limitations.
