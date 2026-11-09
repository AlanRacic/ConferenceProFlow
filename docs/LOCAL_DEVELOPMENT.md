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

It currently runs directly through Visual Studio, while SQL Server runs locally through Docker Compose.

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

Replace placeholder identifiers with the GUID values returned by the corresponding creation requests.

A successful registration creation request returns HTTP `201 Created` with an initial status of `Pending`.

A Pending registration does not represent a confirmed seat allocation.

### Automated HTTP Integration Tests

Automated HTTP integration tests are implemented using ASP.NET Core WebApplicationFactory and xUnit v3.

The tests use Testcontainers to create an isolated SQL Server environment and apply the application's EF Core migrations.

They validate HTTP responses, input validation, database persistence, duplicate handling, and registration status.

Docker Desktop must be running before executing the integration tests.

Use the automated test commands documented in the database testing section of this guide.

### Current Development Limitations

The initial Registration API is intended for local development and testing.

Authentication and authorization are not yet implemented.

Registration requests can currently be created for unpublished sessions to support development before session publication and allocation provisioning are available.

Registration requests remain Pending because asynchronous capacity allocation is not yet implemented.

The API is not ready for public production deployment.

Future development will introduce secure access, session publication rules, reliable messaging, and the Allocation service.

## Development Principles

The local environment is designed to be reproducible without paid cloud services.

Infrastructure will be extended incrementally as RabbitMQ, Kafka, observability components, and additional services are introduced.

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
3. Add the following configuration, replacing the placeholder with the password configured in the local `.env` file:

```json
{
  "ConnectionStrings": {
    "RegistrationDb": "Server=127.0.0.1,14333;Database=ConferenceProFlowRegistration;User Id=sa;Password=YOUR_LOCAL_PASSWORD;Encrypt=True;TrustServerCertificate=True;"
  }
}
```

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

The initial migration creates the following business tables:

- `Conferences`
- `ConferenceSessions`
- `Participants`
- `Registrations`

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

The result should include the four business tables and the EF Core migrations history table.

### 6. Run Automated Tests

ConferenceProFlow includes unit tests, HTTP integration tests, and SQL Server persistence integration tests.

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

The tests use a real SQL Server instance rather than the EF Core InMemory provider.

### 7. Continuous Integration

The GitHub Actions workflow is located at:

```text
.github/workflows/dotnet-ci.yml
```

The workflow restores dependencies, builds the solution, verifies Docker availability, and executes automated tests.

Database integration tests use Testcontainers on the GitHub-hosted Ubuntu runner.

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

The Registration API owns the `ConferenceProFlowRegistration` database.

The future Allocation service will have its own database and will be responsible for session capacity and seat allocation.

The Registration API must not directly modify the Allocation service's database.

This separation establishes clear data ownership and prepares the application for asynchronous communication through messaging infrastructure.

The Registration API now supports HTTP-based creation and retrieval of Pending registration requests. Asynchronous allocation, Transactional Outbox processing, and allocation-result handling will be introduced in subsequent development phases.

---

## RabbitMQ — Local Messaging Infrastructure

ConferenceProFlow uses RabbitMQ for asynchronous communication between the Registration API and the future Allocation Worker.

RabbitMQ runs locally through Docker Compose using the official RabbitMQ image with the Management plugin.

### Configuration

The root `.env.example` file includes RabbitMQ configuration variables.

Copy the example configuration when setting up the repository for the first time, then configure the following values in the local `.env` file:

```dotenv
RABBITMQ_USER=conferenceproflow
RABBITMQ_PASSWORD=YOUR_STRONG_LOCAL_PASSWORD
```

The local `.env` file must never be committed to Git.

### Start RabbitMQ

Ensure Docker Desktop is running.

From the repository root, execute:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d rabbitmq
```

Check the container status:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml ps
```

Verify the RabbitMQ application:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml exec rabbitmq rabbitmq-diagnostics -q check_running
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

Use the configured credentials to sign in to the Management UI.

### Persistence

RabbitMQ data is stored in the named Docker volume `rabbitmq_data`.

Normal container restarts preserve this volume.

Deleting the volume removes its persisted data, including RabbitMQ definitions and stored messages.

The initial credentials and virtual host are provisioned when RabbitMQ initializes an empty data directory. Changing the environment variables later does not automatically update existing users or passwords.

### Stop RabbitMQ

To stop only RabbitMQ:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml stop rabbitmq
```

To restart it:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml start rabbitmq
```

To stop and remove all services defined in Docker Compose while preserving named volumes:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml down
```

Do not use `down -v` unless intentionally deleting persistent development data.

### Current Scope

This phase introduces RabbitMQ infrastructure only.

Application publishers, exchanges, queues, consumers, Transactional Outbox processing, and allocation workflows will be implemented in subsequent development tasks.

The current Registration API continues to operate independently of RabbitMQ.

---

## Allocation Worker — Local Development

The Allocation Worker is a standalone .NET 10 Worker Service responsible for the future processing of session capacity and seat allocation requests.

The project is located at:

`src/ConferenceProFlow.Allocation.Worker`

### Current Implementation

The initial Worker Service includes:

- .NET Generic Host configuration.
- Dependency injection.
- Structured logging.
- A hosted BackgroundService.
- Cancellation-aware application shutdown.

The Worker currently provides application lifecycle functionality only.

RabbitMQ consumption, allocation persistence, and seat reservation processing are not yet implemented.

### Run the Allocation Worker

From the repository root, execute:

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj
```

Alternatively, open `ConferenceProFlow.slnx` in Visual Studio, select `ConferenceProFlow.Allocation.Worker` as the startup project, and press `F5`.

The Worker runs as a background application and does not expose HTTP endpoints.

### Stop the Worker

When running from a terminal, press `Ctrl + C` to request graceful shutdown.

When running through Visual Studio debugging, stop the debugging session.

The BackgroundService handles cancellation and terminates without treating normal shutdown as an application failure.

### Infrastructure Dependencies

The initial Worker Service does not require SQL Server or RabbitMQ to be running.

These dependencies will be introduced incrementally as the Allocation domain, persistence infrastructure, and messaging consumers are implemented.

### Service Ownership

The Allocation Worker will own a separate Allocation database.

It must not directly access or modify the Registration service's database.

Future communication between the two services will use asynchronous messaging.

### Next Development Steps

Upcoming implementation tasks include:

- Allocation domain entities.
- Independent SQL Server persistence.
- EF Core migrations.
- Allocation unit and integration tests.
- RabbitMQ consumers.
- Concurrency-safe seat allocation.
- Idempotent message processing.