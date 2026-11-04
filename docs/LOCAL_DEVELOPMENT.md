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

The Registration API currently runs directly through Visual Studio.

Its existing health endpoint is available at `/health`.

Database persistence will be configured in the next development phase.

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

The current implementation focuses on persistence and database integrity. Registration HTTP workflows, asynchronous allocation, and Transactional Outbox processing will be introduced in subsequent milestones.