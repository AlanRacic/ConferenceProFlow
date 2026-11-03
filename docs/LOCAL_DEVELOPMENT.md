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