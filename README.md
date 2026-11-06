# ConferenceProFlow

[![.NET CI](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml/badge.svg?branch=master)](https://github.com/AlanRacic/ConferenceProFlow/actions/workflows/dotnet-ci.yml)

### Distributed Conference Operations Platform

ConferenceProFlow is an enterprise-style .NET backend project focused on designing, developing, and operating a distributed conference management system.

The platform is designed to support conference registrations, capacity allocation, reliable asynchronous processing, event-driven analytics, and operational monitoring.

The project emphasizes software architecture, distributed systems, data consistency, reliability, scalability, performance optimization, and professional software development practices.

## Technology Stack

The planned technology stack includes:

* **Backend:** C#, .NET 10, ASP.NET Core
* **Database:** SQL Server, Entity Framework Core
* **Messaging:** RabbitMQ, MassTransit
* **Event Streaming:** Apache Kafka
* **Data Consistency:** Transactional Outbox, Idempotent Consumers
* **Containerization:** Docker, Docker Compose
* **Orchestration:** Kubernetes
* **Observability:** OpenTelemetry, Prometheus, Grafana
* **Performance Testing:** k6
* **Automated Testing:** xUnit, Integration Tests, Testcontainers
* **CI/CD:** GitHub Actions
* **Cloud:** Microsoft Azure (optional)

Technologies will be introduced incrementally based on architectural and business requirements.

## Architecture

The system will follow a distributed, service-oriented architecture with clearly defined responsibilities.

The initial implementation will focus on conference registration and capacity management. Messaging, event processing, analytics, and orchestration capabilities will be introduced in subsequent development phases.

Architecture diagrams and technical decisions will be documented as development progresses.

## Development Approach

Development follows an iterative, feature-based workflow with an emphasis on:

* Clear functional and non-functional requirements
* Incremental implementation
* Automated testing and continuous integration
* Meaningful Git commits and Pull Requests
* Architecture Decision Records (ADRs)
* Regular code reviews and refactoring
* Performance measurement and optimization
* Reproducible local deployment

## Local Development

The completed system is intended to support local deployment using Docker Compose without requiring paid cloud services.

Detailed installation, configuration, and execution instructions will be added as the corresponding components are implemented.

## Project Documentation

- [Project Charter](docs/PROJECT_CHARTER.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Domain Model](docs/DOMAIN_MODEL.md)
- [Local Development Guide](docs/LOCAL_DEVELOPMENT.md)

Additional architecture documentation, ADRs, development guides, and performance reports will be added throughout the project.

## Project Status

**Phase 1 — Core Registration API**

The initial Registration API has been implemented and validated through automated testing.

### Implemented

- ASP.NET Core Minimal API using .NET 10.
- Domain entities for conferences, sessions, participants, and registrations.
- Registration lifecycle with Pending, Confirmed, and Rejected states.
- Entity Framework Core persistence using SQL Server 2022.
- Version-controlled database migrations.
- Database integrity through foreign keys, unique indexes, and constraints.
- Local SQL Server infrastructure using Docker Compose.
- REST endpoints for conference and session management.
- REST endpoints for participant creation and retrieval.
- REST endpoints for creating and retrieving registration requests.
- Duplicate participant and registration conflict handling.
- Unit tests for registration domain behavior.
- SQL Server persistence integration tests using Testcontainers.
- Automated HTTP integration tests using WebApplicationFactory.
- GitHub Actions continuous integration.

New registration requests are stored with **Pending** status. Seat allocation and automatic confirmation are not yet implemented.

### Current API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/conferences` | Create a conference |
| GET | `/api/conferences` | List conferences |
| GET | `/api/conferences/{id}` | Retrieve a conference |
| POST | `/api/conferences/{id}/sessions` | Create a conference session |
| GET | `/api/conferences/{id}/sessions` | List conference sessions |
| GET | `/api/conferences/{id}/sessions/{sessionId}` | Retrieve a session |
| POST | `/api/participants` | Create a participant |
| GET | `/api/participants/{id}` | Retrieve a participant |
| POST | `/api/registrations` | Create a Pending registration |
| GET | `/api/registrations/{id}` | Retrieve registration status |

Example HTTP requests are available in [`docs/REGISTRATION_API.http`](docs/REGISTRATION_API.http).

Local setup and testing instructions are available in the [Local Development Guide](docs/LOCAL_DEVELOPMENT.md).

### Planned Development

Upcoming development phases will introduce:

- Session publication and registration eligibility rules.
- Authentication, authorization, and API security.
- Registration request idempotency.
- RabbitMQ-based asynchronous messaging.
- Transactional Outbox and reliable message publishing.
- An independent Allocation Worker with its own database.
- Concurrency-safe capacity allocation.
- Idempotent message processing and registration status updates.
- Kafka-based event streaming and analytics.
- Distributed observability and performance testing.
- Local Kubernetes deployment.

The project is developed incrementally, with architecture and implementation decisions reviewed throughout development.

### Development Limitations

The current HTTP API is intended for local development and automated testing.

Authentication and authorization are not yet implemented. Registration requests can currently be created for unpublished sessions to support development before session publication and allocation provisioning are available.

The API must not be exposed as a public production service in its current state.

The completed system is intended to support reproducible local deployment without paid cloud dependencies.