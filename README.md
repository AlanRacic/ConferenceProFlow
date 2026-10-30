# ConferenceProFlow

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

* [Project Charter](docs/PROJECT_CHARTER.md)

Additional architecture documentation, ADRs, development guides, and performance reports will be added throughout the project.

## Project Status

**Phase 0 — Project Planning & Architecture**

The project is currently in its initial planning and architecture design stage.

Implementation will proceed incrementally, with each major development phase followed by technical review and validation.