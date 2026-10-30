# ConferenceProFlow — Architecture

**Architecture Design — Version 0.1**

**Status:** Initial Design

## 1. Architectural Overview

ConferenceProFlow is designed as an enterprise-style distributed backend system for conference registration, session capacity allocation, and event-driven analytics.

The architecture emphasizes clear service boundaries, reliable asynchronous communication, data consistency, observability, and scalability.

The system will be developed incrementally, beginning with the core registration functionality.

## 2. Core Components

### Registration API

The Registration API is an ASP.NET Core application responsible for receiving registration requests and managing registration records.

Its responsibilities include:

* Accepting registration requests.
* Maintaining registration identifiers and statuses.
* Persisting outgoing business messages using the Transactional Outbox Pattern.
* Processing allocation results.
* Providing registration status through HTTP endpoints.

Registration statuses will initially include Pending, Confirmed, and Rejected.

### Allocation Worker

The Allocation Worker is an independently deployed .NET background service responsible for managing session capacity.

Its responsibilities include:

* Consuming registration requests.
* Enforcing session capacity constraints.
* Preventing duplicate allocation operations.
* Publishing allocation results.
* Maintaining allocation-related data.

Capacity allocation must remain correct under concurrent processing.

## 3. Data Ownership

Each service will own its application data.

The Registration API will use a Registration database.

The Allocation Worker will use an Allocation database.

Both databases may run within the same local SQL Server instance, while maintaining separate logical ownership.

Services must not directly modify each other's databases.

## 4. Communication

The initial distributed architecture will use RabbitMQ for asynchronous communication.

The Registration API will publish registration requests.

The Allocation Worker will process these requests and publish allocation results.

The Registration API will consume the results and update registration statuses.

Reliable message publication will use the Transactional Outbox Pattern.

Consumers will implement idempotent processing to handle duplicate message deliveries.

## 5. Business Consistency

The system must prevent confirmed registrations from exceeding available session capacity.

Capacity allocation must use appropriate database concurrency controls and atomic operations.

Registration and allocation data will be updated through separate local transactions.

The system will use eventual consistency between services.

## 6. Planned Extensions

Later development phases will introduce Apache Kafka for event streaming and analytics, OpenTelemetry for distributed tracing, performance testing with k6, and Kubernetes for local container orchestration.

Additional technologies will be introduced only when their use is justified by a business or architectural requirement.

## 7. Deployment Strategy

Development will initially use Visual Studio and local SQL Server infrastructure.

Docker Compose will provide a reproducible local environment.

The completed application must support local deployment without requiring paid cloud services.

Azure deployment will remain optional.

## 8. Architecture Evolution

Architectural decisions will be reviewed after each major development phase.

Significant design decisions and changes will be documented using Architecture Decision Records.

The architecture will evolve according to verified functional requirements, reliability needs, and performance measurements.