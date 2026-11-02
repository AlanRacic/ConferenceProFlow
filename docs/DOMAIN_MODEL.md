# ConferenceProFlow — Domain Model

**Version:** 0.1
**Status:** Initial Design
**Related Issue:** Domain Model and Registration Lifecycle

## 1. Overview

This document defines the initial business entities, relationships, registration lifecycle, capacity allocation rules, and service ownership boundaries for ConferenceProFlow.

The initial business workflow focuses on individual conference session registrations.

A session registration represents a request to reserve one seat for one participant at one conference session.

## 2. Core Business Entities

### 2.1 Conference

Represents an organized conference.

**Properties:**

* Id — Unique identifier
* Name — Conference name
* StartsAtUtc — Conference start date and time
* EndsAtUtc — Conference end date and time

**Business Rules:**

* A conference may contain multiple sessions.
* The end date must be later than the start date.
* A conference identifier must be unique.

**Data Owner:** Registration Service

### 2.2 ConferenceSession

Represents an individual session belonging to a conference.

**Properties:**

* Id — Unique identifier
* ConferenceId — Parent conference identifier
* Title — Session title
* StartsAtUtc — Session start date and time
* EndsAtUtc — Session end date and time
* IsPublished — Indicates whether the session accepts registrations

**Business Rules:**

* Each session belongs to exactly one conference.
* A session must have a valid start and end time.
* A session cannot be registered for unless it is published and available for registration.
* Session scheduling and catalog information belong to the Registration Service.
* Operational capacity is managed separately by the Allocation Service.

**Data Owner:** Registration Service

### 2.3 Participant

Represents a person who can register for conference sessions.

**Properties:**

* Id — Unique identifier
* DisplayName — Participant name
* Email — Participant email address

**Business Rules:**

* A participant may register for multiple different sessions.
* A participant may have only one registration record for a particular session.
* Email addresses must follow the application's defined normalization and uniqueness rules.
* Initial development and demonstration environments will use synthetic participant data.

**Data Owner:** Registration Service

### 2.4 Registration

Represents a participant's request to reserve a place at a conference session.

**Properties:**

* Id — Unique identifier
* ParticipantId — Participant identifier
* SessionId — Conference session identifier
* Status — Current registration status
* CreatedAtUtc — Registration creation timestamp
* UpdatedAtUtc — Last status update timestamp
* RejectionReason — Optional reason for rejection

**Registration Statuses:**

* Pending
* Confirmed
* Rejected

**Business Rules:**

* A new valid registration begins with Pending status.
* A registration may transition from Pending to Confirmed.
* A registration may transition from Pending to Rejected.
* Confirmed and Rejected are terminal statuses in the initial implementation.
* A participant cannot create multiple registrations for the same session.
* A registration must reference an existing participant and an eligible session.
* Duplicate allocation results must not produce duplicate state changes.
* Conflicting or unexpected state transitions must be detected.

**Data Owner:** Registration Service

### 2.5 SessionInventory

Represents the operational seat inventory for a conference session.

**Properties:**

* SessionId — Identifier of the corresponding conference session
* Capacity — Maximum number of available seats
* ReservedCount — Number of successfully allocated seats

**Business Rules:**

* Capacity must be greater than zero.
* ReservedCount cannot be negative.
* ReservedCount must never exceed Capacity.
* Capacity allocation must be performed atomically.
* Each confirmed seat allocation must correspond to one unique registration.
* Duplicate messages must not allocate additional seats.

**Data Owner:** Allocation Service

## 3. Entity Relationships

The initial logical relationships are:

* One Conference has many ConferenceSessions.
* One ConferenceSession has many Registrations.
* One Participant has many Registrations.
* One ConferenceSession has one SessionInventory record.
* One Registration may have one successful seat allocation.

The relationship between ConferenceSession and SessionInventory crosses service boundaries.

It is represented through a shared business identifier rather than a cross-database foreign key.

The Allocation Service will maintain its own allocation records and message-processing records.

## 4. Registration Lifecycle

### 4.1 Registration Creation

A client submits a registration request through the Registration API.

The request identifies a participant and conference session and provides an idempotency key.

The API validates the request and verifies the required registration prerequisites.

If the request is valid, the Registration Service creates a registration with Pending status.

The registration and its outgoing message are persisted within the same local database transaction.

The API returns the registration identifier and current status.

The distributed implementation will use HTTP 202 Accepted for newly accepted asynchronous requests.

### 4.2 Allocation Processing

The Allocation Service receives a registration request message.

It verifies whether the message has already been successfully processed.

For a new request, it attempts to allocate one seat using an atomic database operation.

If capacity is available, the allocation succeeds.

If the session is full, the request is rejected.

The allocation decision, processing record, and outgoing result event are committed consistently within the Allocation database.

### 4.3 Registration Completion

The Registration Service receives the allocation result.

A successful allocation changes the registration status from Pending to Confirmed.

An unsuccessful allocation changes the status from Pending to Rejected.

Duplicate results must not create additional business state transitions.

Conflicting results must be detected and investigated rather than silently overwriting a completed registration.

## 5. Idempotency Rules

### API Requests

Clients will provide an Idempotency-Key for registration creation.

A repeated request using the same key and equivalent request data must reference the same logical operation.

Reusing the same key for a different request must result in a conflict.

Independent requests attempting to register the same participant for the same session must not create multiple registration records.

The database will enforce the relevant uniqueness constraints.

### Message Processing

Messages may be delivered more than once.

Consumers must prevent duplicated business effects.

The Allocation Service will use durable message-processing records and unique allocation identifiers.

Business changes and message-processing records must be committed within the same local database transaction.

## 6. Capacity Consistency

The Allocation Service is the authoritative owner of operational capacity.

Allocation must guarantee:

0 <= ReservedCount <= Capacity

Capacity changes must be atomic.

Concurrent requests must not cause overbooking.

The Registration Service must not directly modify Allocation database records.

For the initial local demonstration, session catalog records and matching inventory records may be provisioned using coordinated synthetic seed data.

A subsequent development task will define reliable event-based provisioning for dynamically created sessions.

## 7. Service Data Ownership

### Registration Database

Owns:

* Conferences
* ConferenceSessions
* Participants
* Registrations
* API idempotency records
* Outgoing message records
* Incoming message-processing records

### Allocation Database

Owns:

* SessionInventories
* SeatAllocations
* Incoming message-processing records
* Outgoing message records

Each service may access only its own database.

Both databases may run within the same SQL Server instance during local development.

## 8. Initial Messaging Contracts

The distributed workflow will initially require three business message types.

### RegistrationRequested

Indicates that a valid registration request has been accepted and requires seat allocation.

Includes:

* MessageId
* RegistrationId
* SessionId
* OccurredAtUtc
* ContractVersion

### AllocationConfirmed

Indicates that a seat was successfully allocated.

Includes:

* MessageId
* RegistrationId
* SessionId
* OccurredAtUtc
* ContractVersion

### AllocationRejected

Indicates that seat allocation was unsuccessful.

Includes:

* MessageId
* RegistrationId
* SessionId
* RejectionReason
* OccurredAtUtc
* ContractVersion

The final message serialization format, naming conventions, and compatibility strategy will be defined during the messaging implementation phase.

## 9. Initial Scope Limitations

The initial implementation does not include:

* Reservation cancellation
* Waiting lists
* Payment processing
* Participant authentication and authorization
* Ticket generation
* Seat selection
* Dynamic capacity changes
* Complex conference scheduling rules
* Production video streaming

These features may be considered in future development phases when justified by project requirements.

## 10. Architectural Principles

The system will follow these principles:

1. Each service owns its business data.
2. Cross-service communication must respect service boundaries.
3. Business constraints must be enforced at the appropriate authoritative data source.
4. Messaging must tolerate duplicate delivery.
5. Local database transactions must protect related business changes.
6. Eventual consistency must be explicitly handled.
7. New architectural complexity must be justified by concrete requirements.
8. Important decisions must be documented and reviewed.

## 11. Next Steps

After approval of the initial domain model, development will proceed with:

* Initial .NET solution and project scaffolding
* Entity implementation
* Entity Framework Core configuration
* Database constraints and migrations
* Core registration API
* Automated tests

RabbitMQ integration, Transactional Outbox, and distributed allocation processing will follow in subsequent phases.