# ConferenceProFlow — M2.6 End-to-End Allocation and Reconciliation Demo

This guide reproduces the **complete local registration-to-allocation-to-registration round trip**, using .NET 10, SQL Server, RabbitMQ, Docker Compose, and Visual Studio. It is a **development-only walkthrough**; no paid cloud services are required.

```text
Registration API
  -> RegistrationDb (Pending Registration + Registration Outbox)
  -> RabbitMQ: registration.requested.v1
  -> Allocation Worker (durable request Inbox -> capacity-safe decision)
  -> AllocationDb (Allocation + result Outbox)
  -> RabbitMQ: allocation.decided.v1
  -> Registration API (result consumer + reconciliation)
  -> RegistrationDb (Confirmed / Rejected Registration + result Inbox)
```

In **M2.5**, only the Allocation decision became final; Registration remained `Pending`. In **M2.6**, the allocation-result event travels back and Registration is reconciled asynchronously. A successful `201 Created` registration response **does not itself guarantee a seat**.

## 1. Prerequisites

- .NET 10 SDK, Visual Studio with `.http` file support, Docker Desktop running Linux containers, and the project checked out locally.
- Docker Compose file `infrastructure/compose.yaml` and a local `.env` based on `.env.example`.
- Both existing databases, `ConferenceProFlowRegistration` and `ConferenceProFlowAllocation`, with their respective **EF Core migrations applied**.
- Valid database connection strings and RabbitMQ settings in each application's **User Secrets**. Keep secrets out of Git, screenshots, and documentation.
- The five hosted services enabled in local development:

| Application | User Secrets key | Purpose |
|---|---|---|
| Registration API | `OutboxPublisher:Enabled` | Publishes registration-request events |
| Registration API | `AllocationResultConsumer:Enabled` | Receives decisions and updates Registration |
| Allocation Worker | `RabbitMqConsumer:Enabled` | Receives registration requests |
| Allocation Worker | `AllocationInboxProcessor:Enabled` | Makes capacity-safe decisions |
| Allocation Worker | `AllocationOutboxPublisher:Enabled` | Publishes allocation-result events |

Set each key to `true` for this demo. Preserve the rest of the existing User Secrets; do **not** overwrite existing connection strings or credentials. See [`LOCAL_DEVELOPMENT.md`](LOCAL_DEVELOPMENT.md) for configuration and migration instructions.

## 2. Start the local infrastructure

From the repository root, for example `C:\work\ConferenceProFlow`, run in **Visual Studio → View → Terminal**:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d
```

Check that the containers are running:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml ps
```

The documented local endpoints are SQL Server `127.0.0.1,14333`, RabbitMQ AMQP `127.0.0.1:5672`, and RabbitMQ Management `http://127.0.0.1:15672` (virtual host `conferenceproflow`). Do not proceed if the infrastructure is unavailable.

## 3. Start Registration API and Allocation Worker

Keep both processes running in separate Visual Studio terminals.

**Terminal 1 — Registration API:**

```powershell
dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj --launch-profile https
```

Expected development API URL: `https://localhost:7217`.

**Terminal 2 — Allocation Worker:**

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj --launch-profile ConferenceProFlow.Allocation.Worker
```

Check the application logs for both consumers and the relevant publishers/processors. **Start/confirm the Registration result consumer before producing allocation decisions**: it owns the durable result queue and binding needed for the Allocation publisher's mandatory routing. Do not start multiple Worker instances for this single-instance demo.

## 4. Create a fresh conference and session

In Visual Studio, open **[`ALLOCATION_E2E.http`](ALLOCATION_E2E.http)** and execute only:

1. **Request 1 — Create a dedicated conference** (`demoCreateConference`).
2. **Request 2 — Create a new session** (`demoCreateSession`).

Each successful POST should return `201 Created`. Visual Studio named-request expressions automatically feed the conference ID into request 2 and later feed the session ID into registration requests.

**Stop after request 2.** Copy the new session's response `id` **once**, for the SQL inventory seed below. Do not start creating registrations before provisioning inventory.

Every complete demo run should create a **new session**. Participant email addresses in the updated `.http` file use `{{$guid}}` and are generated uniquely; no manual participant or registration GUID entry is needed.

> `.http` troubleshooting: if request 2 returns `400` and the error says `'#' is invalid after a single JSON value`, ensure comments are separated from the preceding JSON body by a new `###` request delimiter. Use the corrected `ALLOCATION_E2E.http` file rather than appending instructional comments immediately after JSON without a delimiter.

## 5. Seed capacity using Visual Studio SQL Server Object Explorer

The Registration service owns session details; **automatic Allocation inventory provisioning is not yet implemented**. We deliberately create the corresponding Allocation-side `SessionInventory` for this local demonstration.

1. Open **View → SQL Server Object Explorer**.
2. Connect to your local SQL Server at `127.0.0.1,14333` using your existing development credentials.
3. Expand **Databases → ConferenceProFlowAllocation**, right-click the database, and select **New Query**.
4. Open [`ALLOCATION_E2E_SEED.sql`](ALLOCATION_E2E_SEED.sql), paste its entire contents into that query editor, and replace only `PASTE-SESSION-GUID-HERE` in the declaration of `@SessionIdText` with the session ID from HTTP request 2.
5. Verify the active connection is the **local Allocation database**, then execute the SQL query.

The guarded seed creates a row in `SessionInventories`. Expected initial output:

| Field | Expected |
|---|---|
| `SessionId` | New session ID from request 2 |
| `Capacity` | `2` |
| `ReservedCount` | `0` |
| `RowVersion` | Generated by SQL Server |

The script rejects invalid IDs and sessions that already have an inventory row. It **must not be run against production databases**; it is not an API for production inventory management.

## 6. Submit three registrations

Return to `docs/ALLOCATION_E2E.http` and execute **requests 3–8 in order**:

| Request | Action |
|---|---|
| 3 | Create Nora Ellis |
| 4 | Register Nora for the new session |
| 5 | Create Ethan Brooks |
| 6 | Register Ethan for the same session |
| 7 | Create Maya Chen |
| 8 | Register Maya for the same session |

Expect `201 Created` for the successful POST requests. The registration response initially reports `Pending`. There is **no need to paste participant, registration, or session GUIDs into these requests**: named response variables supply them automatically.

The Registration Outbox publisher, Allocation consumer/Inbox processor, Allocation result Outbox publisher, and Registration result consumer run asynchronously. Allow a little time for each part of the pipeline to complete.

## 7. Check final Registration statuses through HTTP

Execute **GET requests 9–11** in the same `.http` file. Each automatically uses the corresponding created registration ID.

**Expected aggregate outcome for a fresh session:**

| Final Registration status | Expected count |
|---|---:|
| `Confirmed` | `2` |
| `Rejected` | `1` |
| `Pending` | `0` |

The rejected registration should include `"rejectionReason": "Session capacity reached."`. Which participant is rejected is **not guaranteed**: message processing is asynchronous, and the demonstration does not promise globally ordered delivery.

If a status still reads `Pending`, repeat its **GET** after processing completes; **do not create a replacement registration or manually update its status**. If it stays pending, inspect logs and the queues/Outbox states described below.

## 8. Verify both databases in SQL Server Object Explorer

This is a **read-only development diagnostic**. Although it reads both databases in one SQL window, the running applications still own and write only their **separate** databases.

Right-click `ConferenceProFlowAllocation` → **New Query** and run the following SQL. **Replace the one session GUID placeholder** with the ID returned by request 2. The SQL uses fully qualified table names and does not modify any rows.

```sql
DECLARE @SessionId uniqueidentifier = 'PASTE-SESSION-GUID-HERE';

-- (A) Capacity and actual reservations: expect 2 / 2.
SELECT SessionId, Capacity, ReservedCount
FROM [ConferenceProFlowAllocation].[dbo].[SessionInventories]
WHERE SessionId = @SessionId;

-- (B) The three Registration outcomes, the three Allocation decisions,
-- and the two durable Inbox records (one per direction).
SELECT
    (SELECT COUNT(*) FROM [ConferenceProFlowRegistration].[dbo].[Registrations] WHERE SessionId = @SessionId) AS RegistrationTotal,
    (SELECT COUNT(*) FROM [ConferenceProFlowRegistration].[dbo].[Registrations] WHERE SessionId = @SessionId AND Status = 0) AS RegistrationPending,
    (SELECT COUNT(*) FROM [ConferenceProFlowRegistration].[dbo].[Registrations] WHERE SessionId = @SessionId AND Status = 1) AS RegistrationConfirmed,
    (SELECT COUNT(*) FROM [ConferenceProFlowRegistration].[dbo].[Registrations] WHERE SessionId = @SessionId AND Status = 2) AS RegistrationRejected,
    (SELECT COUNT(*) FROM [ConferenceProFlowAllocation].[dbo].[Allocations] WHERE SessionId = @SessionId) AS AllocationTotal,
    (SELECT COUNT(*) FROM [ConferenceProFlowAllocation].[dbo].[Allocations] WHERE SessionId = @SessionId AND Status = 1) AS AllocationConfirmed,
    (SELECT COUNT(*) FROM [ConferenceProFlowAllocation].[dbo].[Allocations] WHERE SessionId = @SessionId AND Status = 2) AS AllocationRejected,
    (SELECT COUNT(*) FROM [ConferenceProFlowAllocation].[dbo].[InboxMessages] WHERE SessionId = @SessionId) AS AllocationInboxTotal,
    (SELECT COUNT(*) FROM [ConferenceProFlowAllocation].[dbo].[InboxMessages] WHERE SessionId = @SessionId AND ProcessedAtUtc IS NOT NULL) AS AllocationInboxProcessed,
    (SELECT COUNT(*) FROM [ConferenceProFlowRegistration].[dbo].[AllocationResultInboxMessages] WHERE SessionId = @SessionId) AS RegistrationResultInboxTotal;

-- (C) Verify that final status and rejection reason agree per Registration.
SELECT p.DisplayName,
       r.Id AS RegistrationId,
       CASE r.Status WHEN 0 THEN 'Pending' WHEN 1 THEN 'Confirmed' WHEN 2 THEN 'Rejected' ELSE 'Unknown' END AS RegistrationStatus,
       CASE a.Status WHEN 1 THEN 'Confirmed' WHEN 2 THEN 'Rejected' ELSE 'Missing' END AS AllocationStatus,
       ri.Status AS ResultInboxStatus,
       r.RejectionReason AS RegistrationRejectionReason,
       a.RejectionReason AS AllocationRejectionReason,
       CASE WHEN a.Id IS NOT NULL AND ri.Id IS NOT NULL AND a.Status = r.Status THEN 'OK' ELSE 'CHECK' END AS ReconciliationCheck
FROM [ConferenceProFlowRegistration].[dbo].[Registrations] AS r
JOIN [ConferenceProFlowRegistration].[dbo].[Participants] AS p ON p.Id = r.ParticipantId
LEFT JOIN [ConferenceProFlowAllocation].[dbo].[Allocations] AS a ON a.RegistrationId = r.Id
LEFT JOIN [ConferenceProFlowRegistration].[dbo].[AllocationResultInboxMessages] AS ri ON ri.RegistrationId = r.Id
WHERE r.SessionId = @SessionId
ORDER BY r.CreatedAtUtc, r.Id;

-- (D) Request Outbox publication: three published registration requests.
SELECT o.Id AS MessageId, o.MessageType, o.PublishedAtUtc, o.PublishAttempts, o.LastError
FROM [ConferenceProFlowRegistration].[dbo].[OutboxMessages] AS o
WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(o.Payload, '$.sessionId')) = @SessionId
ORDER BY o.OccurredAtUtc, o.Id;

-- (E) Result Outbox publication: three published allocation decisions.
SELECT o.Id AS MessageId, o.AllocationId, o.MessageType, o.PublishedAtUtc, o.PublishAttempts, o.LastError
FROM [ConferenceProFlowAllocation].[dbo].[AllocationOutboxMessages] AS o
WHERE o.AllocationId IN (SELECT a.Id FROM [ConferenceProFlowAllocation].[dbo].[Allocations] AS a WHERE a.SessionId = @SessionId)
ORDER BY o.OccurredAtUtc, o.Id;
```

Expected after the three-registration demo:

| Check | Expected |
|---|---:|
| Inventory `Capacity` / `ReservedCount` | `2` / `2` |
| Registrations / Allocations | `3` / `3` |
| Registration `Confirmed` / `Rejected` / `Pending` | `2` / `1` / `0` |
| Allocation `Confirmed` / `Rejected` | `2` / `1` |
| Allocation request-Inbox rows / processed | `3` / `3` |
| Registration result-Inbox rows | `3` |
| Reconciliation rows where `ReconciliationCheck = 'OK'` | `3` |
| Registration request-Outbox / Allocation result-Outbox | `3` published / `3` published |

A published Outbox message generally has a non-`NULL` `PublishedAtUtc`. If records are still pending, check background service activity and retry timing before drawing conclusions. For the rejected Registration, the two databases should agree on the reason `Session capacity reached.`.

## 9. Optional reliability demonstration — stop and restart the Allocation Worker

**Use this only after the baseline demo has completed.** Keep Registration API, SQL Server and RabbitMQ running throughout.

1. Stop **only** the Allocation Worker with `Ctrl+C` in its terminal.
2. In Visual Studio, use the separately prepared local recovery `.http` requests to create a **new participant and new registration** for an already provisioned, full session. Keep the same session ID for these requests; named HTTP request variables still avoid manual participant and registration IDs.
3. GET the registration while the Worker is stopped. Expect `Pending`.
4. Restart the Worker with the **same single-line** terminal command from section 3.
5. Repeat the GET after processing. For a session whose two places are already reserved, expect `Rejected` with `Session capacity reached.` — without resubmitting the registration.

This check illustrates that a previously accepted request can be processed after a Worker interruption. It does **not** by itself prove recovery from every broker/database outage or exactly-once delivery. Other failure and redelivery paths are covered by selected automated Testcontainers tests.

**Important:** This recovery scenario creates an **additional** registration. If you reuse the baseline session (originally 2 Confirmed / 1 Rejected), its totals will then become **2 Confirmed / 2 Rejected**. The summary in section 8 applies to the **fresh three-registration baseline before this additional test**. For a clean repeat, create a new session; never delete durable records to force matching totals.

Do not commit a run-specific recovery file containing your actual session GUID. Keep the reusable project documentation generic.

## 10. Troubleshooting

| Symptom | Likely checks |
|---|---|
| `400 Bad Request` on `.http` session creation | Check request JSON and `###` boundaries; comments must not leak into the JSON body |
| Registration API does not start | SQL connection string, migrations, Docker SQL availability, launch profile, User Secrets |
| Allocation Worker does not start | Allocation SQL connection, migrations, RabbitMQ credentials/virtual host, enabled hosted services |
| Registration request returns `409` | Previously used participant email or duplicate participant/session combination; use a fresh session and generated email |
| Registration Outbox has no `PublishedAtUtc` | `OutboxPublisher:Enabled`, RabbitMQ connectivity, publisher logs, `LastError`/`NextAttemptAtUtc` |
| Request published but Allocation Inbox is absent | RabbitMQ request consumer, request queue/binding, failed-request parking queue, Worker logs |
| Allocation Inbox remains unprocessed | `AllocationInboxProcessor:Enabled` and matching `SessionInventory`; check processing logs/cooldown |
| Allocation exists but Registration remains `Pending` | `AllocationOutboxPublisher:Enabled`, result Outbox publication, `AllocationResultConsumer:Enabled`, result queue/binding and consumer logs |
| Conflict / invalid result message | Inspect `registration.failed-allocation-results.v1` and application logs; do not casually replay or delete records |
| Incorrect totals | Ensure SQL is filtered to the correct fresh session; an extra recovery registration changes counts |

RabbitMQ topology: exchange `conferenceproflow.events`, request queue `allocation.registration-requests.v1`, result queue `registration.allocation-results.v1`. Parking queues: `allocation.failed-registration-requests.v1` and `registration.failed-allocation-results.v1`. See [`MESSAGING_DESIGN.md`](MESSAGING_DESIGN.md) for ACK, publisher confirmation, retry, and idempotency details.

## 11. Automated regression and final validation

From the repository root, with Docker Desktop running for Testcontainers integration tests:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

Automated suites cover domain allocation/concurrency, SQL persistence and reconciliation, RabbitMQ consumers, and selected retry/redelivery/parking behavior. **The manually executed two-service walkthrough is not itself an automated full-system E2E test.** Report what actually passed; do not infer universal reliability guarantees from one successful run.

## 12. Architecture boundaries

- Each service owns its own database. Cross-database queries above are **developer-only diagnostics**, not application architecture.
- Transactional Outbox protects each service's local business-data-plus-event commit; durable Inbox and idempotency protect the receiving side against repeated deliveries, under the implemented constraints.
- The system provides **at-least-once** messaging behavior, not globally exactly-once processing, and eventual rather than immediate cross-service consistency.
- `SessionInventory` is still provisioned manually for the local demo; production-grade automatic provisioning is outside this milestone.
- Authentication/authorization, comprehensive monitoring/alerting, audited replay, performance qualification and multi-replica publisher/processor coordination remain future work.
- This is a **production-aware portfolio implementation**, not an assertion of full production readiness.