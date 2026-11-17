# ConferenceProFlow — M2.5 End-to-End Allocation Demo

This guide demonstrates the real local request path:

`Registration API → Registration DB + Outbox → RabbitMQ → Allocation Consumer → Allocation SQL Inbox → Allocation Inbox Processor → Confirmed / Rejected Allocation`.

The workflow uses the existing .NET 10 applications and Docker Compose. No paid services are required. This is a **development-only demonstration**; inventory is provisioned with a deliberate SQL seed because cross-service inventory provisioning has not yet been implemented.

> **Important:** `Allocation.Status` is updated in the Allocation database, but `Registration.Status` remains `Pending` in the Registration database. Publishing allocation-result events and reconciling those Registration statuses are planned for a later milestone. Do not interpret a `201 Created` registration response as a confirmed seat.

## 1. Prerequisites and local configuration

- .NET 10 SDK, Docker Desktop with Linux containers, Visual Studio with HTTP-file support (or a compatible HTTP client), and a SQL Server client such as SSMS or Visual Studio SQL Server Object Explorer.
- Repo root: `C:\work\ConferenceProFlow` on the original developer machine; use your own checkout path elsewhere.
- A local root `.env` based on `.env.example`, containing SQL Server and RabbitMQ credentials. Never commit `.env` or User Secrets.
- Registration API User Secrets: `ConnectionStrings:RegistrationDb`, `RabbitMq:*`, `OutboxPublisher:Enabled=true`.
- Allocation Worker User Secrets: `ConnectionStrings:AllocationDb`, `RabbitMq:*`, `RabbitMqConsumer:Enabled=true`, `AllocationInboxProcessor:Enabled=true`.
- Existing Registration and Allocation EF Core migrations already applied to their **separate** databases.

If not configured already, refer to `docs/LOCAL_DEVELOPMENT.md`. Do not print credentials in demo screenshots or shared logs.

## 2. Start infrastructure

From the repository root in PowerShell:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d
```

Check:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml ps
```

The local endpoints are SQL Server `127.0.0.1,14333`, RabbitMQ AMQP `127.0.0.1:5672`, and RabbitMQ Management `http://127.0.0.1:15672` (vhost `conferenceproflow`).

## 3. Start both applications

Open **two** PowerShell terminals (or launch each project separately through Visual Studio).

Terminal A — Registration API:

```powershell
dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj --launch-profile https
```

Expected HTTPS endpoint: `https://localhost:7217`. The launch profile configures Development, allowing User Secrets to load. Ensure `OutboxPublisher:Enabled` is `true`.

Terminal B — Allocation Worker:

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj --launch-profile ConferenceProFlow.Allocation.Worker
```

The Worker's launch profile configures Development. Both `RabbitMqConsumer:Enabled` and `AllocationInboxProcessor:Enabled` must be `true`. Check startup logs for the RabbitMQ consumer **and** Allocation Inbox processor. No need to start a second Worker instance.

If HTTPS trust fails on a new machine, run `dotnet dev-certs https --trust` and retry.

## 4. Create the conference and session

Open `docs/ALLOCATION_E2E.http` in Visual Studio. Execute requests **1** and **2** in order:

1. `demoCreateConference` — creates a conference.
2. `demoCreateSession` — creates a new session and returns an `id` GUID.

**Copy the session `id` GUID** from request 2's response. Do **not** send the registration requests yet.

Create a fresh session for every new complete demo. If participants from a previous demo already exist, change the three example emails to new unique addresses; the API enforces participant-email uniqueness.

## 5. Provision capacity 2 for that session

Open `docs/ALLOCATION_E2E_SEED.sql` in SSMS or your SQL client, connected to the **local development** SQL Server at `127.0.0.1,14333`.

Replace `PASTE-SESSION-GUID-HERE` with the session ID from step 4 and execute the script. It explicitly switches to the Allocation database and inserts one `SessionInventories` row with:

- `Capacity = 2`
- `ReservedCount = 0`
- `SessionId =` the actual Registration session GUID
- `RowVersion` generated automatically by SQL Server

The script intentionally refuses to run without a valid GUID or when the inventory already exists. The Registration database is **not** modified directly by this script. This seed is only a temporary local-development provisioning step, not a production API or supported operating procedure.

## 6. Send three registration requests

Return to `docs/ALLOCATION_E2E.http`. Execute requests **3–8**, in order:

- Create Nora Ellis, then register Nora.
- Create Ethan Brooks, then register Ethan.
- Create Maya Chen, then register Maya.

All three registrations target the **same** session. The Registration API returns `201 Created` and an initial `Pending` status for each request. Publication and consumption are asynchronous; allow time for the Outbox publisher and Inbox processor polling cycles to run.

The test is about aggregate results, not which of the three participants is rejected if events are reordered. Once processing finishes, **two** Allocation results should be Confirmed and **one** Rejected, with no overbooking.

## 7. Inspect the Allocation database

Connect to `ConferenceProFlowAllocation` in a SQL client and replace the GUID placeholder with your demo session ID:

```sql
USE [ConferenceProFlowAllocation];
GO
DECLARE @SessionId uniqueidentifier = 'PASTE-SESSION-GUID-HERE';

SELECT SessionId, Capacity, ReservedCount
FROM dbo.SessionInventories
WHERE SessionId = @SessionId;

SELECT im.Id AS MessageId, im.RegistrationId, im.SessionId,
       im.ReceivedAtUtc, im.ProcessedAtUtc,
       a.Status AS AllocationStatus, a.RejectionReason
FROM dbo.InboxMessages AS im
LEFT JOIN dbo.Allocations AS a ON a.RegistrationId = im.RegistrationId
WHERE im.SessionId = @SessionId
ORDER BY im.ReceivedAtUtc, im.Id;

SELECT COUNT(*) AS TotalAllocations,
       SUM(CASE WHEN Status = 1 THEN 1 ELSE 0 END) AS Confirmed,
       SUM(CASE WHEN Status = 2 THEN 1 ELSE 0 END) AS Rejected
FROM dbo.Allocations
WHERE SessionId = @SessionId;
```

Expected results after all three are processed:

| Check | Expected |
|---|---:|
| Capacity | 2 |
| ReservedCount | 2 |
| Inbox rows for demo session | 3 |
| Inbox rows where `ProcessedAtUtc IS NOT NULL` | 3 |
| Confirmed allocations (`Status = 1`) | 2 |
| Rejected allocations (`Status = 2`) | 1 |
| Total allocations | 3 |

The rejected allocation should contain `RejectionReason = 'Session capacity reached.'`. There should be no duplicate `RegistrationId` in Allocation.

## 8. Verify Outbox publication and Registration status

On the Registration database, replace the same session GUID:

```sql
USE [ConferenceProFlowRegistration];
GO
DECLARE @SessionId uniqueidentifier = 'PASTE-SESSION-GUID-HERE';

SELECT Id, ParticipantId, SessionId, Status, CreatedAtUtc
FROM dbo.Registrations
WHERE SessionId = @SessionId
ORDER BY CreatedAtUtc, Id;

SELECT o.Id AS MessageId, o.MessageType, o.PublishedAtUtc,
       o.PublishAttempts, o.LastError,
       JSON_VALUE(o.Payload, '$.registrationId') AS RegistrationId
FROM dbo.OutboxMessages AS o
WHERE TRY_CONVERT(uniqueidentifier,
      JSON_VALUE(o.Payload, '$.sessionId')) = @SessionId
ORDER BY o.OccurredAtUtc, o.Id;
```

All three Registration rows should still have **Status = 0 (`Pending`)**. That is correct for M2.5, even though Allocation has already made Confirmed/Rejected decisions. The matching Outbox rows should generally have non-NULL `PublishedAtUtc` once the publisher finishes.

You may also execute requests 9–11 in `docs/ALLOCATION_E2E.http` to verify that the HTTP GET endpoints still report Pending.

## 9. Optional negative path: missing inventory

For a separate new demo session, **skip** the seed script and submit a registration. After its event is accepted, the Inbox row should exist but remain with `ProcessedAtUtc = NULL` and no Allocation row, because the processor does not invent a session capacity.

Then create a real SessionInventory using the seed script for that session. The running Inbox processor should detect the now-eligible pending message and process it on its next poll. This validates delayed provisioning **without re-publishing** the RabbitMQ event.

Use a separate new session rather than deleting or changing the inventory from the capacity-2 demonstration.

## 10. Troubleshooting

| Symptom | Check |
|---|---|
| API cannot start | Registration connection string, migrations, SQL container, Development environment |
| Worker cannot start | Allocation connection string, migrations, Developer User Secrets, `DOTNET_ENVIRONMENT` |
| Registration 409 | Participant email already exists, or duplicate participant/session registration; create fresh participants |
| Registration exists, Outbox remains unpublished | `OutboxPublisher:Enabled=true`, RabbitMQ connectivity/vhost/credentials, `LastError` and `NextAttemptAtUtc` |
| Outbox published, Inbox absent | RabbitMQ consumer enabled, queue/binding, worker logs and parking queue |
| Inbox exists but remains pending | Processor enabled; corresponding `SessionInventory` exists; worker logs; failure cooldown |
| Inbox processed, Allocation not visible | Query the correct Allocation database and same `RegistrationId`; inspect errors before claiming success |
| Allocation confirmed, Registration still Pending | **Expected in M2.5** until allocation-result events and status reconciliation are added |
| Unexpected totals | Check that the session is fresh and the seed created exactly two seats; filter SQL queries by the intended session GUID |

Do not delete or manually alter durable Inbox/Outbox/Allocation state to make the demo pass. Investigate the actual reason if the expected conditions are not met.

## 11. Automated regression checks

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

Existing automated suites cover consumer intake, SQL Inbox idempotency, domain capacity, optimistic concurrency, concurrent allocation, and the Inbox background processor. **This guide is a manually executable cross-service end-to-end check; it is not an additional automated full-system E2E test.**

## 12. Current architecture boundaries

- The two services retain independent SQL databases, connected through versioned RabbitMQ events.
- Allocation decisions and Inbox completion are committed atomically in the Allocation database.
- The local seed script fills a gap in inventory provisioning for demonstration purposes.
- M2.5 does not yet publish allocation-result events or update `Registration.Status` to Confirmed/Rejected.
- Authentication, authorization, session-publication rules, monitoring and administrative replay tooling remain outside the current implemented scope.
- Current Outbox and Inbox polling implementations are not presented as multi-replica production coordinators.
