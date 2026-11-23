# ConferenceProFlow — M3.4F: Analytics Worker End-to-End Verification

This guide validates a **fresh, reproducible** business flow from HTTP registration through RabbitMQ allocation, Kafka finalization and **Analytics-owned SQL projections**. It also verifies consumer restart/catch-up and deliberate duplicate delivery. This is **local development only** and requires no paid cloud services. It is a verification runbook; the worker implementation is delivered in M3.4A–E.

## 1. Reliability contract and scope

```text
Registration API → Registration SQL Outbox → RabbitMQ
  → Allocation Worker → Allocation result SQL Outbox → RabbitMQ
  → Registration reconciliation + Analytics Outbox (same Registration SQL transaction)
  → Kafka Analytics Publisher → Kafka: registration-finalized.v1
  → Analytics Worker → Analytics SQL transaction
       ├── ProcessedAnalyticsEvents (idempotency ledger)
       ├── SessionRegistrationStats (projection)
       └── QuarantinedAnalyticsEvents (malformed/conflicting event evidence)
  → Kafka offset commit only after durable handling
```

The actual topic name is `conferenceproflow.registration-finalized.v1`.

- **Source of truth for business state:** `ConferenceProFlowRegistration`.
- **Analytics projections:** `ConferenceProFlowAnalytics`, owned exclusively by Analytics Worker.
- **Kafka transport:** at-least-once. Offsets are not committed atomically with SQL.
- **Expected behavior after a replay:** the original `MessageId` / business decision is recognized as already processed; aggregate counts remain unchanged.
- **Scope limitation:** locally validated single active Analytics Worker; this guide does not claim multi-instance production readiness.

## 2. Prerequisites and configuration

1. Work on `feature/kafka-analytics-worker` in `C:\work\ConferenceProFlow`. Keep the M3.4 GitHub Issue open until the final PR merges.
2. .NET 10 SDK, Visual Studio 2026, Docker Desktop and the project's `.env` are configured.
3. The three EF Core databases/migrations already exist: `ConferenceProFlowRegistration`, `ConferenceProFlowAllocation`, `ConferenceProFlowAnalytics`.
4. Local Registration/Allocation demonstration files are available: `docs/ALLOCATION_E2E.http`, `docs/ALLOCATION_E2E_SEED.sql`, `docs/ALLOCATION_E2E_DEMO.md`.
5. The existing business topic has been initialized using the M3.2 infrastructure instructions. No topic deletion or reset is required.
6. In **User Secrets** (keep all existing connection strings and credentials):

   | Application | Required key | Value for this demo |
   | --- | --- | --- |
   | Registration API | `OutboxPublisher:Enabled` | `true` |
   | Registration API | `AllocationResultConsumer:Enabled` | `true` |
   | Registration API | `KafkaAnalyticsPublisher:Enabled` | `true` |
   | Registration API | `Kafka:BootstrapServers` | `127.0.0.1:9092` |
   | Registration API | `Kafka:RegistrationFinalizedTopic` | `conferenceproflow.registration-finalized.v1` |
   | Allocation Worker | `RabbitMqConsumer:Enabled` | `true` |
   | Allocation Worker | `AllocationInboxProcessor:Enabled` | `true` |
   | Allocation Worker | `AllocationOutboxPublisher:Enabled` | `true` |
   | Analytics Worker | `ConnectionStrings:AnalyticsDb` | Existing **local** Analytics connection string |
   | Analytics Worker | `AnalyticsKafkaConsumer:Enabled` | `true` |
   | Analytics Worker | `AnalyticsKafkaConsumer:ObserveOnly` | `false` |
   | Analytics Worker | `AnalyticsKafkaConsumer:BootstrapServers` | `127.0.0.1:9092` |
   | Analytics Worker | `AnalyticsKafkaConsumer:Topic` | `conferenceproflow.registration-finalized.v1` |
   | Analytics Worker | `AnalyticsKafkaConsumer:GroupId` | `conferenceproflow.analytics.v1` |

   **Keep separate** the previous `conferenceproflow.analytics.preview.v1` group (no offset commits) and the durable `conferenceproflow.analytics.v1` group (explicit commits). The durable group's **existing** committed offsets persist across worker restarts.

   Do not copy User Secrets, `.env`, passwords or real participant data to Git.

## 3. Start services (three separate Visual Studio terminals)

From the repository root, start the existing infrastructure:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka ps
```

Keep the three .NET processes running, each in its own terminal. **Start Registration API first**, so the allocation result queue/binding exists before decisions are published; then Allocation Worker and Analytics Worker.

**Terminal 1 — Registration API:**

```powershell
dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj --launch-profile https
```

**Terminal 2 — Allocation Worker:**

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj --launch-profile ConferenceProFlow.Allocation.Worker
```

**Terminal 3 — Analytics Worker:**

```powershell
dotnet run --project src/ConferenceProFlow.Analytics.Worker/ConferenceProFlow.Analytics.Worker.csproj
```

Look for `M3.4E durable Analytics consumer starting` and later `disposition=Applied` and `offset committed`. Startup can first consume retained historical events if this group has no prior committed offsets; other sessions appearing in Analytics SQL is normal.

## 4. Fresh capacity-2 business flow: expected Analytics 2/1

1. In Visual Studio open **`docs/ALLOCATION_E2E.http`**. Execute **request 1** to create a new conference, then **request 2** to create a new session; copy the **new SessionId** from the second HTTP response. Use **a new session for every run**—do not reuse an old full session for the initial 2/1 assertion.
2. **Before any registrations:** In **View → SQL Server Object Explorer**, open `ConferenceProFlowAllocation` → **New Query**. Run the existing guarded `docs/ALLOCATION_E2E_SEED.sql` after replacing its single session-GUID placeholder. It provisions **capacity 2, reserved 0** for this new session. The seed is local demo infrastructure, not an API or production workflow.
3. Execute **requests 3–8** of `docs/ALLOCATION_E2E.http` in order. They create three distinct test participants and three registrations using response expressions and fresh test emails.
4. Execute **GET requests 9–11**. Repeat **the GET requests only** until all three final states are visible. Expected: **2 Confirmed, 1 Rejected, 0 Pending**; the identity of the rejected participant is not guaranteed.
5. In Registration SQL, wait until all three `AnalyticsOutboxMessages` for this SessionId have `PublishedAtUtc` filled. In Analytics SQL, wait until the projection and processed ledger catch up. Eventual consistency means HTTP/SQL/Kafka updates may become visible at different moments.
6. Run the bundled read-only `docs/ANALYTICS_E2E_CHECKS.sql` in **Visual Studio → SQL Server Object Explorer → New Query**. Replace `PASTE-FRESH-SESSION-GUID-HERE` once. Its queries explicitly address the separate databases for **manual human diagnostics only**—the applications do not have cross-service database dependencies.

**Checkpoint A** for this SessionId:

| Check | Expected |
| --- | ---: |
| Registration rows | 3 |
| Registration `Confirmed / Rejected / Pending` | `2 / 1 / 0` |
| Registration Analytics Outbox rows | 3, published |
| Analytics `ProcessedAnalyticsEvents` | 3 |
| Analytics `SessionRegistrationStats` | `ConfirmedCount=2`, `RejectedCount=1` |

Do not delete/rebuild the Kafka topic, consumer group or databases if an expectation fails. Inspect running processes, service settings, logs and Outbox state instead.

## 5. Verify Kafka group offsets before restart

In another Visual Studio PowerShell terminal:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group conferenceproflow.analytics.v1
```

Save or note the committed **CURRENT-OFFSET** for each of the topic's partitions (`0`, `1`, `2`). Offsets are **partition-local** and the committed offset represents the **next** position to read. This command inspects only; it does not reset the group.

## 6. Worker stop → new Kafka event → restart catch-up: expected 2/2

**Keep Registration API, Allocation Worker, Docker, RabbitMQ, Kafka and SQL running. Stop ONLY Analytics Worker** with `Ctrl+C`.

1. Copy the fresh SessionId into the `@sessionId` line of `docs/ANALYTICS_RESTART_DEMO.http`. Do not paste credentials or personal data.
2. Execute its **request 1** (fourth test participant), then **request 2** (fourth registration on the already full session). Execute **GET request 3** repeatedly until the status is **Rejected**.
3. While Analytics Worker is still stopped, rerun the SQL checks. Registration now has **4** finalization events, normally all already published by the active Kafka publisher. The **Analytics projection remains 2/1 with 3 processed rows**, because Analytics Worker was stopped. (If it has changed already, confirm no second Analytics Worker instance is running.)
4. Restart ONLY Analytics Worker using the same `dotnet run --project ...` command from section 3. **Do not change GroupId or reset offsets.**
5. Wait for `disposition=Applied` and an offset commit for the fourth record. Rerun the SQL checks.

**Checkpoint B** for this SessionId:

| Check | Expected |
| --- | ---: |
| Registration rows and published finalization events | 4 |
| Registration final states | `2 Confirmed / 2 Rejected / 0 Pending` |
| Analytics processed ledger rows | 4 |
| Analytics projection | `ConfirmedCount=2`, `RejectedCount=2` |
| Durable consumer group | Position advanced for the partition receiving the fourth record |

The **first checkpoint is 2/1; the final checkpoint after restart is 2/2**. The extra rejection is intentional evidence that the consumer catches up from a previously committed offset. No earlier event should be counted twice.

## 7. Deliberate duplicate Kafka publication (same MessageId, no extra count)

This is an **optional but valuable manual demonstration**; automated Kafka+SQL integration tests already check replay safety. Only perform it against local synthetic test data. The script publishes **one extra Kafka record** to the existing business topic and does **not** touch SQL or reset offsets.

1. Keep Analytics Worker running. In SQL Server Object Explorer open a **Registration** database query and execute the read-only query below, replacing the SessionId:

   ```sql
   DECLARE @SessionId uniqueidentifier = 'PASTE-FRESH-SESSION-GUID-HERE';
   SELECT TOP (1) Payload
   FROM dbo.AnalyticsOutboxMessages
   WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(Payload, '$.sessionId')) = @SessionId
     AND PublishedAtUtc IS NOT NULL
   ORDER BY OccurredAtUtc, Id;
   ```

2. Copy the **entire JSON payload** value, **unchanged** and on **one line**, to a temporary file **outside the repository**. For example, in PowerShell open Notepad:

   ```powershell
   notepad "$env:TEMP\ConferenceProFlow-duplicate.json"
   ```

   Paste the single JSON value, save UTF-8, and close Notepad. Do not regenerate `messageId` / `registrationId` / `occurredAtUtc` or reformat the JSON. The original payload should have fields `messageId`, `registrationId`, `sessionId`, `status`, `occurredAtUtc`.
3. **Inspect** the bundled `scripts/demo/Replay-AnalyticsFinalization.ps1` before running it. It validates that the JSON session ID matches the expected session and sends the payload with its original session key using Kafka's local CLI. From repository root:

   ```powershell
   powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\demo\Replay-AnalyticsFinalization.ps1 -PayloadFile "$env:TEMP\ConferenceProFlow-duplicate.json" -ExpectedSessionId "PASTE-FRESH-SESSION-GUID-HERE"
   ```

   Use an execution-policy override only after reviewing/trusting the script and where organizational policy permits. The script is for local demos only.
4. In Analytics Worker logs, expect `disposition=DuplicateMessage` and `offset committed` after the replay record is consumed. Inspect SQL and consumer offsets again.

**Checkpoint C:** `ProcessedAnalyticsEvents` still has **4 rows** for the session; `SessionRegistrationStats` remains **2/2**; the replay appends a Kafka record and advances the consumer group's committed position, without creating a new SQL business result.

If a replay is not seen, inspect the script's exit status, Kafka availability, worker logs and group positions. Do not reset committed offsets just to force replay.

## 8. Quarantine and SQL-failure verification

The **quarantine** results in `docs/ANALYTICS_E2E_CHECKS.sql` may include historical M3.2 smoke records, which must not influence session aggregates. Malformed/conflicting events are durably stored before their Kafka offsets are acknowledged. Exact quarantine row counts depend on prior retained events; **do not hard-code a quarantine count** for this demo.

**Do not manually stop SQL Server or corrupt a real local row** as part of this demonstration. M3.4E already contains automated disposable-container tests for:

- Valid, duplicate and malformed Kafka records in the same topic with one correct 2/1 projection.
- Explicit Kafka offset commits after durable SQL handling.
- A SQL failure that prevents commit and is then recovered without an unprocessed event being acknowledged.

These tests use disposable SQL Server and Kafka containers, not the project's local databases.

## 9. Full Release build and regression tests

Keep Docker Desktop running for Testcontainers. After the E2E checks succeed:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

All existing solution unit and integration tests should pass. The already completed Analytics M3.4B–E suite has **30 cases** if no additional tests were introduced; the full solution suite will be larger. Record actual totals rather than inventing a fixed full-suite test count. A build/test error must be resolved before opening the Pull Request.

## 10. Clean-up and follow-up

- Stop all three .NET apps with `Ctrl+C` as needed.
- In Analytics Worker User Secrets set `AnalyticsKafkaConsumer:Enabled=false` for idle development; the persisted Kafka consumer group offsets and SQL data are not lost. Re-enable when testing again.
- Keep the synthetic test sessions/records for reproducibility; do **not** run `docker compose down -v` or reset the durable group/offsets.
- Review `Git Changes` on `feature/kafka-analytics-worker`. This step adds documentation, a small HTTP demo, read-only SQL checks, and an optional local replay helper; it does **not** change C# application logic or EF Core migrations.
- **Do not merge yet.** After results are confirmed, finalize README/local-development/architecture documentation, stage/commit, push, run GitHub Actions and prepare the single M3.4 Pull Request.

## 11. What this demo does and does not prove

**Demonstrates:** an end-to-end Registration → RabbitMQ → Kafka → Analytics SQL pipeline; a fresh-session 2/1 projection; catch-up after stopping the Analytics Worker; persistence of consumer-group positions; deduplication of a deliberately replayed Kafka event; and support from automated failure/quarantine tests.

**Does not claim:** exactly-once Kafka+SQL atomic commits; high availability or validated multi-replica coordination; automatic quarantine remediation; disaster recovery; full performance/SLO testing; production security posture; or replay into an isolated new analytics database (future M3.5 scope).
