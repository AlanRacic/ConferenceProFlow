# ConferenceProFlow — M3.5D: Controlled Kafka Replay & Projection Comparison

**Status:** manual local end-to-end verification (not M3.5E automated infrastructure tests).  
**Branch:** `feature/kafka-analytics-replay`.  
**Safety:** never delete/truncate live databases or reset `conferenceproflow.analytics.v1` offsets. This demonstration reuses existing M3.4 application code, M3.5B isolated SQL schema and M3.5C bounded replay runner.

## 1. Purpose and invariants

Reconstruct the analytics projection for **one freshly created capacity-2 session** from the already-retained Kafka log into `ConferenceProFlowAnalyticsReplay`. Compare results with the original `ConferenceProFlowAnalytics` without writing to it. The replay runner reads *all retained records* in the captured topic snapshot (not just the new session) but assertions focus on the new session. Historical smoke/invalid records can be quarantined independently. Kafka retention is seven days in local configuration; a completed snapshot only guarantees reconstruction of **retained** events.

Use the SAME existing SQL Server with these separate databases:

| Role | SQL database | Kafka consumer group |
| --- | --- | --- |
| Live analytics | `ConferenceProFlowAnalytics` | `conferenceproflow.analytics.v1` |
| Isolated replay | `ConferenceProFlowAnalyticsReplay` | `conferenceproflow.analytics.replay.demo01` (fresh/unused) |

Do not change the Kafka topic, SQL migrations, registration finalization event model, or any M3.4 processing implementation.

## 2. Prerequisites

- M3.5C Release Build and Analytics tests have passed.
- `ConferenceProFlowAnalyticsReplay` was created in M3.5B using the **existing** Analytics EF Core migration and still has **zero** rows in `ProcessedAnalyticsEvents`, `SessionRegistrationStats` and `QuarantinedAnalyticsEvents`.
- Replay has **not** run yet. The example group `conferenceproflow.analytics.replay.demo01` has never committed any offsets and has no existing local replay snapshot manifest. If it has already been used, **do not** start a new run against a non-empty database. See section 8.
- Local Docker SQL Server, RabbitMQ, Kafka and the Kafka topics are available. Keep `.env`, passwords and User Secrets local and outside source control.
- The replay connection string refers to **the same SQL Server Data Source** as live but `Database=ConferenceProFlowAnalyticsReplay`; the live connection remains `Database=ConferenceProFlowAnalytics`.
- All three .NET applications from M3.4 are available, as are `docs/ALLOCATION_E2E.http`, `docs/ALLOCATION_E2E_SEED.sql`, and `docs/ANALYTICS_E2E_DEMO.md`.

## 3. Start live pipeline and produce a fresh 2/1 projection

All commands below run from `C:\work\ConferenceProFlow` in **Visual Studio 2026 Terminal** (PowerShell); one command per line.

Start Docker infrastructure:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka ps
```

Before starting the live Analytics Worker, check its existing **User Secrets**:

```json
"AnalyticsKafkaConsumer": {
  "Enabled": true,
  "ObserveOnly": false,
  "BootstrapServers": "127.0.0.1:9092",
  "Topic": "conferenceproflow.registration-finalized.v1",
  "GroupId": "conferenceproflow.analytics.v1"
},
"AnalyticsReplay": {
  "Enabled": false,
  "Resume": false,
  "MaxIdleSeconds": 60,
  "BootstrapServers": "127.0.0.1:9092",
  "Topic": "conferenceproflow.registration-finalized.v1",
  "GroupId": "conferenceproflow.analytics.replay.demo01"
}
```

These are **two sections inside** the existing secrets JSON object. Keep BOTH `ConnectionStrings:AnalyticsDb` and `ConnectionStrings:AnalyticsReplayDb` unchanged. In Registration API and Allocation Worker User Secrets, retain the working RabbitMQ/Kafka publisher and consumer settings used for M3.4F.

Start three processes in three separate terminals:

**Terminal 1 — Registration API:**

```powershell
dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj --launch-profile https
```

**Terminal 2 — Allocation Worker:**

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj --launch-profile ConferenceProFlow.Allocation.Worker
```

**Terminal 3 — live Analytics Worker:**

```powershell
dotnet run --project src/ConferenceProFlow.Analytics.Worker/ConferenceProFlow.Analytics.Worker.csproj
```

Create new test data using the **existing** `docs/ALLOCATION_E2E.http`:

1. Send requests **1 and 2** to create a NEW conference and session; copy the new SessionId GUID from request 2.
2. In **Visual Studio → View → SQL Server Object Explorer**, connect to the existing LOCAL `ConferenceProFlowAllocation` database. Edit and execute the guarded `docs/ALLOCATION_E2E_SEED.sql` with the NEW SessionId (capacity 2; reserved 0). Do this **before** registering participants.
3. Send HTTP requests **3–8** to create and register three distinct synthetic participants. Use requests **9–11** to confirm finalization: **2 Confirmed, 1 Rejected, 0 Pending**.
4. Wait until the three `AnalyticsOutboxMessages` for this session have `PublishedAtUtc` populated and the live Analytics SQL projection reaches `ConfirmedCount=2`, `RejectedCount=1` with **3** unique processed events.
5. Open `docs/KAFKA_REPLAY_COMPARISON.sql` in SQL Server Object Explorer → New Query and substitute `PASTE-FRESH-SESSION-GUID-HERE` with the new SessionId. Execute it **before** replay. The `MISSING_REPLAY` result is expected at this stage.

Do **not** start replay until the source events have been published and the live projection is caught up. Otherwise a comparison may capture an incomplete business flow.

## 4. Baseline live consumer offsets and isolate the demonstration

Record the **CURRENT-OFFSET** per partition for the live consumer group; this is read-only:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group conferenceproflow.analytics.v1
```

Copy the output to your notes; DO NOT use `--reset-offsets`. After the successful baseline SQL and offsets check, **stop all three .NET processes with Ctrl+C** (the Registration API, Allocation Worker, and *live* Analytics Worker). Leave Docker SQL Server, RabbitMQ and Kafka running. Stopping the apps avoids background writes during the captured snapshot and ensures the Analytics Worker project’s shared User Secrets can be switched safely.

Optional: rerun read-only SQL from section 3. Verify the replay database is still empty. This is a fresh run: do not delete any database to make it empty.

## 5. Enable isolated replay (same Worker project, mutually exclusive mode)

Open **ConferenceProFlow.Analytics.Worker → Manage User Secrets**. Modify **only** the two flags below (the other settings from section 3 stay unchanged):

```json
"AnalyticsKafkaConsumer": { "Enabled": false },
"AnalyticsReplay": { "Enabled": true, "Resume": false }
```

The JSON above illustrates **properties inside the existing sections**; **do not replace** the sections with these abbreviated examples or remove their other keys. The existing complete replay section must still specify:

- `BootstrapServers=127.0.0.1:9092`
- `Topic=conferenceproflow.registration-finalized.v1`
- `GroupId=conferenceproflow.analytics.replay.demo01` (a **previously unused** group)
- `Resume=false`, `MaxIdleSeconds=60`

The existing `AnalyticsDb` SQL connection must still point to the live database and the existing `AnalyticsReplayDb` SQL connection must still point to `ConferenceProFlowAnalyticsReplay`. `AnalyticsReplaySafety` verifies database names, SQL Data Source, consumer group prefix and mutual exclusion **before** the Replay Runner can process data.

## 6. Run one-shot bounded replay

With Docker still running and all three live .NET processes stopped, execute:

```powershell
dotnet run --project src/ConferenceProFlow.Analytics.Worker/ConferenceProFlow.Analytics.Worker.csproj
```

Expected behavior:

- Logs indicate a newly captured replay snapshot manifest under `%TEMP%\ConferenceProFlow\ReplaySnapshots\<group-hash>.json`.
- Logs list each Kafka partition with its earliest retained (`Low`), exclusive target (`HighExclusive`) and actual start (`NextOffset`).
- The replay uses its own consumer group; it reuses **M3.4E KafkaAnalyticsRecordHandler** and **M3.4D idempotent SQL processor** against **Replay SQL**.
- It may process historic events for other sessions and quarantine malformed Kafka smoke records. That is OK; all retained topic records belong to the snapshot.
- An explicit Kafka offset commit happens only after successful SQL processing or durable quarantine.
- The one-shot application **exits automatically** and logs `REPLAY COMPLETED` and one completed offset range **for every partition**.

**Do not assume the run passed because the process stopped.** Verify `REPLAY COMPLETED` and successful exit. If it throws/aborts, do not rerun with `Resume=false` or delete the manifest. See section 8.

Kafka partitions can have `Low > 0` due to retention. A completed replay covers **only records retained at snapshot time**, not expired historical records; a complete rebuild of older live analytics cannot be inferred from a fresh-session match.

## 7. Compare isolated replay with live SQL and check group independence

**In Visual Studio → SQL Server Object Explorer**, re-execute `docs/KAFKA_REPLAY_COMPARISON.sql` using the same SessionId. The SQL is strictly `SELECT`-only, with explicitly named databases.

Expected values for the **new session**:

| Check | Expected |
| --- | --- |
| Source Registration | 3 registrations: 2 confirmed, 1 rejected, 0 pending |
| Registration Analytics Outbox | 3 records, all `PublishedAtUtc` non-null |
| Live `SessionRegistrationStats` | 2 confirmed, 1 rejected |
| Replay `SessionRegistrationStats` | 2 confirmed, 1 rejected |
| `ProjectionComparison` | `MATCH` |
| Live `ProcessedAnalyticsEvents` | 3 records |
| Replay `ProcessedAnalyticsEvents` | 3 records |
| Per-event identity comparison | 3 rows, all `MATCH` |

`ProcessedAtUtc` and projection `UpdatedAtUtc` may differ across databases; **do not compare processing timestamps**. Business identities (`MessageId`, `RegistrationId`, `SessionId`, `Status`, `OccurredAtUtc`) and counts must match.

Inspect replay group offsets (read-only; the group now has its own progress):

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group conferenceproflow.analytics.replay.demo01
```

Check the live group again using the command from section 4. Since **all live application processes were stopped before replay**, its committed offsets should remain unchanged. The replay group’s committed offsets can be equal numerically, but it is a **separate group** and never resets or commits offsets for the live group.

The JSON manifest on disk should show `completed: true` and each partition's `nextOffset` equal to `highExclusive`. **The live group’s offsets and SQL rows are the baseline of what must not be changed by replay.** Global live/replay quarantine totals and historical session totals do not need to match due to differing retention and processing windows.

## 8. Failure handling: do not reset or delete data

- **If the replay fails after it started and saved its snapshot:** leave the replay SQL rows, group offsets and `%TEMP%` manifest intact. Inspect the error and restore Kafka/SQL. In the same `AnalyticsReplay` section change **only** `Resume=true`, keep the **same GroupId**, then rerun the identical `dotnet run` command. The runner resumes at **Kafka-committed** positions within the original bounds; the SQL ledger handles duplicate deliveries.
- **If the snapshot reports `Completed=true`:** do not resume. The demonstration already finished. A *new* replay requires a **different unused group AND a genuinely empty, isolated target**; the current single replay DB must **not** be wiped for this exercise.
- **If source events were not in the retained Kafka range:** report the replay as **insufficient history**, not a complete reconstruction. Kafka retention is a real limit.
- **If a replay group exists but its local manifest is missing:** do not manually reset group offsets to force a rerun. Investigate rather than guessing.
- **Never** run `docker compose down -v`, `TRUNCATE`, `DROP DATABASE`, `kafka-consumer-groups --reset-offsets`, or EF migrations against the live Analytics database for this demo.

This phase is a **manual E2E proof**. M3.5E will add disposable Kafka + SQL Server Testcontainers to automate replay, incomplete runs and recovery without touching local project data.

## 9. Restore normal Analytics Worker configuration

After verification, return in User Secrets:

- `AnalyticsReplay:Enabled=false` (and `Resume=false` for the next fresh demo).
- `AnalyticsKafkaConsumer:Enabled=false` while idle, or `true` with `ObserveOnly=false` to restart live analytics later.

Keep both connection strings, Kafka topic, replay snapshot manifest and consumer group offsets untouched. Reenable Registration API and Allocation Worker only if needed for subsequent demonstrations.

In **Git → Git Changes**, verify `feature/kafka-analytics-replay`. This stage adds **only two docs files**; no C# code, migrations or NuGet changes. Do not commit or merge yet — M3.5E automated infrastructure tests and M3.5F final validation/documentation remain.
