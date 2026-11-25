# ConferenceProFlow — Analytics Worker and Isolated Replay (M3.4–M3.5)

**Status:** M3.4 durable Analytics and M3.5 bounded isolated replay implemented, tested and merged. M3.6E1 replay snapshot durability locally regression-tested; final M3.6 PR/CI pending.  
**Runtime:** .NET 10 Generic Host (no HTTP API or dashboard).  
**Ownership:** `ConferenceProFlowAnalytics` is owned by the Analytics Worker and is independent of Registration and Allocation databases.  
**Scope:** One active Analytics Worker and one durable Kafka consumer group; **not a validated multi-replica production deployment**.

## 1. Why Analytics is a separate service

Registration API and Allocation Worker exchange operational commands/results through RabbitMQ. After the authoritative Registration SQL transaction finalizes a Registration, it also persists `RegistrationFinalizedV1` into the **Registration-owned Analytics Outbox**. The independent Kafka publisher publishes that event. Analytics Worker consumes it and maintains a SQL projection of **finalized** registrations without querying Registration or Allocation databases.

```text
Registration API -> Registration SQL Outbox -> RabbitMQ -> Allocation Worker
           ^                               Allocation Result Outbox -> RabbitMQ
           |                                                       |
           +---- Registration reconciliation <---------------------+
                         |
                         +-- Registration status + Result Inbox + Analytics Outbox
                                         (one local SQL transaction)
                                                  |
                                    KafkaAnalyticsOutboxPublisherService
                                                  |
                            conferenceproflow.registration-finalized.v1
                                                  |
                              KafkaAnalyticsConsumerService (M3.4)
                                                  |
                       KafkaAnalyticsRecordHandler (validate/route)
                                       /                       \
                         valid/duplicate                    invalid/conflict
                              |                                     |
                AnalyticsEventProcessingService         AnalyticsQuarantineService
                              |                                     |
                      Analytics SQL transaction             Analytics SQL insert
                              |                                     |
                              +------------------+------------------+
                                                 |
                                        durable SQL result
                                                 |
                                      explicit Kafka Commit(record)
```

**Boundaries:** Analytics does not decide availability, change Registration statuses, synchronize inventory or write to any service-owned database other than its own. SQL and Kafka acknowledgements are **not** one distributed transaction. We target idempotent SQL effects with **at-least-once Kafka delivery**, not exactly-once transport.

## 2. Event contract and Kafka topology

| Property | Implemented value |
| --- | --- |
| Contract | `RegistrationFinalizedV1` (Registration publisher; Analytics has its own compatible consumer model `AnalyticsRegistrationFinalizedV1`) |
| Topic | `conferenceproflow.registration-finalized.v1` |
| Key | Canonical `SessionId` GUID string, format `D` |
| JSON | camelCase; `messageId`, `registrationId`, `sessionId`, `status`, `occurredAtUtc` |
| Allowed statuses | `confirmed`, `rejected` (lowercase) |
| Consumer group | `conferenceproflow.analytics.v1` (durable) |
| Preview group | `conferenceproflow.analytics.preview.v1` (observe only) |
| Broker | Apache Kafka 4.4.0 KRaft single-node Docker profile |
| Topic retention | Initially 7 days; this is **not** indefinite event storage |

A Kafka topic can contain historical development smoke records. Unknown or invalid records are not counted; when durable consumption is enabled they are quarantined by Kafka position if they are still retained and consumed.

## 3. SQL schema and idempotency

The existing migration `InitialAnalyticsPersistence` creates the following tables in `ConferenceProFlowAnalytics`:

| Table | Key and constraints | Role |
| --- | --- | --- |
| `ProcessedAnalyticsEvents` | `MessageId` primary key; unique `RegistrationId`; allowed `Status` check constraint | Durable ledger of successfully applied finalizations |
| `SessionRegistrationStats` | `SessionId` primary key; nonnegative confirmed/rejected counters | One aggregate row per session |
| `QuarantinedAnalyticsEvents` | Composite primary key `(Topic, Partition, Offset)`; bounded payload/key/reason storage | Evidence for malformed, key-mismatched and conflicting records |

For a new valid event, `AnalyticsEventProcessingService` starts a SQL transaction; acquires transaction-scoped `sp_getapplock` locks by message, registration and session; detects previously processed records; inserts the processed-event ledger; atomically increments (`ExecuteUpdateAsync`) or creates the session projection; and commits the SQL transaction. An exception rolls back ledger and projection together. Only then may the Kafka record be acknowledged.

| Processor outcome | SQL action | Consumer disposition |
| --- | --- | --- |
| `Applied` | Insert ledger + increment confirmed **or** rejected | `Applied`, then commit Kafka offset |
| `DuplicateMessage` | No extra count; existing message and business fields agree | `DuplicateMessage`, then commit Kafka offset |
| `DuplicateBusinessEvent` | Same registration and business fields, new message ID; no extra count | `DuplicateBusinessEvent`, then commit Kafka offset |
| `Conflict` | Original ledger/projection unchanged; Kafka position durably recorded in quarantine | `Quarantined`, then commit Kafka offset |
| Malformed/invalid payload or wrong session key | Insert bounded quarantine record; never change statistics | `Quarantined`, then commit Kafka offset |
| SQL processing or quarantine persistence failure | Throw; no successful durable disposition | **Do not commit** failed Kafka offset; restart/backoff |

The processor compares `OccurredAtUtc` as well as registration, session and status when recognizing duplicates. Conflicting values for one `MessageId` or `RegistrationId` are not silently collapsed into an accepted duplicate.

`QuarantinedAnalyticsEvents.Payload` holds a **truncated copy** of the untrusted incoming record (maximum 4,096 characters). Avoid exposing it publicly; do not paste raw quarantine payloads containing private data into Issues, screenshots or logs. The handler rejects incoming JSON payloads longer than 64 Ki characters before attempting full deserialization.

## 4. Kafka offset and failure semantics

The durable consumer is built with:

```text
EnableAutoCommit = false
EnableAutoOffsetStore = false
AutoOffsetReset = Earliest
AllowAutoCreateTopics = false
```

The consumer reads **one record at a time**, creates a fresh DI scope/`AnalyticsDbContext`, calls `KafkaAnalyticsRecordHandler.HandleAsync`, and only after a durable `Applied`, `DuplicateMessage`, `DuplicateBusinessEvent` or `Quarantined` result calls `consumer.Commit(record)`. This records the **next** position to consume for that topic-partition.

If **SQL fails** before completing the transaction or quarantine write, the handler throws; the consumer closes and rebuilds its Kafka client after a 3-second restart delay and reads again from the broker-committed offset. It does **not** commit the record that failed. If **SQL succeeded but Kafka offset commit failed**, the same event can be delivered again: the SQL ledger or Kafka-position uniqueness makes the retry safe without an extra count.

Offset commits happen **after** SQL, but not atomically with SQL. This implementation deliberately favors clarity and correctness of the projection over maximum throughput. It is validated for a **single active Worker**, not all rebalances, rolling deployments, multi-instance coordination, extended outage or very large-message situations.

## 5. Local configuration and startup

### Prerequisites

- .NET 10 SDK, Visual Studio, Docker Desktop (Linux containers), local `.env` created from `.env.example`.
- Existing SQL Server 2022 / RabbitMQ / Kafka Compose infrastructure; Kafka topics initialized according to [Kafka Local Development](KAFKA_LOCAL_DEVELOPMENT.md).
- A dedicated local connection string for `ConferenceProFlowAnalytics` in **Analytics Worker User Secrets**. Do not commit database passwords or `.env`.
- The existing `InitialAnalyticsPersistence` EF Core migration applied to `ConferenceProFlowAnalytics`. A new developer uses `dotnet ef database update ...`, not `migrations add` again.

Start the Compose profile from the **repository root**:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
```

Apply **existing** migrations if necessary:

```powershell
dotnet ef database update --project src/ConferenceProFlow.Analytics.Worker --startup-project src/ConferenceProFlow.Analytics.Worker --context AnalyticsDbContext
```

In Visual Studio choose `ConferenceProFlow.Analytics.Worker` → **Manage User Secrets**. Merge the sample below into existing settings, keeping the correct local SQL password. The sample is a **complete example only**, **not** an instruction to erase other secrets:

```json
{
  "ConnectionStrings": {
    "AnalyticsDb": "Server=127.0.0.1,14333;Database=ConferenceProFlowAnalytics;User Id=sa;Password=YOUR_LOCAL_SQL_PASSWORD;Encrypt=True;TrustServerCertificate=True;"
  },
  "AnalyticsKafkaConsumer": {
    "Enabled": true,
    "ObserveOnly": false,
    "BootstrapServers": "127.0.0.1:9092",
    "Topic": "conferenceproflow.registration-finalized.v1",
    "GroupId": "conferenceproflow.analytics.v1"
  }
}
```

Three supported modes:

| `Enabled` | `ObserveOnly` | `GroupId` | Effect |
| --- | --- | --- | --- |
| `false` | any | any | No Kafka consumption; Worker foundation starts. |
| `true` | `true` | `conferenceproflow.analytics.preview.v1` | Preview only; Kafka messages are logged as metadata, **no SQL writes and no offset commits**. |
| `true` | `false` | `conferenceproflow.analytics.v1` | Durable processing to Analytics SQL; commit Kafka offset after durable handling. |

**Do not reuse the preview group for durable mode.** On first startup of a never-used group, `AutoOffsetReset=Earliest` can consume every **currently retained** record; if offsets already exist for that group, it continues from those committed positions. The Analytics Worker's default `appsettings.json` has `Enabled=false`, `ObserveOnly=true`; durable mode is explicitly opt-in.

For a full demonstration, Registration API and Allocation Worker must also run with their existing messaging components enabled and Registration Kafka publishing enabled. See [M2.6 Allocation E2E](ALLOCATION_E2E_DEMO.md) and [M3.3 Kafka Publishing](KAFKA_PUBLISHING.md). Keep each app in a separate terminal:

```powershell
dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj --launch-profile https
```

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj --launch-profile ConferenceProFlow.Allocation.Worker
```

```powershell
dotnet run --project src/ConferenceProFlow.Analytics.Worker/ConferenceProFlow.Analytics.Worker.csproj
```

The durable Worker logs `M3.4E durable Analytics consumer starting`, then per-record dispositions and offset commits. Stop with **Ctrl+C**. Setting `AnalyticsKafkaConsumer:Enabled=false` does not delete broker offsets or the Analytics SQL database.

### SQL and consumer-group inspection

Use **Visual Studio → View → SQL Server Object Explorer** and the Analytics database `ConferenceProFlowAnalytics`; never use other service databases inside application code. For read-only queries see [Analytics SQL Verification](ANALYTICS_SQL_VERIFICATION.md) or [E2E SQL Checks](ANALYTICS_E2E_CHECKS.sql).

```sql
SELECT SessionId, ConfirmedCount, RejectedCount, UpdatedAtUtc
FROM dbo.SessionRegistrationStats
ORDER BY UpdatedAtUtc DESC;
```

Inspect committed Kafka group offsets from the repository root:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group conferenceproflow.analytics.v1
```

## 6. Automated tests and manual M3.4F demonstration

The Analytics IntegrationTests project has **30 M3.4B–E test cases** in the current implementation (6 persistence, 2 preview, 11 SQL idempotent processing, 11 record handler and Kafka+SQL durable consumer). Some are parameterized xUnit test cases. SQL Server / Kafka Testcontainers require Docker Desktop.

From the repository root:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Release --no-build --verbosity normal
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

The manual runbook [ANALYTICS_E2E_DEMO.md](ANALYTICS_E2E_DEMO.md) tests three distinct checkpoints against a **new** capacity-2 session:

| Checkpoint | Registration state | Analytics state |
| --- | --- | --- |
| A: 3 finalized registrations | 2 Confirmed / 1 Rejected | 2 confirmed / 1 rejected; 3 processed events |
| B: Stop Worker, finalize 4th | 2 Confirmed / 2 Rejected | Temporarily stays 2/1 until restart |
| B: Restart same durable group | 2 Confirmed / 2 Rejected | Catches up to 2/2; 4 processed events |
| C: Republish exact first event | Unchanged | Remains 2/2; `DuplicateMessage`, no new ledger row |

The script `scripts/demo/Replay-AnalyticsFinalization.ps1` resends **one** original Kafka payload for demonstration. This is **not** the separate-group, isolated-database replay implemented in **M3.5**. It does not reset SQL or Kafka offsets. Check a script before running it; use a process-scoped PowerShell execution-policy override only when permitted by your environment and trusted.

## 7. M3.5 — Isolated, bounded Kafka replay

The **same .NET 10 Analytics Worker** now supports mutually exclusive live and one-shot replay modes. In replay mode, `AnalyticsReplay:Enabled=true` and `AnalyticsKafkaConsumer:Enabled=false`. A guard requires an isolated SQL connection to **`ConferenceProFlowAnalyticsReplay`** on the same SQL Server, a replay-only Kafka consumer group (`conferenceproflow.analytics.replay.<run-id>`), and the supported `conferenceproflow.registration-finalized.v1` topic. It reuses the existing `AnalyticsDbContext`, initial migration, record handler, SQL idempotency ledger, projections and quarantine — not a second business-logic implementation.

The replay runner first captures **each Kafka partition's earliest retained offset and exclusive high watermark**. It writes a local JSON manifest at `%LOCALAPPDATA%\ConferenceProFlow\ReplaySnapshots\<group-hash>.json` (Windows default since M3.6E1) and reads exactly those captured ranges. Every successfully applied/recognized duplicate/durably quarantined record is followed by a **manual commit for the replay group**; replay ends only when all captured windows are complete. An interrupted run can explicitly resume with the same snapshot, replay group and SQL state; a completed run cannot be resumed. Missing records caused by retention stop replay instead of silently skipping a gap.

Manual M3.5D verification of a **fresh** session (capacity two) showed matching **2 confirmed / 1 rejected** SQL projections and **three identical finalized-event identities** in live and replay databases, while the live Kafka group offsets stayed unchanged across the isolated replay. Six SQL Server/Kafka Testcontainers replay cases passed locally in M3.5E after a clean rebuild. The M3.5 full-solution Release build and regression passed locally on 24 November 2026; the M3.5 Pull Request subsequently passed GitHub Actions and was merged.

Read [Replay Design](KAFKA_REPLAY_DESIGN.md), [Replay Setup](KAFKA_REPLAY_SETUP.md), [Replay Runner](KAFKA_REPLAY_RUNNER.md), [Replay E2E](KAFKA_REPLAY_E2E_DEMO.md), and [Replay Integration Tests](KAFKA_REPLAY_INTEGRATION_TESTS.md). Replay reconstructs **retained events only**; it is not an unlimited historical archive or a replacement for existing live statistics.

## 8. Operational limitations and next milestone

- **At-least-once:** SQL commit and Kafka offset commit can fail independently. Duplicates may occur in Kafka; SQL projections are idempotent for supported events.
- **Single active instance:** Parallel Kafka consumer group members/long rebalances/rolling deployments and scaling behavior have not been comprehensively validated.
- **Bounded retention:** The local topic currently retains data for seven days; the Worker cannot replay events that Kafka no longer has.
- **Quarantine is not remediation:** malformed or conflicting events are stored for manual analysis, not automatically corrected or routed to a production dead-letter process.
- **Local only:** Docker-based Kafka is a single-node development cluster; no production secrets management, HA, alerting, SLOs or disaster-recovery guarantee is provided.
- **Focused metrics:** only finalized confirmed/rejected counts by session; no dashboard, time-series aggregates, cancellation/amendment handling or privacy-governed operational API.
- **M3.5 implemented locally:** replay uses a new group, isolated SQL and bounded snapshot; the runner is single-process/local-file coordinated, not a multi-node production replay platform. M3.6 locally completed milestone-wide audit and hardening; the final M3.6 PR/CI check is pending.

**Do not reset live Kafka offsets, delete live projection tables or run `docker compose down -v` to satisfy a demo.** A green integration test suite does not imply full production readiness.

## M3.6 hardening note

The bounded Replay Runner's manifest location now defaults to `%LOCALAPPDATA%\ConferenceProFlow\ReplaySnapshots` on Windows, not the temporary directory. An absolute `AnalyticsReplay:SnapshotDirectory` can be set in User Secrets; retain exactly the same path for an interrupted run and explicit `Resume=true`. This is local persistence, **not** shared HA or backup storage. See [Replay Snapshot Storage](REPLAY_SNAPSHOT_STORAGE.md).

The Registration-owned Kafka Outbox also gained diagnostic-only categories, threshold-based error logs and read-only SQL inspection guidance; it does not change the Analytics consumer SQL/offset transaction boundary or introduce producer-side quarantine. See [Kafka Outbox Operational Visibility](KAFKA_OUTBOX_OPERATIONAL_VISIBILITY.md).
