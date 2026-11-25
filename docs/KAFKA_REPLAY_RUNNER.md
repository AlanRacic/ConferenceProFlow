# ConferenceProFlow — Bounded Kafka Replay Runner (M3.5C)

**Status:** M3.5 replay runner implemented, manually demonstrated and verified with six Kafka/SQL Testcontainers scenarios; M3.5 merged after CI. M3.6E1 updates snapshot storage and passed focused + six-runner local regressions; M3.6 PR/CI pending.  
**Prerequisites:** M3.5B replay safety guards and an **empty, migrated** `ConferenceProFlowAnalyticsReplay` database.  
**Related:** [`KAFKA_REPLAY_DESIGN.md`](KAFKA_REPLAY_DESIGN.md) · [`KAFKA_REPLAY_SETUP.md`](KAFKA_REPLAY_SETUP.md) · [`ANALYTICS_WORKER.md`](ANALYTICS_WORKER.md).

## 1. Safety and execution model

The existing .NET 10 Analytics Worker has two **mutually exclusive** modes:

- **Live mode** (default): `AnalyticsReplay:Enabled=false`. The existing M3.4 Kafka consumer and `ConferenceProFlowAnalytics` database continue unchanged.
- **Replay mode** (explicit opt-in): `AnalyticsReplay:Enabled=true`, `AnalyticsKafkaConsumer:Enabled=false`. The same SQL processor and quarantine handler are injected with the **separate replay database** connection. Replay runs once, stops at a captured Kafka snapshot, and the process exits. An exception produces a nonzero process exit; it is **not** reported as success.

The M3.5B `AnalyticsReplaySafety` guard checks the expected SQL database names, identical SQL Server Data Source, supported topic, a replay-only consumer group prefix, and the disabled live consumer in the replay process. The runner also checks that replay EF migrations are applied. **A fresh run requires an empty replay database and unused Kafka replay group.**

Replay never calls `Delete`, `EnsureDeleted`, `Seek` on the live group, or Kafka group offset reset APIs. The live Analytics database and consumer group are neither selected nor updated by this code path. A per-group filesystem lock on the local machine prevents two concurrent replay processes from sharing the same manifest/group.

## 2. Bounded snapshot algorithm

At startup the runner obtains Kafka topic metadata and captures for **every partition**:

- `low`: the earliest retained offset **at snapshot time**;
- `highExclusive`: the exclusive high watermark **at snapshot time**;
- `next`: the first offset to read (initially `low`).

It writes a local JSON manifest **before** consuming the first record. This defines the immutable `[low, highExclusive)` snapshot range per partition. Empty partitions (`low == highExclusive`) are already complete.

The consumer uses manual partition assignment with **explicit start offsets**. Its **separate** Kafka `GroupId` is used to commit offsets; this local runner does not use group rebalancing or horizontal scaling. `EnableAutoCommit=false`, `EnableAutoOffsetStore=false`, `AllowAutoCreateTopics=false`.

For each record within the captured range:

1. Reuse M3.4E `KafkaAnalyticsRecordHandler` and `AnalyticsEventProcessingService` to process or durably quarantine the record in **Replay SQL**.
2. Only after SQL work succeeds, explicitly `Commit(record)` for **the replay group**.
3. Advance the partition's local next offset, update diagnostic counters and atomically persist the manifest.
4. Pause that partition once its captured exclusive high watermark is reached. Newly published records are **outside** this run's snapshot.

The runner reports `REPLAY COMPLETED` **only after all captured partitions reach their exclusive highs**. Unexpected offset gaps, retention loss, SQL errors, broker/commit errors, interruption or prolonged lack of progress abort the run without claiming success.

> **Important:** This implementation assumes the registration-finalized topic is non-compacted and contains no intentional offset gaps in its assigned window. It conservatively refuses an unexplained gap instead of silently declaring a complete replay. Broker-level integration tests for the bounded runner, SQL fault/resume and simulated retention gaps passed locally in M3.5E.

## 3. First-run configuration (Visual Studio User Secrets)

**Do not overwrite** your existing connection strings or other secrets. The `ConnectionStrings:AnalyticsReplayDb` added in M3.5B must point to `ConferenceProFlowAnalyticsReplay`, while `ConnectionStrings:AnalyticsDb` remains the original `ConferenceProFlowAnalytics`.

Add or update these two existing sections:

```json
{
  "AnalyticsKafkaConsumer": {
    "Enabled": false,
    "ObserveOnly": false,
    "BootstrapServers": "127.0.0.1:9092",
    "Topic": "conferenceproflow.registration-finalized.v1",
    "GroupId": "conferenceproflow.analytics.v1"
  },
  "AnalyticsReplay": {
    "Enabled": true,
    "Resume": false,
    "MaxIdleSeconds": 60,
    "BootstrapServers": "127.0.0.1:9092",
    "Topic": "conferenceproflow.registration-finalized.v1",
    "GroupId": "conferenceproflow.analytics.replay.demo01"
  }
}
```

The example above shows **only two sections** of the full secrets file. Keep `ConnectionStrings` in the actual file. Do **not** commit secrets to Git.

**`demo01` must be an unused group** and its replay SQL database must still be empty. M3.5B created the empty database. If a previous attempt ran, follow the resume procedure; do not delete or truncate the live database.

Start local Kafka/SQL infrastructure from `C:\work\ConferenceProFlow`:

```powershell

docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
```

Run the one-shot replay (PowerShell, one line):

```powershell

dotnet run --project src/ConferenceProFlow.Analytics.Worker/ConferenceProFlow.Analytics.Worker.csproj
```

Expected output includes partition boundaries, progress and `REPLAY COMPLETED`. Unlike live mode, this command **exits on its own** when the original snapshot has been processed. The counts reflect replayed records, including unrelated retained events in the topic. To verify a particular demonstration, compare a **fresh session** in the two databases, not global totals.

After successful verification set `AnalyticsReplay:Enabled=false` in User Secrets so normal Analytics Worker launches stay unchanged.

## 4. Snapshot file and deliberate resume

The snapshot is stored locally outside the repository:

`%LOCALAPPDATA%\ConferenceProFlow\ReplaySnapshots\<group-hash>.json` (Windows default since M3.6E1; override with an **absolute** `AnalyticsReplay:SnapshotDirectory`)

The exact path appears in the logs. Previous M3.5 `%TEMP%` manifests are not automatically moved: consult [Replay Snapshot Storage](REPLAY_SNAPSHOT_STORAGE.md) before attempting to resume an unfinished older run. The file contains topic, broker, replay group, expected database name, snapshot bounds, per-partition progress, counters and completion status; **no SQL passwords or Kafka payloads**. A sibling `.lock` file is used only for a process-level lock. Do not commit the manifest to the repository.

If the replay is interrupted or an SQL/Kafka operation fails:

1. **Do not** change the replay `GroupId`, topic or replay database.
2. Fix the underlying problem and leave the existing replay SQL rows intact.
3. Change only `AnalyticsReplay:Resume` to `true`.
4. Run the same `dotnet run` command again.
5. The runner loads the original manifest and starts from that **group's Kafka-committed offsets**. If the SQL transaction completed but the Kafka commit failed, the original event can be read again and M3.4D idempotency prevents double counting.

Replay fails if the manifest is missing or marked complete, if group offsets fall outside the saved snapshot, or if retention advanced beyond unprocessed records. It never expands the original high watermarks during resume. For a fresh demonstration use a **new group and an empty replay database**, not a group-offset reset.

**Diagnostic counters caveat:** In the narrow crash window between a successful Kafka commit and manifest write, the counters in the JSON manifest can lag. Kafka's committed offsets are authoritative for resuming; the SQL ledger/statistics and quarantine tables are authoritative for business results. Do not use manifest counters alone as a financial or audit record.

## 5. Read-only SQL verification

Open **Visual Studio → SQL Server Object Explorer** and run these SELECTs against `ConferenceProFlowAnalyticsReplay`:

```sql
SELECT SessionId, ConfirmedCount, RejectedCount, UpdatedAtUtc
FROM dbo.SessionRegistrationStats ORDER BY UpdatedAtUtc DESC;

SELECT COUNT(*) AS ProcessedEvents FROM dbo.ProcessedAnalyticsEvents;
SELECT COUNT(*) AS QuarantinedEvents FROM dbo.QuarantinedAnalyticsEvents;
```

For a **specific freshly created session**, run the following SQL separately in **both** `ConferenceProFlowAnalytics` and `ConferenceProFlowAnalyticsReplay` using the same `@SessionId`:

```sql
DECLARE @SessionId uniqueidentifier = 'PASTE-FRESH-SESSION-GUID-HERE';
SELECT SessionId, ConfirmedCount, RejectedCount
FROM dbo.SessionRegistrationStats WHERE SessionId = @SessionId;
SELECT MessageId, RegistrationId, Status
FROM dbo.ProcessedAnalyticsEvents WHERE SessionId = @SessionId ORDER BY RegistrationId;
```

The two projections should match **for that session** if all its source events are still within Kafka retention. The replay may contain other historical events or quarantined smoke-test messages.

To inspect the replay group independently:

```powershell

docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group conferenceproflow.analytics.replay.demo01
```

For comparison, query the live group **read-only** with `--group conferenceproflow.analytics.v1`. Never run `--reset-offsets` on the live group.

## 6. Automated checks and limitations

M3.5C adds focused tests for exclusive high watermark logic, empty partitions, offset-gap rejection, manifest round-trip and automatic-commit configuration. **Real Kafka + SQL replay integration tests (including failures and multi-partition behavior) were added and passed in M3.5E.**

```powershell

dotnet build ConferenceProFlow.slnx --configuration Release

dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Release --no-build --filter "FullyQualifiedName~KafkaReplaySnapshotTests" --verbosity normal
```

### Explicit limitations

- **Kafka retention is seven days in our local topic.** A successful bounded replay means *all available events in the captured retained snapshot* were processed. It does not prove restoration of expired history.
- Replay uses a **single process** and explicit partition assignment; it is not a scalable parallel rebalance protocol.
- A fresh replay requires an empty replay SQL database and unused replay group. The program **never** silently clears existing data.
- The local filesystem manifest is a **demonstration artifact**, not a production distributed checkpoint store. Resume requires the original machine's manifest (or controlled transfer).
- Replay has at-least-once Kafka delivery and uses the same durable SQL idempotency/quarantine semantics as the live worker. No cross-system exactly-once claim.
- M3.5D adds a controlled fresh-session projection comparison; M3.5E adds real infrastructure regression and failure/recovery integration tests.

## M3.6E1 snapshot storage upgrade

The OS temporary path is no longer the default for **new** manifests. The runner uses a persistent per-user LocalApplicationData folder; `AnalyticsReplay:SnapshotDirectory` can override it with an absolute path. Relative overrides are rejected and a missing default user data directory causes a fail-closed error. Original Kafka offset/watermark and SQL idempotency semantics are unchanged. See [Replay Snapshot Storage](REPLAY_SNAPSHOT_STORAGE.md).
