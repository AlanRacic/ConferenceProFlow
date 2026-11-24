# M3.5B — Isolated Kafka Replay Database Bootstrap

**Status:** replay configuration guards and isolated SQL schema only. **No replay consumer or Kafka offset operation is implemented in M3.5B.** The bounded runner comes in M3.5C.

Parent design: [`KAFKA_REPLAY_DESIGN.md`](KAFKA_REPLAY_DESIGN.md). The existing live Analytics system and its consumer group remain unchanged.

## Safety contract

- Live database: `ConferenceProFlowAnalytics` (`ConnectionStrings:AnalyticsDb`).
- Replay database: `ConferenceProFlowAnalyticsReplay` (`ConnectionStrings:AnalyticsReplayDb`).
- Both connection strings must identify **the same SQL Server Data Source**. Copy the live string and change **only** its `Database` value. This strict check intentionally rejects differing aliases such as `localhost` versus `127.0.0.1`.
- Replay group must start with `conferenceproflow.analytics.replay.` and have a nonempty, safe run identifier; never use the live or preview consumer group.
- Only the `conferenceproflow.registration-finalized.v1` topic is supported.
- `AnalyticsKafkaConsumer:Enabled` must be `false` during replay setup.
- The special EF Core command must include `-- --replay`. The factory never falls back to the live connection string for this command.
- No existing Analytics table is truncated, dropped, or touched by the replay schema bootstrap. The same `AnalyticsDbContext` and `InitialAnalyticsPersistence` migration are reused.
- M3.5B deliberately refuses application startup if `AnalyticsReplay:Enabled=true`: the replay runner has **not** been implemented yet. The flag is enabled temporarily **only for design-time database bootstrap**.

These safeguards reduce operator mistakes in local development; they are not a substitute for database-scoped credentials, backups, and production authorization.

## 1. Prerequisites

From `C:\work\ConferenceProFlow` on `feature/kafka-analytics-replay`:

1. Existing M3.4 live Analytics database and migration are present.
2. Docker Desktop and the existing SQL Server container are running.
3. EF CLI (`dotnet ef`) is available, as used during M3.4B.
4. Copy the M3.5B package's `src`, `tests`, and `docs` contents to the repository root. Do not create `docs/docs`.
5. No additional NuGet packages, solution projects, or migrations are required.

## 2. Configure User Secrets (Analytics Worker only)

Right-click `ConferenceProFlow.Analytics.Worker` in Visual Studio → **Manage User Secrets**.

Keep your existing `ConnectionStrings:AnalyticsDb` exactly as it is. Add a second entry by **copying** that connection string and changing ONLY `Database=ConferenceProFlowAnalytics` to `Database=ConferenceProFlowAnalyticsReplay`.

Illustrative shape — **do not replace other existing secrets**:

```json
{
  "ConnectionStrings": {
    "AnalyticsDb": "<keep your existing live Analytics SQL connection>",
    "AnalyticsReplayDb": "Server=127.0.0.1,14333;Database=ConferenceProFlowAnalyticsReplay;User Id=sa;Password=<YOUR EXISTING LOCAL PASSWORD>;Encrypt=True;TrustServerCertificate=True;"
  },
  "AnalyticsKafkaConsumer": {
    "Enabled": false
  },
  "AnalyticsReplay": {
    "Enabled": true,
    "BootstrapServers": "127.0.0.1:9092",
    "Topic": "conferenceproflow.registration-finalized.v1",
    "GroupId": "conferenceproflow.analytics.replay.demo01"
  }
}
```

Use a locally configured password; do not commit secrets. If `AnalyticsKafkaConsumer` already has other properties, **retain** them and change only `Enabled` to `false`. Likewise retain any other existing secret sections.

**Important:** For M3.5B `AnalyticsReplay:Enabled=true` is only a temporary setup flag. A normal `dotnet run` of the Worker will intentionally fail closed until this flag is set back to `false`.

## 3. Compile and run the new automated checks

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Release --filter "FullyQualifiedName~AnalyticsReplaySafetyTests|FullyQualifiedName~AnalyticsReplayDatabaseIsolationIntegrationTests" --verbosity normal
```

The tests verify rejected unsafe configurations and apply the **existing** EF migration to two isolated databases inside a disposable SQL Server Testcontainer. They do **not** connect to your real local databases.

## 4. Apply the existing migration to the isolated replay database

First check SQL Server is running. From the project root, execute **one PowerShell line**:

```powershell
dotnet ef database update --project src/ConferenceProFlow.Analytics.Worker --startup-project src/ConferenceProFlow.Analytics.Worker --context AnalyticsDbContext -- --replay
```

This invokes `AnalyticsDbContextFactory` with an explicit replay argument, validates the destination and selects **only** `ConnectionStrings:AnalyticsReplayDb`. The existing `InitialAnalyticsPersistence` migration should be applied; **do not run `migrations add` again**.

If the guard refuses the command, do not bypass it. Check the two database names, server aliases, replay `Enabled` flag, consumer `Enabled` flag, and replay GroupId. Never replace the replay connection with the live connection to make the command work.

## 5. Verify in Visual Studio SQL Server Object Explorer

Navigate to the same SQL Server instance → `Databases` → `ConferenceProFlowAnalyticsReplay`.

The replay database should contain `ProcessedAnalyticsEvents`, `SessionRegistrationStats`, `QuarantinedAnalyticsEvents`, and EF's `__EFMigrationsHistory`.

Run these **read-only** queries against **the replay database**:

```sql
SELECT name FROM sys.tables ORDER BY name;
SELECT MigrationId FROM dbo.__EFMigrationsHistory ORDER BY MigrationId;
SELECT COUNT(*) AS ReplayProcessedEvents FROM dbo.ProcessedAnalyticsEvents;
SELECT COUNT(*) AS ReplaySessionStats FROM dbo.SessionRegistrationStats;
SELECT COUNT(*) AS ReplayQuarantineRows FROM dbo.QuarantinedAnalyticsEvents;
```

New tables should initially have zero rows. **Do not delete or reset the existing live Analytics database**.

## 6. Restore normal local development configuration

After the replay migration succeeds, set `AnalyticsReplay:Enabled` back to `false` in User Secrets. Keep `AnalyticsKafkaConsumer:Enabled=false` unless you intentionally need the *existing* live consumer again. Normal M3.4 application behavior must remain unchanged.

Optionally verify the build and all Analytics tests again; you do not need Kafka for M3.5B SQL bootstrapping.

## 7. What M3.5B does NOT do

- Does not launch a replay Kafka Consumer.
- Does not subscribe to a topic, commit/reset offsets, or change either consumer group.
- Does not reconstruct projection statistics from events yet.
- Does not create a new EF migration, application, container, or Kafka topic.

**Next:** M3.5C implements bounded replay (per-partition low/high watermarks, SQL-first handling, explicit commit and completion reporting), built on this isolated schema.
