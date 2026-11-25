# Kafka Replay — M3.5E Integration Test Coverage

**Status:** Six Kafka + SQL Server Replay Runner integration cases passed locally on 24 November 2026; M3.5 full regression and GitHub CI subsequently passed/merged. M3.6E1 snapshot-location modification and six-runner regression passed locally on 25 November 2026; M3.6 CI pending.

## Goal

Verify the **actual `KafkaAnalyticsReplayRunner`** against disposable Kafka and SQL Server instances, rather than testing only in-memory snapshot helpers. This extends M3.5B configuration/isolation tests, M3.5C snapshot unit tests and the M3.5D manual end-to-end verification.

## Test class

`tests/ConferenceProFlow.Analytics.IntegrationTests/Replay/KafkaReplayRunnerIntegrationTests.cs`

This class contains **six test cases** (one theory with two values, plus four facts):

| Case | What it proves |
| --- | --- |
| Empty topic, one partition | A replay with no records can complete with no SQL writes. |
| Empty topic, three partitions | Every empty partition counts as complete; no artificial timeout. |
| Three partitions, duplicates, invalid and post-snapshot record | Replay tracks fixed exclusive highs, deduplicates the same `MessageId`, quarantines malformed JSON, persists expected counts, excludes a **record injected after capture**, and does not modify live SQL or the separate live-group offset. |
| SQL failure and explicit resume | The first event is committed; a test-only EF Core `SaveChangesInterceptor` fails the next event **after snapshot capture, before SQL commit or Kafka offset commit**. The interceptor is omitted on `Resume=true`, which processes the remaining event with the **same** snapshot and group without double counting. |
| Completed run and reused group | `Resume=true` cannot reopen a completed snapshot. Deleting the test's local manifest and resetting its disposable replay DB is still **not** enough to reuse a Kafka group with committed offsets. |
| Kafka low watermark advances during interruption | `DeleteRecordsAsync` simulates data no longer retained in the **disposable test topic only**. The runner rejects resume instead of silently claiming full replay completion. |

### Test infrastructure and safety

- Uses `KafkaBuilder("apache/kafka:4.4.0")` and `MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")`, matching existing project test patterns.
- Creates the **same Analytics EF Core schema** in two separate databases inside the disposable SQL Server: `ConferenceProFlowAnalytics` and `ConferenceProFlowAnalyticsReplay`.
- Unique topic and replay-group IDs are generated for each test.
- Reuses actual `KafkaAnalyticsRecordHandler`, `AnalyticsEventProcessingService`, `AnalyticsQuarantineService`, `AnalyticsDbContext` and `KafkaAnalyticsReplayRunner`.
- Clears only `ConferenceProFlowAnalyticsReplay` **inside the disposable Testcontainer** between tests. It never reads local User Secrets, `.env`, Docker Compose databases, or the user's running Kafka broker.
- The retention simulation deletes records **only from its unique Testcontainer topic**, never the project topic.
- Temporary snapshot manifests for the unique test groups are cleaned up in a `finally` block. The ordinary M3.5D replay snapshot is not touched.

### Technical interpretation

These tests verify **completion within the captured retained Kafka window**, not an unlimited historical restore. An already-expired event that was absent *before* a fresh snapshot cannot be inferred from the Kafka log alone; full historical reconstruction would need an independent archive/checkpoint or source-of-truth comparison.

The tests exercise the runner directly in a controlled DI container. M3.5B's separate `AnalyticsReplaySafetyTests` continue to cover the application's configuration preflight.

The SQL-failure test covers an uncommitted Kafka record. A deliberately injected **Kafka commit failure after successful SQL commit** remains a separate fault-injection scenario; the existing idempotent SQL ledger is its recovery mechanism, but this new test class does not claim to simulate that exact fault.

## Running the tests (PowerShell, solution root)

First build **without `--no-build`** so the new test class is compiled:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

Then run just the new six cases:

```powershell
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Release --no-build --filter "FullyQualifiedName~KafkaReplayRunnerIntegrationTests" --verbosity normal
```

Observed locally after a clean rebuild: `total: 6`, `failed: 0`, `succeeded: 6`. This number counts the two `[InlineData]` values separately. CI remains a separate verification gate.

After that, run the complete Analytics suite (the six-case result does not substitute for this regression gate):

```powershell
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Release --no-build --verbosity normal
```

If any case fails, keep the original output and report the **test name, first exception and stack trace**. Do not change the local Analytics databases or delete the user's replay manifest as a workaround.

## Known limitations

- Testcontainers require Docker Desktop; images can take time to download on the first run.
- Tests may need extra time on a loaded CI runner; do not confuse broker startup delays with business processing errors.
- Since M3.6E1, the runner resolves local snapshots under per-user LocalApplicationData by default (or an absolute override) and still uses a hash derived from each replay group. Test groups are random and distinct from the M3.5D demo group; test cleanup must target only their own manifests.
- The production-aware design does **not** imply production multi-node coordination, unlimited Kafka retention or cross-system exactly-once guarantees.

**M3.6E1 follow-up:** Added path-resolution tests (default per-user directory, absolute override and invalid relative paths) and reran the six Kafka Replay Runner integration cases successfully. No SQL migration or change to the M3.5E fault-injection/resume assertions was needed.
