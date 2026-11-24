# ConferenceProFlow — Kafka Replay & Consumer Groups (M3.5)

**Status:** M3.5A design baseline implemented through M3.5E and locally verified (controlled 2/1 replay + six replay Kafka/SQL integration test cases); M3.5F local Release build and full regression passed; PR/CI pending.  
**Parent design:** [`KAFKA_ANALYTICS_DESIGN.md`](KAFKA_ANALYTICS_DESIGN.md), decision AD-05.  
**Baseline:** M3.4 durable `KafkaAnalyticsConsumerService`, `KafkaAnalyticsRecordHandler`, idempotent SQL processing, quarantine and explicit offset commits.

## 1. Objective

Reconstruct confirmed/rejected registration statistics **from Kafka records still retained** without resetting the live consumer group or changing the live Analytics SQL database. This is a controlled local replay and verification feature, **not** a guaranteed complete historical restore.

## 2. Isolation model

| Dimension | Live Analytics | Replay |
| --- | --- | --- |
| Kafka topic | `conferenceproflow.registration-finalized.v1` | Same topic, read-only consumption |
| Kafka group | `conferenceproflow.analytics.v1` | `conferenceproflow.analytics.replay.<run-id>` |
| SQL database | `ConferenceProFlowAnalytics` | `ConferenceProFlowAnalyticsReplay` |
| SQL migrations | Existing `InitialAnalyticsPersistence` | **Reuse** the existing migration in isolated DB |
| Message handling | Existing M3.4 processor | Reuse the same validation, ledger, projection and quarantine logic |
| Side effects | Maintains live projections | Must **never** write live projections or reset live offsets |

```text
                          Kafka topic
                             |
                  +----------+----------+
                  |                     |
                  v                     v
           Live consumer             Replay consumer
           (live group)             (unique group)
                  |                     |
                  v                     v
           Live Analytics SQL       Replay Analytics SQL
           (existing)              (fresh, isolated)
```

Prefer a **guarded replay mode of the existing Analytics Worker** over a duplicate service/logic implementation. Replay invocation must be explicit and refuse to start when the configured SQL database or consumer group belongs to the live worker. Keep configuration and secrets out of committed files.

## 3. Offset and bounded-execution semantics

1. A **fresh** replay group has no committed positions; with `AutoOffsetReset=Earliest` it starts from Kafka's **earliest available retained** offsets. This is not necessarily offset zero.
2. At replay start, capture earliest and exclusive high-watermark positions for each topic partition. Do not claim a complete bounded replay merely because an idle consumer timed out.
3. Process all records in the chosen snapshot window, and stop after all partition targets are reached. Treat empty partitions as complete if their start and end positions coincide.
4. For every record, SQL projection processing or quarantine must complete durably before its replay-group offset is committed.
5. An interruption, missing partition assignment, broker outage, failed SQL write or failed offset commit cannot be reported as a successful completed run. An interrupted run may be resumed **deliberately** with the same group and target boundaries; a new run should use a new group.
6. Log/report per-partition earliest/start/end offsets, count of applied/duplicate/quarantined records, and final completion status. Avoid logging full untrusted payloads.

### Retention is a hard limit

The current local topic has **seven-day retention**. A replay can reconstruct only what is still retained. A complete rebuild of a historical live projection cannot be claimed if earlier necessary events have expired. For the demonstration, create a **new** test session and verify all its finalization events are within retention; compare **that session's** totals and event IDs. Do **not** compare entire live and replay databases as though they necessarily cover the same history.

## 4. SQL setup and safety guards

Use `ConferenceProFlowAnalyticsReplay` on the existing SQL Server container. Its database schema is derived from the same `AnalyticsDbContext` and `InitialAnalyticsPersistence` migration as live Analytics, but the two connections must never be confused.

Required preflight checks before replay starts:

- Replay must be explicitly enabled.
- Connection string must target an expected **replay** database name, not `ConferenceProFlowAnalytics`.
- Kafka group ID must begin with the documented replay prefix and cannot equal the live or preview group.
- Topic must match the supported `RegistrationFinalizedV1` topic.
- Reject empty/missing configuration and report clear non-secret error messages.
- No scripts may truncate/drop the live database, reset live Kafka group offsets or remove Kafka volumes.

For a fresh demonstration, create the replay schema through existing EF Core migrations against the **replay** database only. Any automated setup command needs the same database-name guard; running the normal `database update` command against the live connection is not a replay setup.

## 5. Reuse the existing processing path

The M3.4 SQL pipeline already handles:

- New valid finalizations: atomic processed-event ledger + session statistics.
- Duplicate `MessageId` or identical business finalization: no extra count.
- Conflicting business decisions and malformed records: bounded durable quarantine.
- SQL failure: no offset commit; retry/recovery instead of accidental skip.

Keep these rules unchanged in replay. Replay must use the same event contract and key validation, not a simplified custom counter. This helps ensure that the restored projection has the same semantics as live Analytics.

## 6. Demonstration and verification

1. Record current live Analytics SQL totals and live Kafka committed offsets (read-only).
2. Produce a new capacity-2 session with three finalized registrations; confirm live Analytics shows `2 confirmed / 1 rejected` and three unique processed events.
3. With no new demo traffic, start a fresh replay group and isolated DB. Capture the Kafka partition snapshot range and execute bounded replay.
4. Check the same session in replay SQL: `2 confirmed / 1 rejected`, three processed-event identities. Other historical sessions are irrelevant to this controlled assertion.
5. Replay a known duplicate event within the selected retained range (or test with a dedicated isolated test topic) and confirm there is no extra count.
6. Compare live SQL and replay SQL using **read-only SELECT** queries; check the live group's offsets again. Do not claim offsets must be exactly equal to the previous value if live traffic continued; ensure replay never targets the live group.
7. Document the retention window, partition offsets, quarantine counts and any partial-history limitations in the run report.

## 7. Integration test coverage

Automate with Kafka and SQL Server Testcontainers wherever practical:

- Fresh group starts at the earliest retained event and writes only replay DB.
- Guards reject live SQL database name and live/preview group IDs.
- Multiple partitions and empty partitions terminate accurately at captured end offsets.
- Bounded replay ignores events published **after** captured high watermarks for the completed snapshot.
- Duplicate events preserve single business count, quarantine retains invalid/conflicting records.
- SQL failure before commit does not advance the replay group's offset.
- Restart/resume behavior is explicit and does not falsely claim completion.
- Retention gaps are detected/reported; never claim reconstruction of expired history.
- Existing M3.4 IntegrationTests and full solution regression suite remain green.

## 8. Out of scope and limitations

No automatic replacement of live Analytics projections, no reset of live offsets, no global exactly-once claim, no full recovery beyond Kafka retention, no new dashboard/API, no multi-broker HA or multi-replica production proof. All infrastructure runs locally at zero cloud cost.

## 9. Implementation sequence

- **M3.5A:** design baseline, safety requirements and offset model.
- **M3.5B:** guarded replay configuration and isolated database bootstrap.
- **M3.5C:** bounded Kafka replay runner, offset snapshots, completion reporting.
- **M3.5D:** controlled E2E projection comparison and group independence demonstration.
- **M3.5E:** SQL/Kafka Testcontainers integration and failure/restart tests.
- **M3.5F:** docs, Release build, full regression, PR/CI and milestone handoff.

**Implementation review checkpoint:** the implemented runner reuses the M3.4 processor and EF Core migration, uses strict configuration safety checks, and refuses to report success on unexpected offset gaps. The local demonstration and six replay integration test cases passed. Full M3.5 regression passed locally. GitHub Actions CI and M3.6 hardening remain separate gates.
