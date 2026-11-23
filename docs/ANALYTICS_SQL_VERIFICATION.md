# Analytics Worker — SQL verification queries (M3.4E)

Use **Visual Studio → View → SQL Server Object Explorer** and open a query on
`ConferenceProFlowAnalytics` (local SQL Server: `127.0.0.1,14333`).
These are **read-only** verification queries. The Analytics Worker owns this
SQL database; do not query the Registration or Allocation databases from the
Worker.

## 1. Per-session confirmed/rejected projections

```sql
SELECT SessionId, ConfirmedCount, RejectedCount, UpdatedAtUtc
FROM dbo.SessionRegistrationStats
ORDER BY UpdatedAtUtc DESC;
```

For a **fresh session** with capacity 2 and three registration finalizations,
expect `ConfirmedCount = 2` and `RejectedCount = 1`. Other historical sessions
will have their own totals. SQL projections reflect *finalized* events, not
pending Registration rows.

## 2. Processed event ledger

```sql
SELECT TOP (100) MessageId, RegistrationId, SessionId, Status,
       OccurredAtUtc, ProcessedAtUtc
FROM dbo.ProcessedAnalyticsEvents
ORDER BY ProcessedAtUtc DESC;
```

The unique `MessageId` primary key and unique `RegistrationId` index prevent
repeated business finalizations from creating extra ledger rows. Duplicates do
not increase the statistics counts.

## 3. Durable quarantine

```sql
SELECT TOP (100) Topic, Partition, [Offset], MessageKey, Reason, ReceivedAtUtc
FROM dbo.QuarantinedAnalyticsEvents
ORDER BY ReceivedAtUtc DESC;
```

For local development, you can inspect a single quarantined payload as needed:

```sql
SELECT TOP (10) Topic, Partition, [Offset], Payload, Reason
FROM dbo.QuarantinedAnalyticsEvents
ORDER BY ReceivedAtUtc DESC;
```

**Privacy:** `Payload` contains truncated raw incoming event data. Do not copy
these records into GitHub issues, public logs, or screenshots if they contain
personal or confidential information. The Worker logs only metadata and outcome.

A previously sent local M3.2 smoke record in the business topic can legitimately
appear in quarantine; it is not a valid `RegistrationFinalizedV1` event.

## 4. Consumer-group offset verification

From `C:\work\ConferenceProFlow` in Visual Studio's PowerShell Terminal:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group conferenceproflow.analytics.v1
```

Each topic partition has its own committed offset; Kafka commits the *next*
position to read, not the current consumed record number. A newly started group
with no committed offsets reads currently retained records from the beginning
(`AutoOffsetReset=Earliest`). If the group has already committed offsets, it
resumes from those offsets rather than replaying the entire topic.

The offset is not in the same atomic transaction as SQL. A failure **after** SQL
commit but **before** Kafka commit may result in redelivery. Our SQL deduplication
prevents repeated projections in that case.

## 5. Safety and limitations

- No registration records or statistics should be modified manually to fix a
  consumer processing failure in routine use. Fault-injection tests manipulate
  only disposable Testcontainers databases.
- Do not reset Kafka topics/groups or SQL data simply to rerun the worker.
- The M3.4E worker is a single-active-consumer development baseline. Kafka
  rebalances, very long SQL calls, rolling deployments, and multi-replica
  operational behavior are not fully validated yet.
- This step does not add an HTTP API or analytics dashboard. A more thorough
  restart demonstration and documentation review belong to M3.4F.
