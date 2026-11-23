-- ConferenceProFlow M3.4F: READ-ONLY, MANUAL diagnostic for one fresh session.
-- Run in Visual Studio -> SQL Server Object Explorer -> New Query.
-- Set ONLY the new session GUID produced by docs/ALLOCATION_E2E.http request 2.
-- The human-run queries access two service-owned DBs for DIAGNOSTICS only.
-- The applications still must not query each other's databases.
-- No INSERT, UPDATE, DELETE, DROP or offset manipulation is performed.

DECLARE @SessionId uniqueidentifier = 'PASTE-FRESH-SESSION-GUID-HERE';

-- (1) Authoritative Registration statuses. First: 3 total, 2/1/0.
-- After the additional registration during Analytics downtime: 4 total, 2/2/0.
SELECT
    COUNT(*) AS RegistrationTotal,
    SUM(CASE WHEN Status = 0 THEN 1 ELSE 0 END) AS Pending,
    SUM(CASE WHEN Status = 1 THEN 1 ELSE 0 END) AS Confirmed,
    SUM(CASE WHEN Status = 2 THEN 1 ELSE 0 END) AS Rejected
FROM [ConferenceProFlowRegistration].[dbo].[Registrations]
WHERE SessionId = @SessionId;

-- (2) Registration-owned Analytics Outbox. First 3 published events,
-- later 4; PublishedAtUtc should be non-null for Kafka-acknowledged events.
SELECT
    Id AS MessageId,
    RegistrationId,
    JSON_VALUE(Payload, '$.sessionId') AS SessionId,
    JSON_VALUE(Payload, '$.status') AS EventStatus,
    PublishedAtUtc,
    PublishAttempts,
    LastError
FROM [ConferenceProFlowRegistration].[dbo].[AnalyticsOutboxMessages]
WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(Payload, '$.sessionId')) = @SessionId
ORDER BY OccurredAtUtc, Id;

-- (3) Analytics-owned materialized projection. First: 2 confirmed / 1 rejected;
-- while Analytics Worker is stopped: remains 2/1;
-- after restarting Analytics Worker: 2 confirmed / 2 rejected.
SELECT
    SessionId,
    ConfirmedCount,
    RejectedCount,
    UpdatedAtUtc
FROM [ConferenceProFlowAnalytics].[dbo].[SessionRegistrationStats]
WHERE SessionId = @SessionId;

-- (4) Analytics-owned processed event ledger: first 3, then 4 unique rows.
-- Intentionally replaying one ALREADY processed MessageId must not add a row.
SELECT
    MessageId,
    RegistrationId,
    SessionId,
    Status,
    ProcessedAtUtc
FROM [ConferenceProFlowAnalytics].[dbo].[ProcessedAnalyticsEvents]
WHERE SessionId = @SessionId
ORDER BY ProcessedAtUtc, MessageId;

-- (5) Compare ledger totals to materialized projection: counts should agree.
SELECT
    SUM(CASE WHEN Status = N'confirmed' THEN 1 ELSE 0 END) AS LedgerConfirmed,
    SUM(CASE WHEN Status = N'rejected' THEN 1 ELSE 0 END) AS LedgerRejected,
    COUNT(*) AS LedgerTotal
FROM [ConferenceProFlowAnalytics].[dbo].[ProcessedAnalyticsEvents]
WHERE SessionId = @SessionId;

-- (6) Inspect quarantine metadata for evidence of invalid historical records.
-- Quarantine is Kafka-position-oriented and NOT scoped to this session.
-- Historical smoke events may be present and are not included in session stats.
SELECT TOP (20)
    Topic,
    Partition,
    [Offset],
    Reason,
    ReceivedAtUtc
FROM [ConferenceProFlowAnalytics].[dbo].[QuarantinedAnalyticsEvents]
ORDER BY ReceivedAtUtc DESC;
