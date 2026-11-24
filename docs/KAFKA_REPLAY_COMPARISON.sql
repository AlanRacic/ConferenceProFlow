-- ConferenceProFlow M3.5D — READ-ONLY Kafka replay / live analytics comparison.
-- Visual Studio 2026 -> View -> SQL Server Object Explorer -> New Query.
-- Connect to the same LOCAL SQL Server instance that hosts all project databases.
-- Replace the GUID below with the NEW SessionId from docs/ALLOCATION_E2E.http request 2.
-- Run once AFTER the live demo (before replay) and once AFTER replay completes.
-- No INSERT/UPDATE/DELETE/TRUNCATE/DROP, no Kafka operations.
-- Cross-database SELECTs are for MANUAL diagnostics only, not app-to-app coupling.

DECLARE @SessionId uniqueidentifier = 'a9e3fd76-69a5-4f64-9ff1-7112abd2c628';

-- 1) Authoritative registration states. After 3 registrations: 2 confirmed, 1 rejected.
SELECT
    @SessionId AS SessionId,
    COUNT(*) AS RegistrationTotal,
    COALESCE(SUM(CASE WHEN Status = 0 THEN 1 ELSE 0 END), 0) AS Pending,
    COALESCE(SUM(CASE WHEN Status = 1 THEN 1 ELSE 0 END), 0) AS Confirmed,
    COALESCE(SUM(CASE WHEN Status = 2 THEN 1 ELSE 0 END), 0) AS Rejected
FROM [ConferenceProFlowRegistration].[dbo].[Registrations]
WHERE SessionId = @SessionId;

-- 2) Source events durably published to Kafka.
SELECT
    @SessionId AS SessionId,
    COUNT(*) AS AnalyticsOutboxRows,
    COALESCE(SUM(CASE WHEN PublishedAtUtc IS NOT NULL THEN 1 ELSE 0 END), 0) AS PublishedRows,
    COALESCE(SUM(CASE WHEN PublishedAtUtc IS NULL THEN 1 ELSE 0 END), 0) AS PendingPublishRows
FROM [ConferenceProFlowRegistration].[dbo].[AnalyticsOutboxMessages]
WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(Payload, '$.sessionId')) = @SessionId;

-- 3) Live and replay projections, side by side. Expected after replay: two rows, each 2 / 1.
SELECT 'LIVE' AS Projection, SessionId, ConfirmedCount, RejectedCount, UpdatedAtUtc
FROM [ConferenceProFlowAnalytics].[dbo].[SessionRegistrationStats]
WHERE SessionId = @SessionId
UNION ALL
SELECT 'REPLAY', SessionId, ConfirmedCount, RejectedCount, UpdatedAtUtc
FROM [ConferenceProFlowAnalyticsReplay].[dbo].[SessionRegistrationStats]
WHERE SessionId = @SessionId;

-- 4) Explicit projection comparison. Before replay, MISSING_REPLAY is EXPECTED.
SELECT
    @SessionId AS SessionId,
    live.ConfirmedCount AS LiveConfirmed,
    replay.ConfirmedCount AS ReplayConfirmed,
    live.RejectedCount AS LiveRejected,
    replay.RejectedCount AS ReplayRejected,
    CASE
        WHEN live.SessionId IS NULL THEN 'MISSING_LIVE'
        WHEN replay.SessionId IS NULL THEN 'MISSING_REPLAY'
        WHEN live.ConfirmedCount = replay.ConfirmedCount
         AND live.RejectedCount = replay.RejectedCount THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS ProjectionComparison
FROM (SELECT 1 AS Anchor) AS anchor
LEFT JOIN [ConferenceProFlowAnalytics].[dbo].[SessionRegistrationStats] AS live
    ON live.SessionId = @SessionId
LEFT JOIN [ConferenceProFlowAnalyticsReplay].[dbo].[SessionRegistrationStats] AS replay
    ON replay.SessionId = @SessionId;

-- 5) Processed-event ledger counts. Expected: 3 LIVE and 3 REPLAY after completion.
SELECT 'LIVE' AS Projection,
       COUNT(*) AS ProcessedEventCount,
       COALESCE(SUM(CASE WHEN Status = N'confirmed' THEN 1 ELSE 0 END), 0) AS Confirmed,
       COALESCE(SUM(CASE WHEN Status = N'rejected' THEN 1 ELSE 0 END), 0) AS Rejected
FROM [ConferenceProFlowAnalytics].[dbo].[ProcessedAnalyticsEvents]
WHERE SessionId = @SessionId
UNION ALL
SELECT 'REPLAY', COUNT(*),
       COALESCE(SUM(CASE WHEN Status = N'confirmed' THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN Status = N'rejected' THEN 1 ELSE 0 END), 0)
FROM [ConferenceProFlowAnalyticsReplay].[dbo].[ProcessedAnalyticsEvents]
WHERE SessionId = @SessionId;

-- 6) Exact identity comparison, not just matching aggregate totals.
-- Expected AFTER replay: exactly three rows, all EventComparison = MATCH.
WITH LiveEvents AS
(
    SELECT MessageId, RegistrationId, SessionId, Status, OccurredAtUtc
    FROM [ConferenceProFlowAnalytics].[dbo].[ProcessedAnalyticsEvents]
    WHERE SessionId = @SessionId
),
ReplayEvents AS
(
    SELECT MessageId, RegistrationId, SessionId, Status, OccurredAtUtc
    FROM [ConferenceProFlowAnalyticsReplay].[dbo].[ProcessedAnalyticsEvents]
    WHERE SessionId = @SessionId
)
SELECT
    COALESCE(l.MessageId, r.MessageId) AS MessageId,
    l.RegistrationId AS LiveRegistrationId,
    r.RegistrationId AS ReplayRegistrationId,
    l.Status AS LiveStatus,
    r.Status AS ReplayStatus,
    CASE
        WHEN l.MessageId IS NULL THEN 'ONLY_IN_REPLAY'
        WHEN r.MessageId IS NULL THEN 'ONLY_IN_LIVE'
        WHEN l.RegistrationId = r.RegistrationId
         AND l.SessionId = r.SessionId
         AND l.Status = r.Status
         AND l.OccurredAtUtc = r.OccurredAtUtc THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS EventComparison
FROM LiveEvents AS l
FULL OUTER JOIN ReplayEvents AS r ON l.MessageId = r.MessageId
ORDER BY MessageId;

-- 7) Global quarantine totals are DIAGNOSTIC ONLY and may differ because of
-- the replay snapshot and Kafka retention; do not require them to match.
SELECT 'LIVE' AS Projection, COUNT(*) AS QuarantinedRecords
FROM [ConferenceProFlowAnalytics].[dbo].[QuarantinedAnalyticsEvents]
UNION ALL
SELECT 'REPLAY', COUNT(*)
FROM [ConferenceProFlowAnalyticsReplay].[dbo].[QuarantinedAnalyticsEvents];
