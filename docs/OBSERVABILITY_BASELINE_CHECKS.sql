/*
ConferenceProFlow — M4.1 read-only, retrospective baseline inspection.
Run in Visual Studio SQL Server Object Explorer connected to ConferenceProFlowRegistration.
No schema changes, inserts, updates, deletes or migrations.
Use a fresh demo SessionId if you wish to restrict to one session; otherwise keep NULL.
This is NOT a timed load/performance benchmark. SQL values are historical proxies.
*/

DECLARE @SessionId uniqueidentifier = NULL; -- e.g. 'paste-a-real-session-guid-here'

-- 1. Registration statuses for the selected scope. Database enum: Pending=0, Confirmed=1, Rejected=2.
SELECT
    CASE Status WHEN 0 THEN 'Pending' WHEN 1 THEN 'Confirmed' WHEN 2 THEN 'Rejected' ELSE 'Unexpected' END AS RegistrationStatus,
    COUNT_BIG(*) AS RegistrationCount
FROM dbo.Registrations
WHERE @SessionId IS NULL OR SessionId = @SessionId
GROUP BY Status
ORDER BY Status;

-- 2. Historical finalization delay using stored entity timestamps.
-- Only FINAL registrations; this is NOT HTTP request duration or a measured trace span.
WITH Finalized AS
(
    SELECT CAST(DATEDIFF_BIG(MILLISECOND, CreatedAtUtc, UpdatedAtUtc) AS float) AS DurationMs
    FROM dbo.Registrations
    WHERE Status IN (1, 2)
      AND UpdatedAtUtc >= CreatedAtUtc
      AND (@SessionId IS NULL OR SessionId = @SessionId)
)
SELECT
    COUNT_BIG(*) AS FinalizedCount,
    CAST(AVG(DurationMs) AS decimal(18,2)) AS AverageFinalizationMs,
    CAST(MIN(DurationMs) AS decimal(18,2)) AS MinFinalizationMs,
    CAST(MAX(DurationMs) AS decimal(18,2)) AS MaxFinalizationMs
FROM Finalized;

-- 3. Approximate historical p50/p95 from the same timestamp proxy.
-- Output is empty if there are no finalized registrations.
WITH Finalized AS
(
    SELECT CAST(DATEDIFF_BIG(MILLISECOND, CreatedAtUtc, UpdatedAtUtc) AS float) AS DurationMs
    FROM dbo.Registrations
    WHERE Status IN (1, 2)
      AND UpdatedAtUtc >= CreatedAtUtc
      AND (@SessionId IS NULL OR SessionId = @SessionId)
), Percentiles AS
(
    SELECT
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DurationMs) OVER () AS P50Ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DurationMs) OVER () AS P95Ms
    FROM Finalized
)
SELECT TOP (1)
    CAST(P50Ms AS decimal(18,2)) AS P50FinalizationMs,
    CAST(P95Ms AS decimal(18,2)) AS P95FinalizationMs
FROM Percentiles;

-- 4. Kafka Analytics Outbox state (all messages, no dependency on JSON extraction).
SELECT
    COUNT_BIG(*) AS TotalAnalyticsOutboxMessages,
    SUM(CASE WHEN PublishedAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS PublishedCount,
    SUM(CASE WHEN PublishedAtUtc IS NULL AND QuarantinedAtUtc IS NULL THEN 1 ELSE 0 END) AS PendingCount,
    SUM(CASE WHEN QuarantinedAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS QuarantinedCount,
    SUM(CASE WHEN PublishedAtUtc IS NULL AND PublishAttempts >= 5 THEN 1 ELSE 0 END) AS UnpublishedFiveOrMoreAttempts,
    MAX(PublishAttempts) AS MaxPublishAttempts
FROM dbo.AnalyticsOutboxMessages;

-- 5. Oldest pending Kafka Analytics Outbox record (if any).
SELECT TOP (10)
    Id, RegistrationId, OccurredAtUtc, PublishAttempts, NextAttemptAtUtc, PublishedAtUtc, QuarantinedAtUtc
FROM dbo.AnalyticsOutboxMessages
WHERE PublishedAtUtc IS NULL AND QuarantinedAtUtc IS NULL
ORDER BY OccurredAtUtc ASC;
