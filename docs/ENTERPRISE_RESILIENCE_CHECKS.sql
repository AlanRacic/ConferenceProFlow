-- ConferenceProFlow M3.6B — read-only Analytics Outbox health review.
-- Run in Visual Studio -> SQL Server Object Explorer, connected to
-- ConferenceProFlowRegistration (NOT master or AnalyticsReplay).
-- Do NOT publish connection strings/credentials in screenshots or logs.

-- 1. Aggregate current publication state.
SELECT
    COUNT_BIG(*) AS TotalEvents,
    SUM(CASE WHEN PublishedAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS Published,
    SUM(CASE WHEN PublishedAtUtc IS NULL AND QuarantinedAtUtc IS NULL THEN 1 ELSE 0 END) AS Pending,
    SUM(CASE WHEN QuarantinedAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS Quarantined,
    MAX(PublishAttempts) AS MaxPublishAttempts
FROM dbo.AnalyticsOutboxMessages;

-- 2. Pending retries, sorted by severity / age (bounded sample).
SELECT TOP (30)
    Id,
    RegistrationId,
    MessageType,
    OccurredAtUtc,
    PublishAttempts,
    NextAttemptAtUtc,
    LastError,
    PublishedAtUtc,
    QuarantinedAtUtc
FROM dbo.AnalyticsOutboxMessages
WHERE PublishedAtUtc IS NULL
ORDER BY PublishAttempts DESC, OccurredAtUtc ASC;

-- 3. Confirm that no row is simultaneously marked published and quarantined.
SELECT COUNT_BIG(*) AS PublishedAndQuarantined
FROM dbo.AnalyticsOutboxMessages
WHERE PublishedAtUtc IS NOT NULL AND QuarantinedAtUtc IS NOT NULL;

-- These are diagnostics. A non-zero Pending count does not alone prove a bug:
-- an event may be waiting for Kafka, a retry interval, or operator review.
