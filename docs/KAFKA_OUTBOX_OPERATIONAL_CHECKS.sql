-- M3.6E2 | Registration Analytics Outbox operational diagnostics.
-- READ ONLY: run in ConferenceProFlowRegistration in Visual Studio SQL Server Object Explorer.
-- No UPDATE, DELETE, INSERT, schema changes, or resetting offsets.

-- 1. Distribution of durable publication states.
SELECT
    CASE
        WHEN PublishedAtUtc IS NOT NULL THEN 'Published'
        WHEN QuarantinedAtUtc IS NOT NULL THEN 'Quarantined (existing state)'
        WHEN PublishAttempts >= 5 THEN 'Needs attention (>=5 attempts)'
        WHEN NextAttemptAtUtc IS NOT NULL THEN 'Retry scheduled'
        ELSE 'Pending'
    END AS PublicationState,
    COUNT(*) AS MessageCount
FROM dbo.AnalyticsOutboxMessages
GROUP BY
    CASE
        WHEN PublishedAtUtc IS NOT NULL THEN 'Published'
        WHEN QuarantinedAtUtc IS NOT NULL THEN 'Quarantined (existing state)'
        WHEN PublishAttempts >= 5 THEN 'Needs attention (>=5 attempts)'
        WHEN NextAttemptAtUtc IS NOT NULL THEN 'Retry scheduled'
        ELSE 'Pending'
    END
ORDER BY PublicationState;

-- 2. Pending messages needing operator attention.
-- Align @AlertAfterAttempts with KafkaAnalyticsPublisher:AlertAfterAttempts (default: 5).
DECLARE @AlertAfterAttempts int = 5;
SELECT TOP (50)
    Id AS MessageId,
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
  AND QuarantinedAtUtc IS NULL
  AND PublishAttempts >= @AlertAfterAttempts
ORDER BY PublishAttempts DESC, OccurredAtUtc ASC;

-- 3. All pending and retry-scheduled records (including first attempts).
SELECT TOP (50)
    Id AS MessageId,
    RegistrationId,
    OccurredAtUtc,
    PublishAttempts,
    NextAttemptAtUtc,
    LastError
FROM dbo.AnalyticsOutboxMessages
WHERE PublishedAtUtc IS NULL AND QuarantinedAtUtc IS NULL
ORDER BY OccurredAtUtc ASC, Id ASC;
