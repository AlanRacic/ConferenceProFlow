-- M4.2D3 — READ-ONLY E2E diagnostics across THREE local service-owned databases.
-- Visual Studio -> SQL Server Object Explorer -> New Query (on local SQL 127.0.0.1,14333).
-- Replace the placeholder on the next line with the fresh SessionId from HTTP request 02.
-- These cross-DB queries are HUMAN diagnostics; no application may query another service's DB.
-- No INSERT / UPDATE / DELETE / schema changes / Kafka offset modification.
SET NOCOUNT ON;
DECLARE @SessionIdText nvarchar(36) = N'PASTE-FRESH-SESSION-GUID-HERE';
DECLARE @SessionId uniqueidentifier = TRY_CONVERT(uniqueidentifier, @SessionIdText);
IF @SessionId IS NULL
    THROW 51001, 'Replace PASTE-FRESH-SESSION-GUID-HERE with the fresh session GUID.', 1;

-- A. Authoritative Registration: expect 3 rows, 0 Pending, 2 Confirmed, 1 Rejected.
SELECT
    COUNT(*) AS RegistrationTotal,
    SUM(CASE WHEN Status = 0 THEN 1 ELSE 0 END) AS Pending,
    SUM(CASE WHEN Status = 1 THEN 1 ELSE 0 END) AS Confirmed,
    SUM(CASE WHEN Status = 2 THEN 1 ELSE 0 END) AS Rejected
FROM [ConferenceProFlowRegistration].[dbo].[Registrations]
WHERE SessionId = @SessionId;

-- B. Capacity-safe result: expect Capacity=2 and ReservedCount=2.
SELECT SessionId, Capacity, ReservedCount
FROM [ConferenceProFlowAllocation].[dbo].[SessionInventories]
WHERE SessionId = @SessionId;

-- C. Registration request Outbox: 3 rows, PublishedAtUtc non-NULL.
-- TraceParent is W3C context captured during the original HTTP request.
SELECT Id AS MessageId, PublishedAtUtc, PublishAttempts, TraceParent, TraceState,
       CASE WHEN TraceParent IS NOT NULL AND LEN(TraceParent)=55 THEN SUBSTRING(TraceParent,4,32) END AS PersistedTraceId
FROM [ConferenceProFlowRegistration].[dbo].[OutboxMessages]
WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(Payload, '$.sessionId')) = @SessionId
ORDER BY OccurredAtUtc, Id;

-- D. Allocation Inbox: 3 received/processed rows; tracing context survives SQL boundary.
SELECT i.Id AS MessageId, i.RegistrationId, i.ProcessedAtUtc,
       i.TraceParent, i.TraceState,
       CASE a.Status WHEN 1 THEN 'Confirmed' WHEN 2 THEN 'Rejected' ELSE 'Missing' END AS AllocationDecision
FROM [ConferenceProFlowAllocation].[dbo].[InboxMessages] AS i
LEFT JOIN [ConferenceProFlowAllocation].[dbo].[Allocations] AS a
  ON a.RegistrationId = i.RegistrationId
WHERE i.SessionId = @SessionId
ORDER BY i.ReceivedAtUtc, i.Id;

-- E. Allocation Result Outbox: 3 published records with W3C context where available.
SELECT o.Id AS MessageId, o.AllocationId, o.PublishedAtUtc,
       o.TraceParent, o.TraceState, o.PublishAttempts
FROM [ConferenceProFlowAllocation].[dbo].[AllocationOutboxMessages] AS o
WHERE o.AllocationId IN
      (SELECT a.Id FROM [ConferenceProFlowAllocation].[dbo].[Allocations] AS a
       WHERE a.SessionId = @SessionId)
ORDER BY o.OccurredAtUtc, o.Id;

-- F. Registration result Inbox: 3 durable decisions; registration/reconciliation completed.
SELECT COUNT(*) AS RegistrationResultInboxTotal
FROM [ConferenceProFlowRegistration].[dbo].[AllocationResultInboxMessages]
WHERE SessionId = @SessionId;

-- G. Registration-owned Analytics Outbox: 3 published Kafka events + trace context.
SELECT Id AS MessageId, RegistrationId, PublishedAtUtc,
       TraceParent, TraceState, PublishAttempts
FROM [ConferenceProFlowRegistration].[dbo].[AnalyticsOutboxMessages]
WHERE TRY_CONVERT(uniqueidentifier, JSON_VALUE(Payload, '$.sessionId')) = @SessionId
ORDER BY OccurredAtUtc, Id;

-- H. Analytics materialized projection: expect 2 Confirmed, 1 Rejected.
SELECT SessionId, ConfirmedCount, RejectedCount, UpdatedAtUtc
FROM [ConferenceProFlowAnalytics].[dbo].[SessionRegistrationStats]
WHERE SessionId = @SessionId;

-- I. Analytics idempotency ledger: expect 3 distinct processed events.
SELECT MessageId, RegistrationId, SessionId, Status, ProcessedAtUtc
FROM [ConferenceProFlowAnalytics].[dbo].[ProcessedAnalyticsEvents]
WHERE SessionId = @SessionId
ORDER BY ProcessedAtUtc, MessageId;
