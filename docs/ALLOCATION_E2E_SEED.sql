-- ConferenceProFlow M2.5 — development-only session inventory provisioning.
-- Run AFTER creating the demo session in docs/ALLOCATION_E2E.http
-- and BEFORE creating its first registration.
-- Connect to the local development SQL Server: 127.0.0.1,14333.
-- Never run this script against production databases.

USE [ConferenceProFlowAllocation];
GO

SET NOCOUNT ON;

-- Paste `id` from the `demoCreateSession` HTTP response below.
DECLARE @SessionIdText nvarchar(36) = N'32d59e23-726f-4366-bc8a-083f26f764c0';
DECLARE @SessionId uniqueidentifier = TRY_CONVERT(uniqueidentifier, @SessionIdText);
DECLARE @Capacity int = 2;

IF @SessionId IS NULL
    THROW 51001, 'Replace PASTE-SESSION-GUID-HERE with the real demo session GUID.', 1;

IF @Capacity <= 0
    THROW 51002, 'Capacity must be positive.', 1;

IF EXISTS (SELECT 1 FROM dbo.SessionInventories WHERE SessionId = @SessionId)
    THROW 51003, 'An inventory row already exists for this session. Use a fresh demo session ID.', 1;

INSERT INTO dbo.SessionInventories
    (SessionId, Capacity, ReservedCount, CreatedAtUtc)
VALUES
    (@SessionId, @Capacity, 0, TODATETIMEOFFSET(SYSUTCDATETIME(), '+00:00'));

-- SQL Server generates the RowVersion automatically. Do not set it manually.
SELECT SessionId, Capacity, ReservedCount, CreatedAtUtc, RowVersion
FROM dbo.SessionInventories
WHERE SessionId = @SessionId;
