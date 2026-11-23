using System.Data;
using System.Globalization;
using ConferenceProFlow.Analytics.Worker.Domain.Entities;
using ConferenceProFlow.Analytics.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Analytics.Worker.Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace ConferenceProFlow.Analytics.Worker.Application.Processing;

/// <summary>
/// M3.4D: SQL-only, durable idempotent processing. This is intentionally NOT
/// wired to Kafka yet. M3.4E will add record validation/quarantine and commit
/// Kafka offsets only after this method (or quarantine) has completed durably.
///
/// Two transaction-scoped SQL Server application locks enforce the same
/// MessageId/RegistrationId decisions across competing workers. A third lock
/// serializes first-insert and increments for the same SessionId. Independent
/// SQL primary/unique constraints remain the final integrity boundary.
/// </summary>
public sealed class AnalyticsEventProcessingService(AnalyticsDbContext db)
{
    public async Task<AnalyticsProcessingResult> ProcessAsync(
        AnalyticsRegistrationFinalizedV1 integrationEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);
        integrationEvent.Validate();

        // Explicit transaction is essential: ExecuteUpdate does NOT implicitly
        // share a transaction with SaveChanges. We must atomically persist both
        // the deduplication ledger and the aggregate counter change.
        await using var transaction = await db.Database.BeginTransactionAsync(
            IsolationLevel.ReadCommitted, cancellationToken);

        // Fixed lock order prevents message/registration/session lock cycles.
        // Locks are released automatically when the transaction ends.
        await AcquireTransactionLockAsync($"analytics-message:{integrationEvent.MessageId:N}", cancellationToken);
        await AcquireTransactionLockAsync($"analytics-registration:{integrationEvent.RegistrationId:N}", cancellationToken);

        var byMessageId = await db.ProcessedAnalyticsEvents.AsNoTracking()
            .SingleOrDefaultAsync(x => x.MessageId == integrationEvent.MessageId, cancellationToken);

        if (byMessageId is not null)
        {
            return SameBusinessFields(byMessageId, integrationEvent)
                ? new AnalyticsProcessingResult(AnalyticsProcessingOutcome.DuplicateMessage)
                : new AnalyticsProcessingResult(AnalyticsProcessingOutcome.Conflict,
                    "Existing messageId has a different business payload.");
        }

        var byRegistrationId = await db.ProcessedAnalyticsEvents.AsNoTracking()
            .SingleOrDefaultAsync(x => x.RegistrationId == integrationEvent.RegistrationId, cancellationToken);

        if (byRegistrationId is not null)
        {
            return SameBusinessFields(byRegistrationId, integrationEvent)
                ? new AnalyticsProcessingResult(AnalyticsProcessingOutcome.DuplicateBusinessEvent)
                : new AnalyticsProcessingResult(AnalyticsProcessingOutcome.Conflict,
                    "Existing registrationId has a different finalization payload.");
        }

        await AcquireTransactionLockAsync($"analytics-session:{integrationEvent.SessionId:N}", cancellationToken);

        var now = DateTimeOffset.UtcNow;
        db.ProcessedAnalyticsEvents.Add(new ProcessedAnalyticsEvent
        {
            MessageId = integrationEvent.MessageId,
            RegistrationId = integrationEvent.RegistrationId,
            SessionId = integrationEvent.SessionId,
            Status = integrationEvent.Status,
            OccurredAtUtc = integrationEvent.OccurredAtUtc,
            ProcessedAtUtc = now
        });

        // Persist the ledger first INSIDE the transaction. A subsequent
        // aggregate failure must roll this insert back as well.
        await db.SaveChangesAsync(cancellationToken);

        var confirmedDelta = integrationEvent.Status == AnalyticsRegistrationFinalizedV1.ConfirmedStatus ? 1L : 0L;
        var rejectedDelta = integrationEvent.Status == AnalyticsRegistrationFinalizedV1.RejectedStatus ? 1L : 0L;

        // One SQL UPDATE performs the increment (no unsafe EF tracked
        // read/modify/write). The per-session lock protects first-row creation.
        var updated = await db.SessionRegistrationStats
            .Where(x => x.SessionId == integrationEvent.SessionId)
            .ExecuteUpdateAsync(setters => setters
                .SetProperty(x => x.ConfirmedCount, x => x.ConfirmedCount + confirmedDelta)
                .SetProperty(x => x.RejectedCount, x => x.RejectedCount + rejectedDelta)
                .SetProperty(x => x.UpdatedAtUtc, now), cancellationToken);

        if (updated == 0)
        {
            db.SessionRegistrationStats.Add(new SessionRegistrationStat
            {
                SessionId = integrationEvent.SessionId,
                ConfirmedCount = confirmedDelta,
                RejectedCount = rejectedDelta,
                UpdatedAtUtc = now
            });
            await db.SaveChangesAsync(cancellationToken);
        }
        else if (updated != 1)
        {
            throw new InvalidOperationException(
                $"Unexpected session statistics update count: {updated}.");
        }

        await transaction.CommitAsync(cancellationToken);
        return new AnalyticsProcessingResult(AnalyticsProcessingOutcome.Applied);
    }

    private static bool SameBusinessFields(
        ProcessedAnalyticsEvent existing, AnalyticsRegistrationFinalizedV1 incoming) =>
        existing.RegistrationId == incoming.RegistrationId &&
        existing.SessionId == incoming.SessionId &&
        string.Equals(existing.Status, incoming.Status, StringComparison.Ordinal) &&
        existing.OccurredAtUtc == incoming.OccurredAtUtc;

    private async Task AcquireTransactionLockAsync(string resource, CancellationToken cancellationToken)
    {
        // sp_getapplock is transactional and applies across SQL connections
        // targeting this Analytics database, not just inside one .NET process.
        var connection = db.Database.GetDbConnection();
        await using var command = connection.CreateCommand();
        command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction()
            ?? throw new InvalidOperationException("An active SQL transaction is required.");
        command.CommandText = """
            DECLARE @lockResult int;
            EXEC @lockResult = sys.sp_getapplock
                @Resource = @resource,
                @LockMode = N'Exclusive',
                @LockOwner = N'Transaction',
                @LockTimeout = 15000;
            SELECT @lockResult;
            """;
        command.CommandTimeout = 20;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@resource";
        parameter.DbType = DbType.String;
        parameter.Size = 100;
        parameter.Value = resource;
        command.Parameters.Add(parameter);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        var result = Convert.ToInt32(value, CultureInfo.InvariantCulture);
        if (result < 0)
        {
            throw new TimeoutException(
                $"SQL Server could not acquire Analytics transaction lock (code {result}).");
        }
    }
}
