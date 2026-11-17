using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.Worker.Processing;

public sealed class AllocationProcessingService
{
    private const string CapacityReachedReason = "Session capacity reached.";

    // A single attempt may lose a race with another allocation processor.
    // Retry the entire decision, not just the failed SaveChanges call.
    private const int MaxProcessingAttempts = 16;

    private readonly AllocationDbContext _db;

    public AllocationProcessingService(AllocationDbContext db)
    {
        _db = db;
    }

    public async Task<AllocationProcessingResult> ProcessAsync(
        Guid messageId,
        CancellationToken cancellationToken = default)
    {
        if (messageId == Guid.Empty)
        {
            throw new ArgumentException("MessageId must not be empty.", nameof(messageId));
        }

        for (var attempt = 1; attempt <= MaxProcessingAttempts; attempt++)
        {
            try
            {
                return await ProcessAttemptAsync(messageId, cancellationToken);
            }
            catch (Exception exception) when (attempt < MaxProcessingAttempts && IsRetryableConcurrencyFailure(exception))
            {
                // SaveChanges rolls back its transaction on failure. The current DbContext still tracks stale inventory values and an unsaved allocation, so discard them before re-evaluating the request.
                _db.ChangeTracker.Clear();

                var delayMs = Math.Min(attempt * 25 + Random.Shared.Next(10, 40), 300);

                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), cancellationToken);
            }
        }

        // The final attempt propagates its original exception.
        throw new InvalidOperationException("Allocation processing ended without a result.");
    }

    private async Task<AllocationProcessingResult> ProcessAttemptAsync(
        Guid messageId,
        CancellationToken cancellationToken)
    {
        var inboxMessage = await _db.InboxMessages
            .SingleOrDefaultAsync(
                x => x.Id == messageId,
                cancellationToken);

        if (inboxMessage is null)
        {
            return AllocationProcessingResult.NotFound;
        }

        if (inboxMessage.ProcessedAtUtc is not null)
        {
            return AllocationProcessingResult.AlreadyProcessed;
        }

        var existingAllocation = await _db.Allocations
            .AsNoTracking()
            .SingleOrDefaultAsync(
                x => x.RegistrationId == inboxMessage.RegistrationId,
                cancellationToken);

        if (existingAllocation is not null)
        {
            if (existingAllocation.SessionId != inboxMessage.SessionId)
            {
                throw new InvalidOperationException("Existing allocation belongs to a different session.");
            }

            // Recovery path: an allocation already exists, so never reserve another seat. Completing the Inbox is the only required write.
            inboxMessage.MarkProcessed(DateTimeOffset.UtcNow);
            await _db.SaveChangesAsync(cancellationToken);

            return AllocationProcessingResult.ExistingAllocation;
        }

        var inventory = await _db.SessionInventories
            .SingleOrDefaultAsync(
                x => x.SessionId == inboxMessage.SessionId,
                cancellationToken);

        if (inventory is null)
        {
            // Unknown inventory is not the same as a full session.
            // Leave this Inbox record pending until capacity is configured.
            return AllocationProcessingResult.WaitingForInventory;
        }

        var reserved = inventory.TryReserveSeat();
        var processedAtUtc = DateTimeOffset.UtcNow;

        var allocation = reserved
            ? AllocationEntity.CreateConfirmed(
                inboxMessage.RegistrationId,
                inboxMessage.SessionId,
                processedAtUtc)
            : AllocationEntity.CreateRejected(
                inboxMessage.RegistrationId,
                inboxMessage.SessionId,
                CapacityReachedReason,
                processedAtUtc);

        _db.Allocations.Add(allocation);
        inboxMessage.MarkProcessed(processedAtUtc);

        // EF Core commits inventory, allocation and Inbox updates atomically.
        // For confirmed seats, SQL Server RowVersion detects stale inventory.
        await _db.SaveChangesAsync(cancellationToken);

        return reserved
            ? AllocationProcessingResult.Confirmed
            : AllocationProcessingResult.Rejected;
    }

    private static bool IsRetryableConcurrencyFailure(Exception exception)
    {
        if (exception is DbUpdateConcurrencyException)
        {
            return true;
        }

        // Two processors handling the same registration can race on the unique RegistrationId index. SQL deadlocks are also retryable.
        if (exception is DbUpdateException
            {
                InnerException: SqlException sqlException
            })
        {
            return sqlException.Number is 2601 or 2627 or 1205;
        }

        return exception is SqlException { Number: 1205 };
    }
}