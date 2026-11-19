using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using ConferenceProFlow.Allocation.Worker.Messaging.Outbox;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.Worker.Processing;

public sealed class AllocationProcessingService
{
    private const string CapacityReachedReason = "Session capacity reached.";

    // Retry the whole decision after a recognized concurrency conflict, never only the stale SaveChanges call.
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
            catch (Exception exception) when (
                attempt < MaxProcessingAttempts &&
                IsRetryableConcurrencyFailure(exception))
            {
                // Failed SaveChanges is rolled back; remove stale entities and the uncommitted outbox entry before calculating a fresh decision.
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
            .SingleOrDefaultAsync(x => x.Id == messageId, cancellationToken);

        if (inboxMessage is null)
        {
            return AllocationProcessingResult.NotFound;
        }

        if (inboxMessage.ProcessedAtUtc is not null)
        {
            // Existing allocations from before M2.6 are intentionally not auto-backfilled here; see M2.6B documentation.
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

            // Recovery: an allocation already exists. Do not reserve another seat.
            // Ensure its result event exists before completing the pending Inbox.
            var hasOutboxEvent = await _db.AllocationOutboxMessages
                .AnyAsync(
                    x => x.AllocationId == existingAllocation.Id,
                    cancellationToken);

            if (!hasOutboxEvent)
            {
                _db.AllocationOutboxMessages.Add(AllocationOutboxMessageFactory.Create(existingAllocation));
            }

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
            // Missing inventory is not an allocation rejection.
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
        _db.AllocationOutboxMessages.Add(
            AllocationOutboxMessageFactory.Create(allocation));
        inboxMessage.MarkProcessed(processedAtUtc);

        // ONE SaveChanges transaction for inventory, allocation, inbox and outbox.
        // The outbox event remains pending for the publisher introduced in M2.6C.
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