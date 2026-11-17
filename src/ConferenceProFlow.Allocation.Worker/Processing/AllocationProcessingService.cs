using ConferenceProFlow.Allocation.Worker.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.Worker.Processing;

public sealed class AllocationProcessingService
{
    private const string CapacityReachedReason = "Session capacity reached.";

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

        // A previous or manually recovered allocation must not reserve a second seat.
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
            // An unknown capacity is not the same as a full session.
            // Keep the Inbox message pending until inventory is configured.
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

        // One SaveChanges call is atomic on SQL Server by default.
        // A concurrency or database failure must leave the whole operation uncommitted.
        await _db.SaveChangesAsync(cancellationToken);

        return reserved
            ? AllocationProcessingResult.Confirmed
            : AllocationProcessingResult.Rejected;
    }
}