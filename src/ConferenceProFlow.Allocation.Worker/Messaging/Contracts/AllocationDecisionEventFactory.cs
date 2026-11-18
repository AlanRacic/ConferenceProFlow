using ConferenceProFlow.Allocation.Worker.Domain.Enums;
using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;

namespace ConferenceProFlow.Allocation.Worker.Messaging.Contracts;

/// <summary>
/// Maps a persisted allocation decision to the versioned integration event.
/// The MessageId is generated once by the caller and later persisted in the Allocation Outbox (M2.6B), so publication retries reuse the same ID.
/// </summary>
public static class AllocationDecisionEventFactory
{
    public static AllocationDecidedV1 Create(
        AllocationEntity allocation,
        Guid messageId,
        DateTimeOffset occurredAtUtc)
    {
        ArgumentNullException.ThrowIfNull(allocation);

        if (messageId == Guid.Empty)
        {
            throw new ArgumentException("MessageId must not be empty.", nameof(messageId));
        }

        if (occurredAtUtc == default || occurredAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("OccurredAtUtc must be a valid UTC timestamp.", nameof(occurredAtUtc));
        }

        var status = allocation.Status switch
        {
            AllocationStatus.Confirmed => AllocationDecidedV1.ConfirmedStatus,
            AllocationStatus.Rejected => AllocationDecidedV1.RejectedStatus,
            _ => throw new InvalidOperationException("Unsupported allocation status.")
        };

        return new AllocationDecidedV1(
            messageId,
            allocation.Id,
            allocation.RegistrationId,
            allocation.SessionId,
            status,
            allocation.RejectionReason,
            occurredAtUtc);
    }
}