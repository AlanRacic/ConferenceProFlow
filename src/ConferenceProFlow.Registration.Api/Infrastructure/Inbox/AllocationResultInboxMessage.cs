using ConferenceProFlow.Registration.Api.Messaging.Contracts;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Inbox;

/// <summary>
/// A durable record of a business decision applied to a Registration.
/// Id = integration event MessageId, with additional unique constraints on AllocationId and RegistrationId to prevent conflicting business decisions.
/// </summary>
public sealed class AllocationResultInboxMessage
{
    private AllocationResultInboxMessage() { } // EF Core

    public Guid Id { get; private set; }
    public Guid AllocationId { get; private set; }
    public Guid RegistrationId { get; private set; }
    public Guid SessionId { get; private set; }
    public string Status { get; private set; } = string.Empty;
    public string? RejectionReason { get; private set; }
    public DateTimeOffset OccurredAtUtc { get; private set; }
    public DateTimeOffset ReceivedAtUtc { get; private set; }

    public static AllocationResultInboxMessage Create(AllocationDecidedV1 result, DateTimeOffset receivedAtUtc)
    {
        var error = AllocationDecidedV1Validator.GetError(result);
        if (error is not null)
            throw new ArgumentException(error, nameof(result));

        if (receivedAtUtc == default || receivedAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("ReceivedAtUtc must be a valid UTC timestamp.", nameof(receivedAtUtc));

        return new AllocationResultInboxMessage
        {
            Id = result.MessageId,
            AllocationId = result.AllocationId,
            RegistrationId = result.RegistrationId,
            SessionId = result.SessionId,
            Status = result.Status,
            RejectionReason = result.RejectionReason,
            OccurredAtUtc = result.OccurredAtUtc,
            ReceivedAtUtc = receivedAtUtc
        };
    }

    public bool Matches(AllocationDecidedV1 result) =>
        Id == result.MessageId &&
        AllocationId == result.AllocationId &&
        RegistrationId == result.RegistrationId &&
        SessionId == result.SessionId &&
        Status == result.Status &&
        RejectionReason == result.RejectionReason &&
        OccurredAtUtc == result.OccurredAtUtc;

    public bool MatchesBusinessDecision(AllocationDecidedV1 result) =>
        AllocationId == result.AllocationId &&
        RegistrationId == result.RegistrationId &&
        SessionId == result.SessionId &&
        Status == result.Status &&
        RejectionReason == result.RejectionReason &&
        OccurredAtUtc == result.OccurredAtUtc;
}