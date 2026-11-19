namespace ConferenceProFlow.Registration.Api.Messaging.Contracts;

public static class AllocationDecidedV1Validator
{
    public static string? GetError(AllocationDecidedV1 message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message.MessageId == Guid.Empty || message.AllocationId == Guid.Empty ||
            message.RegistrationId == Guid.Empty || message.SessionId == Guid.Empty)
        {
            return "MessageId, AllocationId, RegistrationId, and SessionId must not be empty.";
        }

        if (message.OccurredAtUtc == default || message.OccurredAtUtc.Offset != TimeSpan.Zero)
        {
            return "OccurredAtUtc must be a valid UTC timestamp.";
        }

        if (message.Status == AllocationDecidedV1.ConfirmedStatus)
        {
            return message.RejectionReason is null
                ? null
                : "Confirmed results must not have a rejection reason.";
        }

        if (message.Status == AllocationDecidedV1.RejectedStatus)
        {
            return string.IsNullOrWhiteSpace(message.RejectionReason) || message.RejectionReason.Trim().Length > 500
                ? "Rejected results require a reason of at most 500 characters."
                : null;
        }

        return "Status must be 'confirmed' or 'rejected'.";
    }
}