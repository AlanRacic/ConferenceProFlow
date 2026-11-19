namespace ConferenceProFlow.Allocation.Worker.Messaging.Contracts;

/// <summary>
/// Versioned integration event announcing an Allocation service business decision.
/// A durable outbox publisher and Registration-side consumer will be added in subsequent M2.6 phases.
/// </summary>
public sealed record AllocationDecidedV1(
    Guid MessageId,
    Guid AllocationId,
    Guid RegistrationId,
    Guid SessionId,
    string Status,
    string? RejectionReason,
    DateTimeOffset OccurredAtUtc)
{
    public const string MessageType = "allocation.decided.v1";

    public const string ConfirmedStatus = "confirmed";

    public const string RejectedStatus = "rejected";
}
