namespace ConferenceProFlow.Registration.Api.Messaging.Contracts;

/// <summary>
/// Consumer-side copy of the Allocation service's versioned integration contract.
/// Services do not reference each other's assemblies.
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