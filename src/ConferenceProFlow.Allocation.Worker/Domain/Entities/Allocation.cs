using ConferenceProFlow.Allocation.Worker.Domain.Enums;

namespace ConferenceProFlow.Allocation.Worker.Domain.Entities;

public sealed class Allocation
{
    private Allocation()
    {
        // Required by EF Core.
    }

    public Guid Id { get; private set; }

    public Guid RegistrationId { get; private set; }

    public Guid SessionId { get; private set; }

    public AllocationStatus Status { get; private set; }

    public string? RejectionReason { get; private set; }

    public DateTimeOffset CreatedAtUtc { get; private set; }

    public static Allocation CreateConfirmed(Guid registrationId, Guid sessionId, DateTimeOffset createdAtUtc)
    {
        return Create(
            registrationId,
            sessionId,
            AllocationStatus.Confirmed,
            null,
            createdAtUtc);
    }

    public static Allocation CreateRejected(Guid registrationId, Guid sessionId, string reason, DateTimeOffset createdAtUtc)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new ArgumentException("Rejection reason is required.", nameof(reason));
        }

        var normalizedReason = reason.Trim();

        if (normalizedReason.Length > 500)
        {
            throw new ArgumentException("Rejection reason must not exceed 500 characters.", nameof(reason));
        }

        return Create(
            registrationId,
            sessionId,
            AllocationStatus.Rejected,
            normalizedReason,
            createdAtUtc);
    }

    private static Allocation Create(
        Guid registrationId,
        Guid sessionId,
        AllocationStatus status,
        string? rejectionReason,
        DateTimeOffset createdAtUtc)
    {
        if (registrationId == Guid.Empty)
        {
            throw new ArgumentException("RegistrationId must not be empty.", nameof(registrationId));
        }

        if (sessionId == Guid.Empty)
        {
            throw new ArgumentException("SessionId must not be empty.", nameof(sessionId));
        }

        if (createdAtUtc == default || createdAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("CreatedAtUtc must be a valid UTC timestamp.", nameof(createdAtUtc));
        }

        return new Allocation
        {
            Id = Guid.NewGuid(),
            RegistrationId = registrationId,
            SessionId = sessionId,
            Status = status,
            RejectionReason = rejectionReason,
            CreatedAtUtc = createdAtUtc
        };
    }
}