using ConferenceProFlow.Registration.Api.Domain.Enums;

namespace ConferenceProFlow.Registration.Api.Domain.Entities;

public sealed class Registration
{
    private Registration()
    {
        // Required for EF Core materialization.
    }

    public Guid Id { get; private set; }

    public Guid ParticipantId { get; private set; }

    public Guid SessionId { get; private set; }

    public RegistrationStatus Status { get; private set; }

    public DateTimeOffset CreatedAtUtc { get; private set; }

    public DateTimeOffset UpdatedAtUtc { get; private set; }

    public string? RejectionReason { get; private set; }

    public static Registration Create(Guid participantId, Guid sessionId, DateTimeOffset createdAtUtc)
    {
        if (participantId == Guid.Empty)
            throw new ArgumentException("ParticipantId cannot be empty.", nameof(participantId));

        if (sessionId == Guid.Empty)
            throw new ArgumentException("SessionId cannot be empty.", nameof(sessionId));

        EnsureUtc(createdAtUtc);

        return new Registration
        {
            Id = Guid.NewGuid(),
            ParticipantId = participantId,
            SessionId = sessionId,
            Status = RegistrationStatus.Pending,
            CreatedAtUtc = createdAtUtc,
            UpdatedAtUtc = createdAtUtc
        };
    }

    public void Confirm(DateTimeOffset updatedAtUtc)
    {
        if (Status == RegistrationStatus.Confirmed)
            return;

        if (Status != RegistrationStatus.Pending)
            throw new InvalidOperationException("Only pending registrations can be confirmed.");

        ValidateUpdateTime(updatedAtUtc);

        Status = RegistrationStatus.Confirmed;
        UpdatedAtUtc = updatedAtUtc;
    }

    public void Reject(string reason, DateTimeOffset updatedAtUtc)
    {
        if (string.IsNullOrWhiteSpace(reason))
            throw new ArgumentException("A rejection reason is required.", nameof(reason));

        var normalizedReason = reason.Trim();

        if (Status == RegistrationStatus.Rejected)
        {
            if (RejectionReason == normalizedReason)
                return;

            throw new InvalidOperationException("Registration has already been rejected for a different reason.");
        }

        if (Status != RegistrationStatus.Pending)
            throw new InvalidOperationException("Only pending registrations can be rejected.");

        ValidateUpdateTime(updatedAtUtc);

        Status = RegistrationStatus.Rejected;
        RejectionReason = normalizedReason;
        UpdatedAtUtc = updatedAtUtc;
    }

    private void ValidateUpdateTime(DateTimeOffset updatedAtUtc)
    {
        EnsureUtc(updatedAtUtc);

        if (updatedAtUtc < CreatedAtUtc)
            throw new ArgumentException("Update time cannot precede creation time.", nameof(updatedAtUtc));
    }

    private static void EnsureUtc(DateTimeOffset timestamp)
    {
        if (timestamp.Offset != TimeSpan.Zero)
            throw new ArgumentException("Timestamp must use UTC.");
    }
}