namespace ConferenceProFlow.Allocation.Worker.Domain.Entities;

public sealed class InboxMessage
{
    private InboxMessage()
    {
        // Required by EF Core.
    }

    public Guid Id { get; private set; }

    public Guid RegistrationId { get; private set; }

    public Guid SessionId { get; private set; }

    public DateTimeOffset OccurredAtUtc { get; private set; }

    public DateTimeOffset ReceivedAtUtc { get; private set; }

    public DateTimeOffset? ProcessedAtUtc { get; private set; }

    public static InboxMessage Create(
        Guid messageId,
        Guid registrationId,
        Guid sessionId,
        DateTimeOffset occurredAtUtc,
        DateTimeOffset receivedAtUtc)
    {
        if (messageId == Guid.Empty)
        {
            throw new ArgumentException("MessageId must not be empty.", nameof(messageId));
        }

        if (registrationId == Guid.Empty)
        {
            throw new ArgumentException("RegistrationId must not be empty.", nameof(registrationId));
        }

        if (sessionId == Guid.Empty)
        {
            throw new ArgumentException("SessionId must not be empty.", nameof(sessionId));
        }

        if (occurredAtUtc == default || occurredAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("OccurredAtUtc must be a valid UTC timestamp.", nameof(occurredAtUtc));
        }

        if (receivedAtUtc == default || receivedAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("ReceivedAtUtc must be a valid UTC timestamp.", nameof(receivedAtUtc));
        }

        return new InboxMessage
        {
            Id = messageId,
            RegistrationId = registrationId,
            SessionId = sessionId,
            OccurredAtUtc = occurredAtUtc,
            ReceivedAtUtc = receivedAtUtc,
            ProcessedAtUtc = null
        };
    }
}