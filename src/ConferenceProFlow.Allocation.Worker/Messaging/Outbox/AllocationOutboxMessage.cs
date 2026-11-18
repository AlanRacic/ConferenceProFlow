namespace ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

/// <summary>
/// Durable Allocation-side outbox record. RabbitMQ publication is implemented in M2.6C.
/// Exactly one logical decision event is stored per AllocationId.
/// </summary>
public sealed class AllocationOutboxMessage
{
    private AllocationOutboxMessage()
    {
        // Required for EF Core materialization.
    }

    public Guid Id { get; private set; }
    public Guid AllocationId { get; private set; }
    public string MessageType { get; private set; } = string.Empty;
    public string Payload { get; private set; } = string.Empty;
    public DateTimeOffset OccurredAtUtc { get; private set; }

    // Publisher state will be managed by the M2.6C publisher.
    public DateTimeOffset? PublishedAtUtc { get; private set; }
    public int PublishAttempts { get; private set; }
    public string? LastError { get; private set; }
    public DateTimeOffset? NextAttemptAtUtc { get; private set; }
    public DateTimeOffset? QuarantinedAtUtc { get; private set; }

    public static AllocationOutboxMessage Create(
        Guid id,
        Guid allocationId,
        string messageType,
        string payload,
        DateTimeOffset occurredAtUtc)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("Message ID must not be empty.", nameof(id));
        }

        if (allocationId == Guid.Empty)
        {
            throw new ArgumentException("Allocation ID must not be empty.", nameof(allocationId));
        }

        if (string.IsNullOrWhiteSpace(messageType))
        {
            throw new ArgumentException("Message type is required.", nameof(messageType));
        }

        var normalizedMessageType = messageType.Trim();

        if (normalizedMessageType.Length > 160)
        {
            throw new ArgumentException("Message type must not exceed 160 characters.", nameof(messageType));
        }

        if (string.IsNullOrWhiteSpace(payload))
        {
            throw new ArgumentException("Message payload is required.", nameof(payload));
        }

        if (occurredAtUtc == default || occurredAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("OccurredAtUtc must be a valid UTC timestamp.", nameof(occurredAtUtc));
        }

        return new AllocationOutboxMessage
        {
            Id = id,
            AllocationId = allocationId,
            MessageType = normalizedMessageType,
            Payload = payload,
            OccurredAtUtc = occurredAtUtc,
            PublishedAtUtc = null,
            PublishAttempts = 0,
            LastError = null,
            NextAttemptAtUtc = null,
            QuarantinedAtUtc = null
        };
    }
}