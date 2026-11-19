namespace ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

/// <summary>
/// A durable allocation-result event. The payload and identity never change when publication is retried.
/// </summary>
public sealed class AllocationOutboxMessage
{
    private AllocationOutboxMessage() { } // EF Core

    public Guid Id { get; private set; }
    public Guid AllocationId { get; private set; }
    public string MessageType { get; private set; } = string.Empty;
    public string Payload { get; private set; } = string.Empty;
    public DateTimeOffset OccurredAtUtc { get; private set; }
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
            throw new ArgumentException("Message ID must not be empty.", nameof(id));
        if (allocationId == Guid.Empty)
            throw new ArgumentException("Allocation ID must not be empty.", nameof(allocationId));
        if (string.IsNullOrWhiteSpace(messageType))
            throw new ArgumentException("Message type is required.", nameof(messageType));
        var normalizedType = messageType.Trim();
        if (normalizedType.Length > 160)
            throw new ArgumentException("Message type must not exceed 160 characters.", nameof(messageType));
        if (string.IsNullOrWhiteSpace(payload))
            throw new ArgumentException("Message payload is required.", nameof(payload));
        ValidateUtc(occurredAtUtc, nameof(occurredAtUtc));

        return new AllocationOutboxMessage
        {
            Id = id,
            AllocationId = allocationId,
            MessageType = normalizedType,
            Payload = payload,
            OccurredAtUtc = occurredAtUtc
        };
    }

    public void MarkPublished(DateTimeOffset publishedAtUtc)
    {
        ValidateUtc(publishedAtUtc, nameof(publishedAtUtc));
        if (publishedAtUtc < OccurredAtUtc)
            throw new ArgumentException("Publication cannot precede event creation.", nameof(publishedAtUtc));
        if (PublishedAtUtc is not null || QuarantinedAtUtc is not null)
            throw new InvalidOperationException("Message is already published or quarantined.");

        PublishAttempts++;
        PublishedAtUtc = publishedAtUtc;
        LastError = null;
        NextAttemptAtUtc = null;
    }

    public void RecordFailure(string error, DateTimeOffset failedAtUtc)
    {
        ValidateUtc(failedAtUtc, nameof(failedAtUtc));
        if (failedAtUtc < OccurredAtUtc)
            throw new ArgumentException("Failure cannot precede event creation.", nameof(failedAtUtc));
        if (PublishedAtUtc is not null || QuarantinedAtUtc is not null)
            throw new InvalidOperationException("Message is already published or quarantined.");
        if (string.IsNullOrWhiteSpace(error))
            throw new ArgumentException("Failure description is required.", nameof(error));

        PublishAttempts++;
        LastError = TrimError(error);
        NextAttemptAtUtc = failedAtUtc + AllocationOutboxRetryPolicy.GetDelay(PublishAttempts);
    }

    public void Quarantine(string reason, DateTimeOffset quarantinedAtUtc)
    {
        ValidateUtc(quarantinedAtUtc, nameof(quarantinedAtUtc));
        if (quarantinedAtUtc < OccurredAtUtc)
            throw new ArgumentException("Quarantine cannot precede event creation.", nameof(quarantinedAtUtc));
        if (PublishedAtUtc is not null || QuarantinedAtUtc is not null)
            throw new InvalidOperationException("Message is already published or quarantined.");
        if (string.IsNullOrWhiteSpace(reason))
            throw new ArgumentException("Quarantine reason is required.", nameof(reason));

        PublishAttempts++;
        LastError = TrimError(reason);
        NextAttemptAtUtc = null;
        QuarantinedAtUtc = quarantinedAtUtc;
    }

    private static string TrimError(string error)
    {
        var trimmed = error.Trim();
        return trimmed.Length <= 2000 ? trimmed : trimmed[..2000];
    }

    private static void ValidateUtc(DateTimeOffset value, string argument)
    {
        if (value == default || value.Offset != TimeSpan.Zero)
            throw new ArgumentException("Timestamp must be a valid UTC value.", argument);
    }
}