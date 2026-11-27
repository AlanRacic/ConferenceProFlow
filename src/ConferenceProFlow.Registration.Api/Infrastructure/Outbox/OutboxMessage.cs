using System.Diagnostics;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

public sealed class OutboxMessage
{
    private OutboxMessage()
    {
        // Required for EF Core materialization.
    }

    public Guid Id { get; private set; }

    public string MessageType { get; private set; } = string.Empty;

    public string Payload { get; private set; } = string.Empty;

    public DateTimeOffset OccurredAtUtc { get; private set; }

    // W3C trace context persisted across the durable Outbox boundary.
    public string? TraceParent { get; private set; }

    public string? TraceState { get; private set; }

    public DateTimeOffset? PublishedAtUtc { get; private set; }

    public int PublishAttempts { get; private set; }

    public string? LastError { get; private set; }

    public DateTimeOffset? NextAttemptAtUtc { get; private set; }

    public DateTimeOffset? QuarantinedAtUtc { get; private set; }

    public static OutboxMessage Create(
    Guid id,
    string messageType,
    string payload,
    DateTimeOffset occurredAtUtc,
    string? traceParent = null,
    string? traceState = null)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("Message ID must not be empty.", nameof(id));
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

        // Trace context is optional, but must be valid when supplied.
        if (traceParent is null && traceState is not null)
        {
            throw new ArgumentException(
                "TraceState requires TraceParent.",
                nameof(traceState));
        }

        if (traceState is { Length: > 512 })
        {
            throw new ArgumentException(
                "TraceState must not exceed 512 characters.",
                nameof(traceState));
        }

        if (traceParent is not null &&
            (traceParent.Length > 55 ||
             !ActivityContext.TryParse(traceParent, traceState, out _)))
        {
            throw new ArgumentException(
                "Invalid W3C trace context.",
                nameof(traceParent));
        }

        return new OutboxMessage
        {
            Id = id,
            MessageType = normalizedMessageType,
            Payload = payload,
            OccurredAtUtc = occurredAtUtc,
            TraceParent = traceParent,
            TraceState = traceState,
            PublishedAtUtc = null,
            PublishAttempts = 0,
            LastError = null,
            NextAttemptAtUtc = null,
            QuarantinedAtUtc = null
        };
    }

    public void MarkPublished(DateTimeOffset publishedAtUtc)
    {
        if (publishedAtUtc == default || publishedAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("PublishedAtUtc must be a valid UTC timestamp.", nameof(publishedAtUtc));
        }

        if (PublishedAtUtc is not null)
        {
            throw new InvalidOperationException("Outbox message has already been published.");
        }

        if (publishedAtUtc < OccurredAtUtc)
        {
            throw new ArgumentException("PublishedAtUtc cannot precede OccurredAtUtc.", nameof(publishedAtUtc));
        }

        if (QuarantinedAtUtc is not null)
        {
            throw new InvalidOperationException("Cannot publish a quarantined message.");
        }

        PublishAttempts++;
        PublishedAtUtc = publishedAtUtc;
        LastError = null;
        NextAttemptAtUtc = null;
    }

    public void RecordFailure(string error, DateTimeOffset failedAtUtc)
    {
        if (PublishedAtUtc is not null)
        {
            throw new InvalidOperationException("Cannot record a publishing failure for a published message.");
        }

        if (string.IsNullOrWhiteSpace(error))
        {
            throw new ArgumentException("Error description is required.", nameof(error));
        }

        if (failedAtUtc == default || failedAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("FailedAtUtc must be a valid UTC timestamp.", nameof(failedAtUtc));
        }

        if (failedAtUtc < OccurredAtUtc)
        {
            throw new ArgumentException("FailedAtUtc cannot precede OccurredAtUtc.", nameof(failedAtUtc));
        }

        var normalizedError = error.Trim();

        if (normalizedError.Length > 2000)
        {
            normalizedError = normalizedError[..2000];
        }

        if (QuarantinedAtUtc is not null)
        {
            throw new InvalidOperationException("Cannot retry a quarantined message.");
        }

        PublishAttempts++;

        LastError = normalizedError;

        NextAttemptAtUtc = failedAtUtc + OutboxRetryPolicy.GetDelay(PublishAttempts);
    }

    public void Quarantine(string reason, DateTimeOffset quarantinedAtUtc)
    {
        if (PublishedAtUtc is not null)
        {
            throw new InvalidOperationException("Cannot quarantine a published message.");
        }

        if (QuarantinedAtUtc is not null)
        {
            throw new InvalidOperationException("Outbox message is already quarantined.");
        }

        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new ArgumentException("Quarantine reason is required.", nameof(reason));
        }

        if (quarantinedAtUtc == default || quarantinedAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("QuarantinedAtUtc must be a valid UTC timestamp.", nameof(quarantinedAtUtc));
        }

        if (quarantinedAtUtc < OccurredAtUtc)
        {
            throw new ArgumentException("QuarantinedAtUtc cannot precede OccurredAtUtc.", nameof(quarantinedAtUtc));
        }

        var normalizedReason = reason.Trim();

        if (normalizedReason.Length > 2000)
        {
            normalizedReason = normalizedReason[..2000];
        }

        PublishAttempts++;

        LastError = normalizedReason;

        NextAttemptAtUtc = null;

        QuarantinedAtUtc = quarantinedAtUtc;
    }
}