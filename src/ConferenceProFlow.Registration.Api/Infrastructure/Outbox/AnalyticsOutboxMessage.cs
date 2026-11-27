using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Text.Json;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

/// <summary>
/// A durable Kafka-bound event recorded in the same RegistrationDbContext
/// SaveChanges transaction as the final Registration status and result Inbox.
/// Publication state is separate from the RabbitMQ Outbox.
/// </summary>
public sealed class AnalyticsOutboxMessage
{
    // Required by EF Core.
    private AnalyticsOutboxMessage() { }

    /// <summary>Stable integration event MessageId. Never regenerated on retry.</summary>
    public Guid Id { get; private set; }
    /// <summary>One finalization event per Registration.</summary>
    public Guid RegistrationId { get; private set; }

    public string MessageType { get; private set; } = string.Empty;
    public string Payload { get; private set; } = string.Empty;
    public DateTimeOffset OccurredAtUtc { get; private set; }

    // Optional W3C tracing metadata; not part of the business JSON contract.
    // MaxLength attributes allow EF Core to add bounded nullable columns without
    // overwriting the already locally modified RegistrationDbContext.cs.
    [MaxLength(55)]
    public string? TraceParent { get; private set; }

    [MaxLength(512)]
    public string? TraceState { get; private set; }

    // Publication state is changed only by the Kafka Analytics publisher.
    public DateTimeOffset? PublishedAtUtc { get; private set; }
    public int PublishAttempts { get; private set; }
    public string? LastError { get; private set; }
    public DateTimeOffset? NextAttemptAtUtc { get; private set; }
    public DateTimeOffset? QuarantinedAtUtc { get; private set; }

    /// <summary>Mark delivered only after Kafka confirms receipt of the record.</summary>
    public void MarkPublished(DateTimeOffset publishedAtUtc)
    {
        if (publishedAtUtc == default || publishedAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("Publication time must be UTC.", nameof(publishedAtUtc));

        if (PublishedAtUtc.HasValue)
            return;
        PublishAttempts++;
        PublishedAtUtc = publishedAtUtc;
        LastError = null;
        NextAttemptAtUtc = null;
    }

    /// <summary>Keep the event pending after a failed Kafka delivery attempt.</summary>
    public void RecordPublishFailure(DateTimeOffset attemptedAtUtc, string error, TimeSpan retryDelay)
    {
        if (attemptedAtUtc == default || attemptedAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("Attempt time must be UTC.", nameof(attemptedAtUtc));
        if (string.IsNullOrWhiteSpace(error))
            throw new ArgumentException("A failure description is required.", nameof(error));
        if (retryDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(retryDelay));
        if (PublishedAtUtc.HasValue)
            throw new InvalidOperationException("A published event cannot be marked as failed.");
        PublishAttempts++;
        LastError = error.Length > 2000 ? error[..2000] : error;
        NextAttemptAtUtc = attemptedAtUtc.Add(retryDelay);
    }

    public static AnalyticsOutboxMessage FromEvent(
        RegistrationFinalizedV1 integrationEvent,
        string? traceParent = null,
        string? traceState = null)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);
        integrationEvent.Validate();

        // Explicit factory validation: existing callers remain valid with nulls.
        if (traceParent is null && traceState is not null)
            throw new ArgumentException("TraceState requires TraceParent.", nameof(traceState));
        if (traceState is { Length: > 512 })
            throw new ArgumentException("TraceState must not exceed 512 characters.", nameof(traceState));
        if (traceParent is not null &&
            (traceParent.Length != 55 ||
             !ActivityContext.TryParse(traceParent, traceState, out _)))
        {
            throw new ArgumentException("Invalid W3C trace context.", nameof(traceParent));
        }

        return new AnalyticsOutboxMessage
        {
            Id = integrationEvent.MessageId,
            RegistrationId = integrationEvent.RegistrationId,
            MessageType = RegistrationFinalizedV1.EventType,
            Payload = JsonSerializer.Serialize(integrationEvent),
            OccurredAtUtc = integrationEvent.OccurredAtUtc,
            TraceParent = traceParent,
            TraceState = traceState,
            PublishAttempts = 0
        };
    }
}
