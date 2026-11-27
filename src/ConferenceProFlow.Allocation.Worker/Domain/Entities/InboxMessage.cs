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

    // Optional W3C context survives the durable SQL Inbox boundary.
    public string? TraceParent { get; private set; }

    public string? TraceState { get; private set; }

    public static InboxMessage Create(
        Guid messageId,
        Guid registrationId,
        Guid sessionId,
        DateTimeOffset occurredAtUtc,
        DateTimeOffset receivedAtUtc,
        string? traceParent = null,
        string? traceState = null)
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

        // Legacy messages can have no tracing data; supplied context must be valid.
        if (traceParent is null && traceState is not null)
        {
            throw new ArgumentException("TraceState requires TraceParent.", nameof(traceState));
        }

        if (traceState?.Length > 512)
        {
            throw new ArgumentException("TraceState exceeds 512 characters.", nameof(traceState));
        }

        if (traceParent is not null &&
            (traceParent.Length != 55 ||
             !System.Diagnostics.ActivityContext.TryParse(traceParent, traceState, out _)))
        {
            throw new ArgumentException("Invalid W3C trace context.", nameof(traceParent));
        }

        return new InboxMessage
        {
            Id = messageId,
            RegistrationId = registrationId,
            SessionId = sessionId,
            OccurredAtUtc = occurredAtUtc,
            ReceivedAtUtc = receivedAtUtc,
            ProcessedAtUtc = null,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    public void MarkProcessed(DateTimeOffset processedAtUtc)
    {
        if (processedAtUtc == default || processedAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("ProcessedAtUtc must be a valid UTC timestamp.", nameof(processedAtUtc));
        }

        if (ProcessedAtUtc is not null)
        {
            throw new InvalidOperationException("Inbox message has already been processed.");
        }

        ProcessedAtUtc = processedAtUtc;
    }
}