namespace ConferenceProFlow.Analytics.Worker.Domain.Entities;

/// <summary>
/// Invalid or conflicting Kafka record, identified by its exact topic,
/// partition, and offset. Payload/diagnostics are deliberately bounded.
/// M3.4E will validate and truncate untrusted data before persistence.
/// </summary>
public sealed class QuarantinedAnalyticsEvent
{
    public string Topic { get; set; } = string.Empty;
    public int Partition { get; set; }
    public long Offset { get; set; }
    public string? MessageKey { get; set; }
    public string Payload { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
    public DateTimeOffset ReceivedAtUtc { get; set; }
}
