namespace ConferenceProFlow.Analytics.Worker.Domain.Entities;

/// <summary>
/// Durable record of one accepted registration finalization. The database
/// enforces unique MessageId and RegistrationId (business identity).
/// M3.4D will decide whether an incoming record is new, duplicate, or conflict.
/// </summary>
public sealed class ProcessedAnalyticsEvent
{
    public Guid MessageId { get; set; }
    public Guid RegistrationId { get; set; }
    public Guid SessionId { get; set; }
    public string Status { get; set; } = string.Empty;
    public DateTimeOffset OccurredAtUtc { get; set; }
    public DateTimeOffset ProcessedAtUtc { get; set; }
}
