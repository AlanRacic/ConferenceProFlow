namespace ConferenceProFlow.Analytics.Worker.Configuration;

/// <summary>
/// Preview and durable Kafka consumer use separate groups. Both are opt-in.
/// Default settings keep the M3.4C read-only preview OFF.
/// </summary>
public sealed class AnalyticsKafkaConsumerOptions
{
    public const string SectionName = "AnalyticsKafkaConsumer";
    public const string PreviewGroupId = "conferenceproflow.analytics.preview.v1";

    public bool Enabled { get; set; }
    public bool ObserveOnly { get; set; } = true;
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "conferenceproflow.registration-finalized.v1";
    public string GroupId { get; set; } = PreviewGroupId;

    // Preserve M3.4C preview semantics and its existing integration tests.
    public bool IsValid() => !Enabled ||
        (ObserveOnly && HasRequiredSettings());

    // Durable processing requires an EXPLICIT non-preview group. The first
    // startup with a new group replays retained messages from Earliest.
    public bool IsValidForDurable() =>
        Enabled && !ObserveOnly && HasRequiredSettings() &&
        !string.Equals(GroupId, PreviewGroupId, StringComparison.Ordinal);

    private bool HasRequiredSettings() =>
        !string.IsNullOrWhiteSpace(BootstrapServers) &&
        !string.IsNullOrWhiteSpace(Topic) &&
        !string.IsNullOrWhiteSpace(GroupId);
}
