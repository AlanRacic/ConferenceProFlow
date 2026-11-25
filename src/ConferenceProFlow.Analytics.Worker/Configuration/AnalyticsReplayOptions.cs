namespace ConferenceProFlow.Analytics.Worker.Configuration;

/// <summary>
/// Explicit opt-in configuration for an isolated, bounded Kafka replay.
/// A new GroupId is required per fresh run; Resume=true deliberately reuses
/// the local run manifest and the same group's committed Kafka offsets.
/// </summary>
public sealed class AnalyticsReplayOptions
{
    public const string SectionName = "AnalyticsReplay";
    public const string DatabaseName = "ConferenceProFlowAnalyticsReplay";
    public const string LiveDatabaseName = "ConferenceProFlowAnalytics";
    public const string ReplayGroupPrefix = "conferenceproflow.analytics.replay.";
    public const string FinalizedTopic = "conferenceproflow.registration-finalized.v1";

    public bool Enabled { get; set; }
    public string BootstrapServers { get; set; } = "127.0.0.1:9092";
    public string Topic { get; set; } = FinalizedTopic;
    public string GroupId { get; set; } = string.Empty;
    /// <summary>
    /// Optional absolute path for durable replay snapshot manifests. If omitted,
    /// use the current user's LocalApplicationData/ConferenceProFlow/ReplaySnapshots.
    /// Keep this location stable between an interrupted run and Resume=true.
    /// </summary>
    public string? SnapshotDirectory { get; set; }
    public bool Resume { get; set; }
    public int MaxIdleSeconds { get; set; } = 60;
}
