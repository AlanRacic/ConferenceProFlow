namespace ConferenceProFlow.Analytics.Worker.Application.Processing;

/// <summary>
/// Only Applied changes counts. Exact/event-business duplicates are harmless.
/// A Conflict must be durably quarantined before a future Kafka consumer may
/// commit that record's offset (quarantine and commits are M3.4E).
/// </summary>
public enum AnalyticsProcessingOutcome
{
    Applied,
    DuplicateMessage,
    DuplicateBusinessEvent,
    Conflict
}

public sealed record AnalyticsProcessingResult(
    AnalyticsProcessingOutcome Outcome,
    string? Reason = null);
