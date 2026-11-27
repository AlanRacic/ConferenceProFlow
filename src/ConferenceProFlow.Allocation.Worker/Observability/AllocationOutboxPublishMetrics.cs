using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>
/// Durable state transitions for the RabbitMQ Allocation Result Outbox publisher.
/// A quarantined payload did not reach RabbitMQ; that outcome is separate from
/// a failed broker delivery and must not be interpreted as a publish attempt.
/// </summary>
internal static class AllocationOutboxPublishMetrics
{
    internal const string MeterName = "ConferenceProFlow.Allocation.Worker.Outbox.Metrics";
    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> Outcomes = Meter.CreateCounter<long>(
        "conferenceproflow.outbox.publish.outcomes",
        unit: "1",
        description: "Durably recorded publication outcomes, including quarantine and retries; not unique business events.");

    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "conferenceproflow.outbox.publish.duration",
        unit: "s",
        description: "Seconds spent until the associated published, retry or quarantine state was committed.");

    internal static void RecordPublished(TimeSpan elapsed) => Record("published", elapsed);
    internal static void RecordRetryScheduled(TimeSpan elapsed) => Record("retry_scheduled", elapsed);
    internal static void RecordQuarantined(TimeSpan elapsed) => Record("quarantined", elapsed);

    private static void Record(string outcome, TimeSpan elapsed)
    {
        var tags = new TagList
        {
            { "pipeline", "allocation_result_rabbitmq" },
            { "outcome", outcome }
        };
        Outcomes.Add(1, tags);
        var seconds = elapsed.TotalSeconds;
        if (seconds >= 0 && double.IsFinite(seconds))
            Duration.Record(seconds, tags);
    }
}
