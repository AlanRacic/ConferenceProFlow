using System.Diagnostics.Metrics;
using ConferenceProFlow.Analytics.Worker.Application.Processing;

namespace ConferenceProFlow.Analytics.Worker.Observability;

/// <summary>
/// Low-cardinality operational metrics for the durable, LIVE Kafka consumer.
/// A handling attempt describes a SQL-durable result, not a unique business event:
/// Kafka may replay the same record if its offset commit fails.
/// Metrics are best-effort and must never drive the business decision.
/// </summary>
internal static class AnalyticsKafkaConsumerMetrics
{
    internal const string MeterName = "ConferenceProFlow.Analytics.Worker.Metrics";

    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> HandlingAttempts = Meter.CreateCounter<long>(
        "conferenceproflow.analytics.kafka.handling.attempts",
        unit: "1",
        description: "Completed live Kafka record handling attempts (including durable duplicates and errors); shutdown cancellations excluded.");

    private static readonly Histogram<double> HandlingDuration = Meter.CreateHistogram<double>(
        "conferenceproflow.analytics.kafka.handling.duration",
        unit: "s",
        description: "Elapsed time in seconds for the SQL handler and its scope, excluding the subsequent Kafka offset commit.");

    private static readonly Counter<long> OffsetCommitAttempts = Meter.CreateCounter<long>(
        "conferenceproflow.analytics.kafka.offset.commit.attempts",
        unit: "1",
        description: "Explicit Kafka offset commit attempts after a durable handling disposition; success means Commit(record) returned.");

    internal static void RecordHandled(KafkaAnalyticsRecordDisposition disposition, TimeSpan elapsed) =>
        RecordHandling(disposition switch
        {
            KafkaAnalyticsRecordDisposition.Applied => "applied",
            KafkaAnalyticsRecordDisposition.DuplicateMessage => "duplicate_message",
            KafkaAnalyticsRecordDisposition.DuplicateBusinessEvent => "duplicate_business_event",
            KafkaAnalyticsRecordDisposition.Quarantined => "quarantined",
            _ => "other" // Fixed fallback; never use message content or exception details.
        }, elapsed);

    internal static void RecordHandlingFailure(TimeSpan elapsed) =>
        RecordHandling("error", elapsed);

    internal static void RecordOffsetCommitSuccess() => RecordOffsetCommit("success");
    internal static void RecordOffsetCommitFailure() => RecordOffsetCommit("error");

    private static void RecordHandling(string result, TimeSpan elapsed)
    {
        var resultTag = new KeyValuePair<string, object?>("result", result);
        HandlingAttempts.Add(1, resultTag);
        if (elapsed >= TimeSpan.Zero && double.IsFinite(elapsed.TotalSeconds))
            HandlingDuration.Record(elapsed.TotalSeconds, resultTag);
    }

    private static void RecordOffsetCommit(string result)
    {
        OffsetCommitAttempts.Add(1, new KeyValuePair<string, object?>("result", result));
    }
}
