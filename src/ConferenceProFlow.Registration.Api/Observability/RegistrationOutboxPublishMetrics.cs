using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Durable state transitions for the Registration publishers (RabbitMQ and Kafka).
/// Measurements are emitted only after the relevant SQL state is committed.
/// They are process-local best-effort telemetry, not a durable audit record.
/// </summary>
internal static class RegistrationOutboxPublishMetrics
{
    internal const string MeterName = "ConferenceProFlow.Registration.Api.Outbox.Metrics";
    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> Outcomes = Meter.CreateCounter<long>(
        "conferenceproflow.outbox.publish.outcomes",
        unit: "1",
        description: "Durably recorded publication outcomes. Retries count separately; not unique business events.");

    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "conferenceproflow.outbox.publish.duration",
        unit: "s",
        description: "Seconds from processing an outbox message until the resulting state was committed.");

    // Preserve all M4.3C3A methods and analytics_kafka labels unchanged.
    internal static void RecordPublished(TimeSpan elapsed) =>
        Record("analytics_kafka", "published", elapsed);

    internal static void RecordRetryScheduled(TimeSpan elapsed) =>
        Record("analytics_kafka", "retry_scheduled", elapsed);

    // Registration -> RabbitMQ (M4.3C3B).
    internal static void RecordRabbitMqPublished(TimeSpan elapsed) =>
        Record("registration_request_rabbitmq", "published", elapsed);

    internal static void RecordRabbitMqRetryScheduled(TimeSpan elapsed) =>
        Record("registration_request_rabbitmq", "retry_scheduled", elapsed);

    internal static void RecordRabbitMqQuarantined(TimeSpan elapsed) =>
        Record("registration_request_rabbitmq", "quarantined", elapsed);

    private static void Record(string pipeline, string outcome, TimeSpan elapsed)
    {
        var tags = new TagList
        {
            { "pipeline", pipeline },
            { "outcome", outcome }
        };

        Outcomes.Add(1, tags);

        var seconds = elapsed.TotalSeconds;
        if (seconds >= 0 && double.IsFinite(seconds))
        {
            Duration.Record(seconds, tags);
        }
    }
}
