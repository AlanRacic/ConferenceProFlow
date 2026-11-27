using System.Diagnostics.Metrics;
using System.Threading;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Cached snapshots of the TWO RegistrationDbContext outboxes. ObservableGauge callbacks
/// NEVER access SQL Server. A background sampler refreshes the snapshot periodically.
/// No observations are exported before the first successful query or after the TTL.
/// </summary>
internal static class RegistrationOutboxBacklogMetrics
{
    internal const string MeterName = "ConferenceProFlow.Registration.Api.Outbox.Backlog.Metrics";
    private static readonly Meter Meter = new(MeterName);
    private static readonly TimeSpan SnapshotTtl = TimeSpan.FromMinutes(2);

    private sealed record Snapshot(
        long RabbitMqPending, double RabbitMqAgeSeconds,
        long AnalyticsPending, double AnalyticsAgeSeconds, DateTimeOffset ObservedAtUtc);

    private static Snapshot? _snapshot;

    private static readonly ObservableGauge<long> PendingGauge = Meter.CreateObservableGauge<long>(
        "conferenceproflow.outbox.backlog.pending",
        ObservePending,
        unit: "1",
        description: "Unpublished, non-quarantined SQL Outbox rows, including delayed retries.");

    private static readonly ObservableGauge<double> OldestAgeGauge = Meter.CreateObservableGauge<double>(
        "conferenceproflow.outbox.backlog.oldest.age",
        ObserveOldestAge,
        unit: "s",
        description: "Age in seconds of the oldest unpublished, non-quarantined Outbox row (zero if empty).");

    internal static void RecordSnapshot(
        long rabbitMqPending, double rabbitMqAgeSeconds,
        long analyticsPending, double analyticsAgeSeconds)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rabbitMqPending);
        ArgumentOutOfRangeException.ThrowIfNegative(analyticsPending);

        Volatile.Write(ref _snapshot, new Snapshot(
            rabbitMqPending, SanitizeAge(rabbitMqPending, rabbitMqAgeSeconds),
            analyticsPending, SanitizeAge(analyticsPending, analyticsAgeSeconds),
            DateTimeOffset.UtcNow));
    }

    internal static double AgeSeconds(DateTimeOffset? oldestOccurredAtUtc, DateTimeOffset nowUtc) =>
        oldestOccurredAtUtc is null ? 0d : Math.Max(0d, (nowUtc - oldestOccurredAtUtc.Value).TotalSeconds);

    private static double SanitizeAge(long pending, double ageSeconds) =>
        pending == 0 || !double.IsFinite(ageSeconds) ? 0d : Math.Max(0d, ageSeconds);

    private static Snapshot? FreshSnapshot()
    {
        var snapshot = Volatile.Read(ref _snapshot);
        return snapshot is not null &&
               DateTimeOffset.UtcNow - snapshot.ObservedAtUtc <= SnapshotTtl
            ? snapshot : null;
    }

    private static IEnumerable<Measurement<long>> ObservePending()
    {
        var snapshot = FreshSnapshot();
        if (snapshot is null) yield break;
        yield return new Measurement<long>(snapshot.RabbitMqPending,
            new KeyValuePair<string, object?>("pipeline", "registration_request_rabbitmq"));
        yield return new Measurement<long>(snapshot.AnalyticsPending,
            new KeyValuePair<string, object?>("pipeline", "analytics_kafka"));
    }

    private static IEnumerable<Measurement<double>> ObserveOldestAge()
    {
        var snapshot = FreshSnapshot();
        if (snapshot is null) yield break;
        yield return new Measurement<double>(snapshot.RabbitMqAgeSeconds,
            new KeyValuePair<string, object?>("pipeline", "registration_request_rabbitmq"));
        yield return new Measurement<double>(snapshot.AnalyticsAgeSeconds,
            new KeyValuePair<string, object?>("pipeline", "analytics_kafka"));
    }
}
