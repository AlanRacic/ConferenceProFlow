using System.Diagnostics.Metrics;
using System.Threading;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>
/// SQL Outbox gauges use a cached background snapshot, never synchronous queries
/// during the OpenTelemetry export callback. An unavailable DB yields no fresh sample.
/// </summary>
internal static class AllocationOutboxBacklogMetrics
{
    internal const string MeterName = "ConferenceProFlow.Allocation.Worker.Outbox.Backlog.Metrics";
    private static readonly Meter Meter = new(MeterName);
    private static readonly TimeSpan SnapshotTtl = TimeSpan.FromMinutes(2);

    private sealed record Snapshot(long Pending, double AgeSeconds, DateTimeOffset ObservedAtUtc);
    private static Snapshot? _snapshot;

    private static readonly ObservableGauge<long> PendingGauge = Meter.CreateObservableGauge<long>(
        "conferenceproflow.outbox.backlog.pending",
        ObservePending,
        unit: "1",
        description: "Unpublished, non-quarantined Allocation Outbox rows, including delayed retries.");

    private static readonly ObservableGauge<double> OldestAgeGauge = Meter.CreateObservableGauge<double>(
        "conferenceproflow.outbox.backlog.oldest.age",
        ObserveOldestAge,
        unit: "s",
        description: "Age in seconds of the oldest unpublished, non-quarantined Allocation Outbox row.");

    internal static void RecordSnapshot(long pending, double oldestAgeSeconds)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(pending);
        var age = pending == 0 || !double.IsFinite(oldestAgeSeconds)
            ? 0d : Math.Max(0d, oldestAgeSeconds);
        Volatile.Write(ref _snapshot, new Snapshot(pending, age, DateTimeOffset.UtcNow));
    }

    internal static double AgeSeconds(DateTimeOffset? oldestOccurredAtUtc, DateTimeOffset nowUtc) =>
        oldestOccurredAtUtc is null ? 0d : Math.Max(0d, (nowUtc - oldestOccurredAtUtc.Value).TotalSeconds);

    private static Snapshot? FreshSnapshot()
    {
        var snapshot = Volatile.Read(ref _snapshot);
        return snapshot is not null && DateTimeOffset.UtcNow - snapshot.ObservedAtUtc <= SnapshotTtl
            ? snapshot : null;
    }

    private static IEnumerable<Measurement<long>> ObservePending()
    {
        var snapshot = FreshSnapshot();
        if (snapshot is null) yield break;
        yield return new Measurement<long>(snapshot.Pending,
            new KeyValuePair<string, object?>("pipeline", "allocation_result_rabbitmq"));
    }

    private static IEnumerable<Measurement<double>> ObserveOldestAge()
    {
        var snapshot = FreshSnapshot();
        if (snapshot is null) yield break;
        yield return new Measurement<double>(snapshot.AgeSeconds,
            new KeyValuePair<string, object?>("pipeline", "allocation_result_rabbitmq"));
    }
}
