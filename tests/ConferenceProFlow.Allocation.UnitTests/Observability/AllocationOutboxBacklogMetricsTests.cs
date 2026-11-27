using System.Diagnostics.Metrics;
using ConferenceProFlow.Allocation.Worker.Observability;
using Xunit;

namespace ConferenceProFlow.Allocation.UnitTests.Observability;

public sealed class AllocationOutboxBacklogMetricsTests
{
    [Fact]
    public void Snapshot_ExportsPendingAndOldestAgeWithLowCardinalityPipeline()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        AllocationOutboxBacklogMetrics.RecordSnapshot(5, 7.25);
        listener.RecordObservableInstruments();

        Assert.Equal(2, samples.Count);
        Assert.Contains(samples, x => x.Name == "conferenceproflow.outbox.backlog.pending" &&
            x.Pipeline == "allocation_result_rabbitmq" && x.Value == 5 && x.TagCount == 1);
        Assert.Contains(samples, x => x.Name == "conferenceproflow.outbox.backlog.oldest.age" &&
            x.Pipeline == "allocation_result_rabbitmq" && x.Value == 7.25 && x.TagCount == 1);
    }

    [Fact]
    public void NoPendingMessages_HasZeroAge()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        AllocationOutboxBacklogMetrics.RecordSnapshot(0, 999);
        listener.RecordObservableInstruments();
        Assert.Contains(samples, x => x.Name == "conferenceproflow.outbox.backlog.oldest.age" && x.Value == 0);

        var now = new DateTimeOffset(2026, 11, 27, 12, 0, 0, TimeSpan.Zero);
        Assert.Equal(0, AllocationOutboxBacklogMetrics.AgeSeconds(null, now));
        Assert.Equal(0, AllocationOutboxBacklogMetrics.AgeSeconds(now.AddMinutes(1), now));
        Assert.Equal(30, AllocationOutboxBacklogMetrics.AgeSeconds(now.AddSeconds(-30), now));
    }

    private sealed record Sample(string Name, double Value, string? Pipeline, int TagCount);

    private static MeterListener Listen(List<Sample> samples)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, active) =>
        {
            if (instrument.Meter.Name == AllocationOutboxBacklogMetrics.MeterName)
                active.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            samples.Add(new Sample(instrument.Name, value, Tag(tags), tags.Length)));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            samples.Add(new Sample(instrument.Name, value, Tag(tags), tags.Length)));
        listener.Start();
        return listener;
    }

    private static string? Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
            if (tag.Key == "pipeline") return tag.Value as string;
        return null;
    }
}
