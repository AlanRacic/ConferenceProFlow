using System.Diagnostics.Metrics;
using ConferenceProFlow.Allocation.Worker.Observability;
using ConferenceProFlow.Allocation.Worker.Processing;

namespace ConferenceProFlow.Allocation.UnitTests.Observability;

public sealed class AllocationInboxMetricsTests
{
    [Fact]
    public void RecordResult_ReportsEveryKnownDispositionAsOneAttemptAndOneDuration()
    {
        var measurements = new List<(string Name, double Value, string? Result, int TagCount)>();
        using var listener = Listen(measurements);

        foreach (var result in Enum.GetValues<AllocationProcessingResult>())
            AllocationInboxMetrics.RecordResult(result, TimeSpan.FromMilliseconds(250));

        var expected = new[]
        {
            "confirmed", "rejected", "already_processed", "existing_allocation",
            "waiting_for_inventory", "not_found"
        };
        Assert.Equal(expected.Length * 2, measurements.Count);
        foreach (var result in expected)
        {
            Assert.Contains(measurements, item =>
                item.Name == "conferenceproflow.allocation.inbox.processing.attempts" &&
                item.Value == 1 && item.Result == result && item.TagCount == 1);
            Assert.Contains(measurements, item =>
                item.Name == "conferenceproflow.allocation.inbox.processing.duration" &&
                item.Value == 0.25 && item.Result == result && item.TagCount == 1);
        }
    }

    [Fact]
    public void RecordFailure_UsesBoundedErrorTagAndNoExceptionText()
    {
        var measurements = new List<(string Name, double Value, string? Result, int TagCount)>();
        using var listener = Listen(measurements);

        AllocationInboxMetrics.RecordFailure(TimeSpan.FromSeconds(2));
        Assert.Equal(2, measurements.Count);
        Assert.Contains(measurements, item =>
            item.Name == "conferenceproflow.allocation.inbox.processing.attempts" &&
            item.Value == 1 && item.Result == "error" && item.TagCount == 1);
        Assert.Contains(measurements, item =>
            item.Name == "conferenceproflow.allocation.inbox.processing.duration" &&
            item.Value == 2 && item.Result == "error" && item.TagCount == 1);
    }

    private static MeterListener Listen(
        List<(string Name, double Value, string? Result, int TagCount)> measurements)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, active) =>
        {
            if (instrument.Meter.Name == AllocationInboxMetrics.MeterName)
                active.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            measurements.Add((instrument.Name, value, FindResult(tags), tags.Length)));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            measurements.Add((instrument.Name, value, FindResult(tags), tags.Length)));
        listener.Start();
        return listener;
    }

    private static string? FindResult(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
            if (tag.Key == "result")
                return tag.Value as string;
        return null;
    }
}
