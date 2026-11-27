using System.Diagnostics.Metrics;
using ConferenceProFlow.Allocation.Worker.Observability;

namespace ConferenceProFlow.Allocation.UnitTests.Observability;

/// <summary>No Docker, SQL Server, RabbitMQ or Kafka required.</summary>
public sealed class AllocationOutboxPublishMetricsTests
{
    [Fact]
    public void Outcomes_AreCountedWithOnlyFixedLowCardinalityTags()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        AllocationOutboxPublishMetrics.RecordPublished(TimeSpan.FromMilliseconds(250));
        AllocationOutboxPublishMetrics.RecordRetryScheduled(TimeSpan.FromMilliseconds(250));
        AllocationOutboxPublishMetrics.RecordQuarantined(TimeSpan.FromMilliseconds(250));

        string[] expected = ["published", "retry_scheduled", "quarantined"];
        Assert.Equal(expected.Length * 2, samples.Count);
        foreach (var outcome in expected)
        {
            Assert.Contains(samples, item =>
                item.Name == "conferenceproflow.outbox.publish.outcomes" &&
                item.Value == 1 && item.Outcome == outcome &&
                item.Pipeline == "allocation_result_rabbitmq" && item.TagCount == 2);
            Assert.Contains(samples, item =>
                item.Name == "conferenceproflow.outbox.publish.duration" &&
                item.Value == 0.25 && item.Outcome == outcome &&
                item.Pipeline == "allocation_result_rabbitmq" && item.TagCount == 2);
        }
    }

    [Fact]
    public void NegativeDuration_IsNotRecordedButOutcomeIsCounted()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        AllocationOutboxPublishMetrics.RecordRetryScheduled(TimeSpan.FromSeconds(-1));
        Assert.Single(samples);
        Assert.Equal("conferenceproflow.outbox.publish.outcomes", samples[0].Name);
        Assert.Equal("retry_scheduled", samples[0].Outcome);
    }

    private sealed record Sample(string Name, double Value, string? Pipeline,
        string? Outcome, int TagCount);

    private static MeterListener Listen(List<Sample> samples)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, active) =>
        {
            if (instrument.Meter.Name == AllocationOutboxPublishMetrics.MeterName)
                active.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            samples.Add(new Sample(instrument.Name, value,
                GetTag(tags, "pipeline"), GetTag(tags, "outcome"), tags.Length)));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            samples.Add(new Sample(instrument.Name, value,
                GetTag(tags, "pipeline"), GetTag(tags, "outcome"), tags.Length)));
        listener.Start();
        return listener;
    }

    private static string? GetTag(
        ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var tag in tags)
            if (tag.Key == key)
                return tag.Value as string;
        return null;
    }
}
