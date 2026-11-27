using System.Diagnostics.Metrics;
using ConferenceProFlow.Analytics.Worker.Application.Processing;
using ConferenceProFlow.Analytics.Worker.Observability;

namespace ConferenceProFlow.Analytics.IntegrationTests.Observability;

/// <summary>
/// Fast tests requiring neither Docker nor Kafka nor SQL Server.
/// Only verifies emitted instruments, tag cardinality and disposition semantics.
/// </summary>
public sealed class AnalyticsKafkaConsumerMetricsTests
{
    [Fact]
    public void RecordHandled_MapsAllDurableDispositions_ToBoundedTags()
    {
        var measurements = new List<Sample>();
        using var listener = Listen(measurements);

        foreach (var disposition in Enum.GetValues<KafkaAnalyticsRecordDisposition>())
            AnalyticsKafkaConsumerMetrics.RecordHandled(
                disposition, TimeSpan.FromMilliseconds(250));

        string[] expected =
        [
            "applied", "duplicate_message", "duplicate_business_event", "quarantined"
        ];
        Assert.Equal(expected.Length * 2, measurements.Count);
        foreach (var result in expected)
        {
            Assert.Contains(measurements, item =>
                item.Name == "conferenceproflow.analytics.kafka.handling.attempts" &&
                item.Value == 1 && item.Result == result && item.TagCount == 1);
            Assert.Contains(measurements, item =>
                item.Name == "conferenceproflow.analytics.kafka.handling.duration" &&
                item.Value == 0.25 && item.Result == result && item.TagCount == 1);
        }
    }

    [Fact]
    public void RecordHandlingFailure_UsesFixedErrorTag()
    {
        var measurements = new List<Sample>();
        using var listener = Listen(measurements);
        AnalyticsKafkaConsumerMetrics.RecordHandlingFailure(TimeSpan.FromSeconds(2));

        Assert.Equal(2, measurements.Count);
        Assert.Contains(measurements, x =>
            x.Name == "conferenceproflow.analytics.kafka.handling.attempts" &&
            x.Result == "error" && x.Value == 1 && x.TagCount == 1);
        Assert.Contains(measurements, x =>
            x.Name == "conferenceproflow.analytics.kafka.handling.duration" &&
            x.Result == "error" && x.Value == 2 && x.TagCount == 1);
    }

    [Fact]
    public void OffsetCommit_TracksSuccessAndFailure_IndependentlyOfHandling()
    {
        var measurements = new List<Sample>();
        using var listener = Listen(measurements);
        AnalyticsKafkaConsumerMetrics.RecordOffsetCommitSuccess();
        AnalyticsKafkaConsumerMetrics.RecordOffsetCommitFailure();

        Assert.Equal(2, measurements.Count);
        Assert.All(measurements, x =>
        {
            Assert.Equal("conferenceproflow.analytics.kafka.offset.commit.attempts", x.Name);
            Assert.Equal(1, x.Value);
            Assert.Equal(1, x.TagCount);
        });
        Assert.Contains(measurements, x => x.Result == "success");
        Assert.Contains(measurements, x => x.Result == "error");
    }

    [Fact]
    public void InvalidDuration_DoesNotEmitNegativeHistogramValue()
    {
        var measurements = new List<Sample>();
        using var listener = Listen(measurements);
        AnalyticsKafkaConsumerMetrics.RecordHandlingFailure(TimeSpan.FromSeconds(-1));

        Assert.Single(measurements);
        Assert.Equal("conferenceproflow.analytics.kafka.handling.attempts", measurements[0].Name);
    }

    private sealed record Sample(string Name, double Value, string? Result, int TagCount);

    private static MeterListener Listen(List<Sample> samples)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, active) =>
        {
            if (instrument.Meter.Name == AnalyticsKafkaConsumerMetrics.MeterName)
                active.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            samples.Add(new Sample(instrument.Name, value, FindResult(tags), tags.Length)));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            samples.Add(new Sample(instrument.Name, value, FindResult(tags), tags.Length)));
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
