using System.Diagnostics.Metrics;
using ConferenceProFlow.Registration.Api.Observability;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Observability;

/// <summary>No Docker, SQL Server, RabbitMQ or Kafka required.</summary>
public sealed class RegistrationOutboxPublishMetricsTests
{
    [Fact]
    public void KafkaOutcomes_AreCountedWithOnlyFixedLowCardinalityTags()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        RegistrationOutboxPublishMetrics.RecordPublished(TimeSpan.FromMilliseconds(250));
        RegistrationOutboxPublishMetrics.RecordRetryScheduled(TimeSpan.FromMilliseconds(250));

        AssertOutcomes(samples, "analytics_kafka", ["published", "retry_scheduled"]);
    }

    [Fact]
    public void RabbitMqOutcomes_AreCountedWithOnlyFixedLowCardinalityTags()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        RegistrationOutboxPublishMetrics.RecordRabbitMqPublished(TimeSpan.FromMilliseconds(250));
        RegistrationOutboxPublishMetrics.RecordRabbitMqRetryScheduled(TimeSpan.FromMilliseconds(250));
        RegistrationOutboxPublishMetrics.RecordRabbitMqQuarantined(TimeSpan.FromMilliseconds(250));

        AssertOutcomes(samples, "registration_request_rabbitmq",
            ["published", "retry_scheduled", "quarantined"]);
    }

    [Fact]
    public void KafkaNegativeDuration_IsNotRecordedButOutcomeIsCounted()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        RegistrationOutboxPublishMetrics.RecordRetryScheduled(TimeSpan.FromSeconds(-1));
        Assert.Single(samples);
        Assert.Equal("conferenceproflow.outbox.publish.outcomes", samples[0].Name);
        Assert.Equal("retry_scheduled", samples[0].Outcome);
        Assert.Equal("analytics_kafka", samples[0].Pipeline);
    }

    [Fact]
    public void RabbitMqNegativeDuration_IsNotRecordedButOutcomeIsCounted()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        RegistrationOutboxPublishMetrics.RecordRabbitMqQuarantined(TimeSpan.FromSeconds(-1));
        Assert.Single(samples);
        Assert.Equal("conferenceproflow.outbox.publish.outcomes", samples[0].Name);
        Assert.Equal("quarantined", samples[0].Outcome);
        Assert.Equal("registration_request_rabbitmq", samples[0].Pipeline);
    }

    private static void AssertOutcomes(List<Sample> samples, string pipeline, string[] expected)
    {
        Assert.Equal(expected.Length * 2, samples.Count);
        foreach (var outcome in expected)
        {
            Assert.Contains(samples, item =>
                item.Name == "conferenceproflow.outbox.publish.outcomes" &&
                item.Value == 1 && item.Outcome == outcome &&
                item.Pipeline == pipeline && item.TagCount == 2);
            Assert.Contains(samples, item =>
                item.Name == "conferenceproflow.outbox.publish.duration" &&
                item.Value == 0.25 && item.Outcome == outcome &&
                item.Pipeline == pipeline && item.TagCount == 2);
        }
    }

    private sealed record Sample(string Name, double Value, string? Pipeline,
        string? Outcome, int TagCount);

    private static MeterListener Listen(List<Sample> samples)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, active) =>
        {
            if (instrument.Meter.Name == RegistrationOutboxPublishMetrics.MeterName)
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
