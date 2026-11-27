using System.Diagnostics.Metrics;
using ConferenceProFlow.Registration.Api.Observability;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Observability;

/// <summary>No DB, Docker or broker required. Validates exported gauge semantics and stable labels.</summary>
public sealed class RegistrationOutboxBacklogMetricsTests
{
    [Fact]
    public void Snapshot_ExportsBothPipelinesWithOnlyFixedTags()
    {
        var samples = new List<Sample>();
        using var listener = Listen(samples);
        RegistrationOutboxBacklogMetrics.RecordSnapshot(3, 12.5, 0, 0);
        listener.RecordObservableInstruments();

        Assert.Equal(4, samples.Count);
        Assert.Contains(samples, x => x.Name == "conferenceproflow.outbox.backlog.pending" &&
            x.Pipeline == "registration_request_rabbitmq" && x.Value == 3 && x.TagCount == 1);
        Assert.Contains(samples, x => x.Name == "conferenceproflow.outbox.backlog.oldest.age" &&
            x.Pipeline == "registration_request_rabbitmq" && x.Value == 12.5 && x.TagCount == 1);
        Assert.Contains(samples, x => x.Name == "conferenceproflow.outbox.backlog.pending" &&
            x.Pipeline == "analytics_kafka" && x.Value == 0 && x.TagCount == 1);
        Assert.Contains(samples, x => x.Name == "conferenceproflow.outbox.backlog.oldest.age" &&
            x.Pipeline == "analytics_kafka" && x.Value == 0 && x.TagCount == 1);
    }

    [Fact]
    public void Snapshot_ClampsInvalidOrFutureAgeAndEmptyQueueAge()
    {
        Assert.Equal(0, RegistrationOutboxBacklogMetrics.AgeSeconds(null, DateTimeOffset.UtcNow));
        var now = new DateTimeOffset(2026, 11, 27, 12, 0, 0, TimeSpan.Zero);
        Assert.Equal(0, RegistrationOutboxBacklogMetrics.AgeSeconds(now.AddMinutes(1), now));
        Assert.Equal(60, RegistrationOutboxBacklogMetrics.AgeSeconds(now.AddMinutes(-1), now));

        var samples = new List<Sample>();
        using var listener = Listen(samples);
        RegistrationOutboxBacklogMetrics.RecordSnapshot(0, 99, 2, -15);
        listener.RecordObservableInstruments();
        Assert.All(samples.Where(x => x.Name == "conferenceproflow.outbox.backlog.oldest.age"),
            x => Assert.Equal(0, x.Value));
    }

    private sealed record Sample(string Name, double Value, string? Pipeline, int TagCount);

    private static MeterListener Listen(List<Sample> samples)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, active) =>
        {
            if (instrument.Meter.Name == RegistrationOutboxBacklogMetrics.MeterName)
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
