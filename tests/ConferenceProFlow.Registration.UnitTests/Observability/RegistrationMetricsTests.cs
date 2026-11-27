using System.Diagnostics.Metrics;
using ConferenceProFlow.Registration.Api.Domain.Enums;
using ConferenceProFlow.Registration.Api.Observability;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Observability;

public sealed class RegistrationMetricsTests
{
    [Fact]
    public void RecordFinalized_EmitsCounterAndDurationWithBoundedOutcomeTag()
    {
        var measurements = new List<(string Metric, double Value, string? Outcome)>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, activeListener) =>
        {
            if (instrument.Meter.Name == RegistrationMetrics.MeterName)
                activeListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            measurements.Add((instrument.Name, value, ReadOutcome(tags))));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            measurements.Add((instrument.Name, value, ReadOutcome(tags))));
        listener.Start();

        var start = new DateTimeOffset(2026, 11, 27, 8, 0, 0, TimeSpan.Zero);
        RegistrationMetrics.RecordFinalized(RegistrationStatus.Confirmed, start, start.AddSeconds(12));
        RegistrationMetrics.RecordFinalized(RegistrationStatus.Rejected, start, start.AddSeconds(4));
        RegistrationMetrics.RecordFinalized(RegistrationStatus.Pending, start, start.AddSeconds(2));
        RegistrationMetrics.RecordFinalized(RegistrationStatus.Confirmed, start, start.AddSeconds(-1));

        Assert.Equal(4, measurements.Count);
        Assert.Contains(measurements, item =>
            item.Metric == "conferenceproflow.registration.finalized" &&
            item.Value == 1 && item.Outcome == "confirmed");
        Assert.Contains(measurements, item =>
            item.Metric == "conferenceproflow.registration.finalized" &&
            item.Value == 1 && item.Outcome == "rejected");
        Assert.Contains(measurements, item =>
            item.Metric == "conferenceproflow.registration.finalization.duration" &&
            item.Value == 12 && item.Outcome == "confirmed");
        Assert.Contains(measurements, item =>
            item.Metric == "conferenceproflow.registration.finalization.duration" &&
            item.Value == 4 && item.Outcome == "rejected");
    }

    private static string? ReadOutcome(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "outcome")
                return tag.Value as string;
        }
        return null;
    }
}
