using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;
using ConferenceProFlow.Registration.Api.Observability;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Infrastructure.Outbox;

public sealed class AnalyticsOutboxTraceContextTests
{
    private const string Parent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private const string State = "vendor=value";

    private static RegistrationFinalizedV1 Event() =>
        RegistrationFinalizedV1.Create(
            Guid.NewGuid(), Guid.NewGuid(), RegistrationFinalizedV1.ConfirmedStatus);

    [Fact]
    public void FromEvent_WithValidContext_PersistsIdentityPayloadAndContext()
    {
        var evt = Event();
        var row = AnalyticsOutboxMessage.FromEvent(evt, Parent, State);
        Assert.Equal(evt.MessageId, row.Id);
        Assert.Equal(evt.RegistrationId, row.RegistrationId);
        Assert.Equal(Parent, row.TraceParent);
        Assert.Equal(State, row.TraceState);
        Assert.Equal(evt, JsonSerializer.Deserialize<RegistrationFinalizedV1>(row.Payload));
    }

    [Fact]
    public void FromEvent_WithoutContext_RemainsBackwardCompatible()
    {
        var row = AnalyticsOutboxMessage.FromEvent(Event());
        Assert.Null(row.TraceParent);
        Assert.Null(row.TraceState);
    }

    [Fact]
    public void FromEvent_TraceParentOnly_IsAllowed()
    {
        var row = AnalyticsOutboxMessage.FromEvent(Event(), Parent);
        Assert.Equal(Parent, row.TraceParent);
        Assert.Null(row.TraceState);
    }

    [Fact]
    public void FromEvent_TraceStateWithoutParent_IsRejected()
    {
        Assert.Throws<ArgumentException>(() =>
            AnalyticsOutboxMessage.FromEvent(Event(), traceState: State));
    }

    [Fact]
    public void FromEvent_InvalidParent_IsRejected()
    {
        Assert.Throws<ArgumentException>(() =>
            AnalyticsOutboxMessage.FromEvent(Event(), "not-a-traceparent"));
    }

    [Fact]
    public void FromEvent_TooLongTraceState_IsRejected()
    {
        Assert.Throws<ArgumentException>(() =>
            AnalyticsOutboxMessage.FromEvent(Event(), Parent, new string('x', 513)));
    }

    [Fact]
    public void PublishRetry_DoesNotChangeStoredTraceMetadata()
    {
        var row = AnalyticsOutboxMessage.FromEvent(Event(), Parent, State);
        row.RecordPublishFailure(DateTimeOffset.UtcNow, "Broker unavailable", TimeSpan.FromSeconds(1));
        Assert.Equal(Parent, row.TraceParent);
        Assert.Equal(State, row.TraceState);
        row.MarkPublished(DateTimeOffset.UtcNow);
        Assert.Equal(Parent, row.TraceParent);
        Assert.Equal(State, row.TraceState);
    }

    [Fact]
    public void TraceColumns_AreNullableAndHaveBoundedEfModelLengths()
    {
        var parentProperty = typeof(AnalyticsOutboxMessage)
            .GetProperty(nameof(AnalyticsOutboxMessage.TraceParent))!;
        var stateProperty = typeof(AnalyticsOutboxMessage)
            .GetProperty(nameof(AnalyticsOutboxMessage.TraceState))!;
        Assert.Equal(55, parentProperty.GetCustomAttribute<MaxLengthAttribute>()?.Length);
        Assert.Equal(512, stateProperty.GetCustomAttribute<MaxLengthAttribute>()?.Length);
        Assert.Equal(typeof(string), parentProperty.PropertyType);
        Assert.Equal(typeof(string), stateProperty.PropertyType);
    }

    [Fact]
    public void Capture_WhenProcessingSpanIsActive_PreservesItsW3cContext()
    {
        const string name = "ConferenceProFlow.Tests.AnalyticsOutboxTraceCapture";
        using var source = new ActivitySource(name);
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => candidate.Name == name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        var remoteContext = new ActivityContext(
            ActivityTraceId.CreateFromString("4bf92f3577b34da6a3ce929d0e0e4736"),
            ActivitySpanId.CreateFromString("00f067aa0ba902b7"),
            ActivityTraceFlags.Recorded,
            State,
            isRemote: true);
        using var activity = source.StartActivity(
            "reconciliation", ActivityKind.Consumer, remoteContext);
        Assert.NotNull(activity);
        var trace = AnalyticsOutboxTraceContext.Capture();
        Assert.Equal(activity.Id, trace.TraceParent);
        Assert.Equal(activity.TraceStateString, trace.TraceState);
    }
}
