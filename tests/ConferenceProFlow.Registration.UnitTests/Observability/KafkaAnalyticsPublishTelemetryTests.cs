using System.Diagnostics;
using System.Text;
using ConferenceProFlow.Registration.Api.Observability;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Observability;

public sealed class KafkaAnalyticsPublishTelemetryTests
{
    private const string PersistedParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private const string PersistedState = "vendor=value";

    [Fact]
    public void CreateHeaders_PersistedContextWithoutActivity_PropagatesOriginalTrace()
    {
        var id = Guid.NewGuid();
        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            id, "registration.finalized.v1", null, PersistedParent, PersistedState);

        Assert.Equal(id.ToString("D"), Read(headers, "message-id"));
        Assert.Equal("registration.finalized.v1", Read(headers, "event-type"));
        Assert.Equal(PersistedParent, Read(headers, "traceparent"));
        Assert.Equal(PersistedState, Read(headers, "tracestate"));
    }

    [Fact]
    public void CreateHeaders_LegacyOutboxWithoutTracing_OnlyIncludesBusinessHeaders()
    {
        var id = Guid.NewGuid();
        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            id, "registration.finalized.v1", null, null, null);

        Assert.Equal(2, headers.Count);
        Assert.Equal(id.ToString("D"), Read(headers, "message-id"));
        Assert.Equal("registration.finalized.v1", Read(headers, "event-type"));
        Assert.False(headers.TryGetLastBytes("traceparent", out _));
    }

    [Fact]
    public void CreateHeaders_InvalidPersistedParent_DoesNotAffectBusinessHeaders()
    {
        var id = Guid.NewGuid();
        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            id, "registration.finalized.v1", null, "invalid", null);

        Assert.Equal(2, headers.Count);
        Assert.Equal(id.ToString("D"), Read(headers, "message-id"));
        Assert.False(headers.TryGetLastBytes("traceparent", out _));
    }

    [Fact]
    public void CreateHeaders_PersistedParentWithoutTraceState_IsAllowed()
    {
        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            Guid.NewGuid(), "registration.finalized.v1", null, PersistedParent, null);

        Assert.Equal(PersistedParent, Read(headers, "traceparent"));
        Assert.False(headers.TryGetLastBytes("tracestate", out _));
    }

    [Fact]
    public void CreateHeaders_OversizedTraceState_IsNotPropagated()
    {
        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            Guid.NewGuid(), "registration.finalized.v1", null,
            PersistedParent, new string('x', 513));

        Assert.Equal(2, headers.Count);
        Assert.False(headers.TryGetLastBytes("traceparent", out _));
    }

    [Fact]
    public void CreateHeaders_ControlCharactersInTraceState_AreRejected()
    {
        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            Guid.NewGuid(), "registration.finalized.v1", null,
            PersistedParent, "vendor=value\nsecret");

        Assert.Equal(2, headers.Count);
        Assert.False(headers.TryGetLastBytes("traceparent", out _));
    }

    [Fact]
    public void StartActivity_ValidPersistedContext_CreatesLinkNotRemoteParent()
    {
        using var listener = CreateListener();
        using var activity = KafkaAnalyticsPublishTelemetry.StartActivity(PersistedParent, PersistedState);

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Producer, activity!.Kind);
        Assert.Equal(KafkaAnalyticsPublishTelemetry.OperationName, activity.OperationName);
        var link = Assert.Single(activity.Links);
        Assert.Equal(ActivityTraceId.CreateFromString("4bf92f3577b34da6a3ce929d0e0e4736"), link.Context.TraceId);
        Assert.Equal(ActivitySpanId.CreateFromString("00f067aa0ba902b7"), link.Context.SpanId);
        Assert.Equal(PersistedState, link.Context.TraceState);
        Assert.Equal("kafka", activity.GetTagItem("messaging.system"));
    }

    [Fact]
    public void CreateHeaders_ActiveProducerSpan_UsesProducerContextInsteadOfPersistedOrigin()
    {
        using var listener = CreateListener();
        using var activity = KafkaAnalyticsPublishTelemetry.StartActivity(PersistedParent, PersistedState);
        Assert.NotNull(activity);

        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            Guid.NewGuid(), "registration.finalized.v1", activity, PersistedParent, PersistedState);

        Assert.Equal(activity!.Id, Read(headers, "traceparent"));
        Assert.NotEqual(PersistedParent, Read(headers, "traceparent"));
    }

    [Fact]
    public void StartActivity_MissingPersistedContext_StillCreatesNewProducerSpan()
    {
        using var listener = CreateListener();
        using var activity = KafkaAnalyticsPublishTelemetry.StartActivity(null, null);

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Producer, activity!.Kind);
        Assert.Empty(activity.Links);
        Assert.Equal("send", activity.GetTagItem("messaging.operation.name"));
    }

    [Fact]
    public void StartActivity_WithoutListener_CanBeDisabledWithoutBreakingHeaderFallback()
    {
        // No listener is installed here. Header generation must also handle
        // null, regardless of whether another parallel test enables a listener.
        var headers = KafkaAnalyticsPublishTelemetry.CreateHeaders(
            Guid.NewGuid(), "registration.finalized.v1", null, PersistedParent, PersistedState);
        Assert.Equal(PersistedParent, Read(headers, "traceparent"));
    }

    private static ActivityListener CreateListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RegistrationTelemetry.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static string Read(Confluent.Kafka.Headers headers, string name)
    {
        Assert.True(headers.TryGetLastBytes(name, out var value));
        return Encoding.UTF8.GetString(value);
    }
}
