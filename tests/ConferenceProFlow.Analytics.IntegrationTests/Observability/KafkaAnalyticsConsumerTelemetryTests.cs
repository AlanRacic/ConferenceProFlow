using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using ConferenceProFlow.Analytics.Worker.Observability;

namespace ConferenceProFlow.Analytics.IntegrationTests.Observability;

/// <summary>
/// Fast tests in the existing Analytics test project. They need no Kafka,
/// SQL Server, or Docker; the full integration suite is run separately.
/// </summary>
public sealed class KafkaAnalyticsConsumerTelemetryTests
{
    private const string ValidTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    [Fact]
    public void TryExtract_ValidParentAndState_ReturnsContext()
    {
        var headers = NewHeaders(ValidTraceParent, "vendor=value");

        var success = KafkaAnalyticsConsumerTelemetry.TryExtract(headers, out var parent);

        Assert.True(success);
        Assert.Equal(ActivityTraceId.CreateFromString(
            "4bf92f3577b34da6a3ce929d0e0e4736".AsSpan()), parent.TraceId);
        Assert.Equal(ActivitySpanId.CreateFromString(
            "00f067aa0ba902b7".AsSpan()), parent.SpanId);
        Assert.Equal("vendor=value", parent.TraceState);
    }

    [Fact]
    public void TryExtract_ValidParentWithoutState_IsSupported()
    {
        Assert.True(KafkaAnalyticsConsumerTelemetry.TryExtract(
            NewHeaders(ValidTraceParent), out var parent));
        Assert.Equal(ActivitySpanId.CreateFromString(
            "00f067aa0ba902b7".AsSpan()), parent.SpanId);
    }

    [Fact]
    public void TryExtract_MissingHeaders_ReturnsFalse()
    {
        Assert.False(KafkaAnalyticsConsumerTelemetry.TryExtract(null, out _));
        Assert.False(KafkaAnalyticsConsumerTelemetry.TryExtract(new Headers(), out _));
    }

    [Fact]
    public void TryExtract_InvalidParent_ReturnsFalse()
    {
        Assert.False(KafkaAnalyticsConsumerTelemetry.TryExtract(
            NewHeaders("not-a-traceparent"), out _));
    }

    [Fact]
    public void TryExtract_OversizedState_ReturnsFalse()
    {
        Assert.False(KafkaAnalyticsConsumerTelemetry.TryExtract(
            NewHeaders(ValidTraceParent, new string('a', 513)), out _));
    }

    [Fact]
    public void TryExtract_BinaryHeader_ReturnsFalse()
    {
        var headers = new Headers();
        headers.Add("traceparent", new byte[] { 0xFF, 0x00, 0x01 });
        Assert.False(KafkaAnalyticsConsumerTelemetry.TryExtract(headers, out _));
    }

    [Fact]
    public void TryExtract_InvalidStateControlByte_ReturnsFalse()
    {
        var headers = NewHeaders(ValidTraceParent);
        headers.Add("tracestate", new byte[] { 0x0A });
        Assert.False(KafkaAnalyticsConsumerTelemetry.TryExtract(headers, out _));
    }

    [Fact]
    public void TryExtract_LatestRepeatedHeaderIsUsed()
    {
        var headers = NewHeaders("invalid");
        headers.Add("traceparent", Encoding.ASCII.GetBytes(ValidTraceParent));
        Assert.True(KafkaAnalyticsConsumerTelemetry.TryExtract(headers, out _));
    }

    [Fact]
    public void StartProcessingActivity_ValidParent_CreatesLinkedTraceChild()
    {
        using var listener = CreateListener();
        using var activity = KafkaAnalyticsConsumerTelemetry.StartProcessingActivity(
            NewHeaders(ValidTraceParent, "vendor=value"));

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(ActivityTraceId.CreateFromString(
            "4bf92f3577b34da6a3ce929d0e0e4736".AsSpan()), activity.TraceId);
        Assert.Equal(ActivitySpanId.CreateFromString(
            "00f067aa0ba902b7".AsSpan()), activity.ParentSpanId);
        Assert.Equal("vendor=value", activity.TraceStateString);
    }

    [Fact]
    public void StartProcessingActivity_LegacyMessage_CreatesIndependentConsumerSpan()
    {
        using var listener = CreateListener();
        using var activity = KafkaAnalyticsConsumerTelemetry.StartProcessingActivity(null);

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("kafka.analytics.process", activity.DisplayName);
        Assert.Equal("kafka", activity.GetTagItem("messaging.system"));
    }

    private static Headers NewHeaders(string parent, string? state = null)
    {
        var headers = new Headers();
        headers.Add("traceparent", Encoding.ASCII.GetBytes(parent));
        if (state is not null)
        {
            headers.Add("tracestate", Encoding.ASCII.GetBytes(state));
        }
        return headers;
    }

    private static ActivityListener CreateListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "ConferenceProFlow.Analytics.Worker",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
