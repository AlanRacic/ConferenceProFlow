using System.Diagnostics;
using System.Text;
using ConferenceProFlow.Allocation.Worker.Observability;

namespace ConferenceProFlow.Allocation.UnitTests.Observability;

public sealed class AllocationResultPublishTelemetryTests
{
    private const string ValidParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private const string ValidState = "vendor=value";

    [Fact]
    public void CreateHeaders_WithoutListener_ForwardsPersistedContext()
    {
        var headers = AllocationResultPublishTelemetry.CreateHeaders(
            null, ValidParent, ValidState);

        Assert.NotNull(headers);
        Assert.Equal(ValidParent, Encoding.ASCII.GetString(
            Assert.IsType<byte[]>(headers["traceparent"])));
        Assert.Equal(ValidState, Encoding.ASCII.GetString(
            Assert.IsType<byte[]>(headers["tracestate"])));
    }

    [Fact]
    public void CreateHeaders_LegacyMessageWithoutContext_ReturnsNull()
    {
        Assert.Null(AllocationResultPublishTelemetry.CreateHeaders(null, null, null));
    }

    [Fact]
    public void CreateHeaders_InvalidPersistedContext_IsIgnored()
    {
        Assert.Null(AllocationResultPublishTelemetry.CreateHeaders(
            null, "not-a-traceparent", "vendor=value"));
    }

    [Fact]
    public void CreateHeaders_OversizedTraceState_IsIgnored()
    {
        Assert.Null(AllocationResultPublishTelemetry.CreateHeaders(
            null, ValidParent, new string('x', 513)));
    }

    [Fact]
    public void CreateHeaders_ValidParentWithoutTraceState_OnlyAddsTraceParent()
    {
        var headers = AllocationResultPublishTelemetry.CreateHeaders(null, ValidParent, null);

        Assert.NotNull(headers);
        Assert.True(headers.ContainsKey("traceparent"));
        Assert.False(headers.ContainsKey("tracestate"));
    }

    [Fact]
    public void CreateHeaders_ActiveProducerSpan_TakesPrecedenceOverPersistedContext()
    {
        using var producer = new Activity("test-allocation-result-publish");
        producer.SetIdFormat(ActivityIdFormat.W3C);
        producer.Start();
        var headers = AllocationResultPublishTelemetry.CreateHeaders(
            producer, ValidParent, ValidState);

        Assert.NotNull(headers);
        Assert.Equal(producer.Id, Encoding.ASCII.GetString(
            Assert.IsType<byte[]>(headers["traceparent"])));
    }

    [Fact]
    public void StartActivity_WithPersistedContext_CreatesProducerSpanWithLink()
    {
        using var listener = NewListener();
        ActivitySource.AddActivityListener(listener);

        using var activity = AllocationResultPublishTelemetry.StartActivity(ValidParent, ValidState);

        Assert.NotNull(activity);
        Assert.Equal(AllocationResultPublishTelemetry.OperationName, activity.OperationName);
        Assert.Equal(ActivityKind.Producer, activity.Kind);
        var linked = Assert.Single(activity.Links);
        Assert.True(ActivityContext.TryParse(ValidParent, ValidState, out var expected));
        Assert.Equal(expected.TraceId, linked.Context.TraceId);
        Assert.Equal(expected.SpanId, linked.Context.SpanId);
    }

    [Fact]
    public void StartActivity_MissingContext_StartsWithoutLinks()
    {
        using var listener = NewListener();
        ActivitySource.AddActivityListener(listener);

        using var activity = AllocationResultPublishTelemetry.StartActivity(null, null);

        Assert.NotNull(activity);
        Assert.Empty(activity.Links);
        Assert.Equal(ActivityKind.Producer, activity.Kind);
    }

    private static ActivityListener NewListener() => new()
    {
        ShouldListenTo = source => source.Name == AllocationTelemetry.SourceName,
        Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
            ActivitySamplingResult.AllDataAndRecorded,
        SampleUsingParentId = (ref ActivityCreationOptions<string> _) =>
            ActivitySamplingResult.AllDataAndRecorded
    };
}
