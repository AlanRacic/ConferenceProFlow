using System.Diagnostics;
using System.Text;
using ConferenceProFlow.Registration.Api.Observability;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Observability;

public sealed class AllocationResultConsumerTelemetryTests
{
    private const string Parent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private const string State = "vendor=value";

    [Fact]
    public void TryExtract_ByteArrayHeaders_ProducesRemoteContext()
    {
        var headers = Headers();
        Assert.True(AllocationResultConsumerTelemetry.TryExtractContext(headers, out var context));
        Assert.True(context.IsRemote);
        Assert.Equal(ActivityTraceId.CreateFromString("4bf92f3577b34da6a3ce929d0e0e4736"), context.TraceId);
        Assert.Equal(ActivitySpanId.CreateFromString("00f067aa0ba902b7"), context.SpanId);
        Assert.Equal(State, context.TraceState);
    }

    [Fact]
    public void TryExtract_StringHeaders_AreSupported()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Parent,
            ["tracestate"] = State
        };
        Assert.True(AllocationResultConsumerTelemetry.TryExtractContext(headers, out _));
    }

    [Fact]
    public void TryExtract_ReadOnlyMemoryHeaders_AreSupported()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = (ReadOnlyMemory<byte>)Encoding.ASCII.GetBytes(Parent),
            ["tracestate"] = (ReadOnlyMemory<byte>)Encoding.ASCII.GetBytes(State)
        };
        Assert.True(AllocationResultConsumerTelemetry.TryExtractContext(headers, out _));
    }

    [Fact]
    public void TryExtract_TraceParentWithoutTraceState_IsValid()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.ASCII.GetBytes(Parent)
        };
        Assert.True(AllocationResultConsumerTelemetry.TryExtractContext(headers, out var context));
        Assert.Null(context.TraceState);
    }

    [Fact]
    public void TryExtract_NullHeaders_ReturnsFalse()
    {
        Assert.False(AllocationResultConsumerTelemetry.TryExtractContext(null, out _));
    }

    [Fact]
    public void TryExtract_OnlyTraceState_ReturnsFalse()
    {
        var headers = new Dictionary<string, object?> { ["tracestate"] = State };
        Assert.False(AllocationResultConsumerTelemetry.TryExtractContext(headers, out _));
    }

    [Fact]
    public void TryExtract_InvalidTraceParent_ReturnsFalse()
    {
        var headers = new Dictionary<string, object?> { ["traceparent"] = "invalid" };
        Assert.False(AllocationResultConsumerTelemetry.TryExtractContext(headers, out _));
    }

    [Fact]
    public void TryExtract_TooLongTraceState_ReturnsFalse()
    {
        var headers = Headers();
        headers["tracestate"] = new string('a', 513);
        Assert.False(AllocationResultConsumerTelemetry.TryExtractContext(headers, out _));
    }

    [Fact]
    public void TryExtract_NonPrintableHeader_ReturnsFalse()
    {
        var headers = Headers();
        headers["tracestate"] = new byte[] { 0x00, 0x09 };
        Assert.False(AllocationResultConsumerTelemetry.TryExtractContext(headers, out _));
    }

    [Fact]
    public void TryExtract_UnexpectedHeaderType_ReturnsFalse()
    {
        var headers = new Dictionary<string, object?> { ["traceparent"] = 42 };
        Assert.False(AllocationResultConsumerTelemetry.TryExtractContext(headers, out _));
    }

    [Fact]
    public void StartProcessingActivity_WithRemoteParent_CreatesChildConsumerSpan()
    {
        using var listener = CreateListener();
        using var activity = AllocationResultConsumerTelemetry.StartProcessingActivity(Headers());

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(ActivityTraceId.CreateFromString("4bf92f3577b34da6a3ce929d0e0e4736"), activity.TraceId);
        Assert.Equal(ActivitySpanId.CreateFromString("00f067aa0ba902b7"), activity.ParentSpanId);
        Assert.Equal("rabbitmq.allocation-result.process", activity.OperationName);
        Assert.Equal("rabbitmq", activity.GetTagItem("messaging.system"));
        Assert.Equal("process", activity.GetTagItem("messaging.operation.name"));
    }

    [Fact]
    public void StartProcessingActivity_MissingHeaders_DoesNotRequireTraceContext()
    {
        using var listener = CreateListener();
        using var activity = AllocationResultConsumerTelemetry.StartProcessingActivity(null);

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("rabbitmq.allocation-result.process", activity.OperationName);
    }

    [Fact]
    public void StartProcessingActivity_MalformedHeaders_DoesNotThrow()
    {
        using var listener = CreateListener();
        var headers = new Dictionary<string, object?> { ["traceparent"] = "not-a-parent" };
        using var activity = AllocationResultConsumerTelemetry.StartProcessingActivity(headers);

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
    }

    private static Dictionary<string, object?> Headers() => new()
    {
        ["traceparent"] = Encoding.ASCII.GetBytes(Parent),
        ["tracestate"] = Encoding.ASCII.GetBytes(State)
    };

    private static ActivityListener CreateListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RegistrationTelemetry.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
