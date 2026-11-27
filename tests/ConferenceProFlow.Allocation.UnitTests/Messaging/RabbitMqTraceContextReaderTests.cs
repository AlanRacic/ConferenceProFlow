using System.Diagnostics;
using System.Text;
using ConferenceProFlow.Allocation.Worker.Messaging.Consumers;
using ConferenceProFlow.Allocation.Worker.Observability;

namespace ConferenceProFlow.Allocation.UnitTests.Messaging;

public sealed class RabbitMqTraceContextReaderTests
{
    private const string ValidTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    [Fact]
    public void TryExtract_ByteArrayHeaders_ReturnsRemoteContext()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.ASCII.GetBytes(ValidTraceParent),
            ["tracestate"] = Encoding.ASCII.GetBytes("vendor=value")
        };

        var extracted = RabbitMqTraceContextReader.TryExtract(headers, out var context);

        Assert.True(extracted);
        Assert.True(context.IsRemote);
        Assert.Equal("4bf92f3577b34da6a3ce929d0e0e4736", context.TraceId.ToString());
        Assert.Equal("00f067aa0ba902b7", context.SpanId.ToString());
        Assert.Equal("vendor=value", context.TraceState);
    }

    [Fact]
    public void TryExtract_ReadOnlyMemoryHeaders_WorksWithoutTraceState()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = new ReadOnlyMemory<byte>(Encoding.ASCII.GetBytes(ValidTraceParent))
        };

        Assert.True(RabbitMqTraceContextReader.TryExtract(headers, out var context));
        Assert.True(context.IsRemote);
    }

    [Fact]
    public void TryExtract_StringHeaders_AreAccepted()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = ValidTraceParent
        };

        Assert.True(RabbitMqTraceContextReader.TryExtract(headers, out _));
    }

    [Fact]
    public void TryExtract_MissingHeaders_ReturnsFalse()
    {
        Assert.False(RabbitMqTraceContextReader.TryExtract(null, out _));
        Assert.False(RabbitMqTraceContextReader.TryExtract(
            new Dictionary<string, object?>(), out _));
        Assert.False(RabbitMqTraceContextReader.TryExtract(
            new Dictionary<string, object?> { ["tracestate"] = "vendor=value" }, out _));
    }

    [Fact]
    public void TryExtract_InvalidParent_ReturnsFalse()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.ASCII.GetBytes("invalid")
        };

        Assert.False(RabbitMqTraceContextReader.TryExtract(headers, out _));
    }

    [Fact]
    public void TryExtract_ExcessiveTraceState_ReturnsFalse()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.ASCII.GetBytes(ValidTraceParent),
            ["tracestate"] = new string('a', 513)
        };

        Assert.False(RabbitMqTraceContextReader.TryExtract(headers, out _));
    }

    [Fact]
    public void TryExtract_UnsupportedHeaderValue_ReturnsFalse()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = 42
        };

        Assert.False(RabbitMqTraceContextReader.TryExtract(headers, out _));
    }

    [Fact]
    public void TryExtract_NonAsciiParent_ReturnsFalse()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = ValidTraceParent[..^1] + "é"
        };

        Assert.False(RabbitMqTraceContextReader.TryExtract(headers, out _));
    }

    [Fact]
    public void ExtractedParent_CanBeUsedToCreateAConsumerSpan()
    {
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.ASCII.GetBytes(ValidTraceParent)
        };

        Assert.True(RabbitMqTraceContextReader.TryExtract(headers, out var parentContext));

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AllocationTelemetry.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) =>
                ActivitySamplingResult.AllData
        };

        ActivitySource.AddActivityListener(listener);

        using var activity = AllocationTelemetry.Source.StartActivity(
            "rabbitmq.registration-request.process",
            ActivityKind.Consumer,
            parentContext);

        Assert.NotNull(activity);
        Assert.Equal(parentContext.TraceId, activity!.TraceId);
        Assert.Equal(parentContext.SpanId, activity.ParentSpanId);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
    }
}
