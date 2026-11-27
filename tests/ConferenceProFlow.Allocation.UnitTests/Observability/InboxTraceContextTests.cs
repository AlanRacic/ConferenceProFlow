using System.Diagnostics;
using ConferenceProFlow.Allocation.Worker.Observability;

namespace ConferenceProFlow.Allocation.UnitTests.Observability;

public sealed class InboxTraceContextTests
{
    private const string ValidTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    [Fact]
    public void Capture_NoActivityAndNoRemoteParent_ReturnsNull()
    {
        var result = InboxTraceContext.Capture(null, null);

        Assert.Null(result.TraceParent);
        Assert.Null(result.TraceState);
    }

    [Fact]
    public void Capture_NoActivity_PreservesRemoteContext()
    {
        Assert.True(ActivityContext.TryParse(
            ValidTraceParent, "vendor=value", out var remoteParent));

        var result = InboxTraceContext.Capture(null, remoteParent);

        Assert.Equal(ValidTraceParent, result.TraceParent);
        Assert.Equal("vendor=value", result.TraceState);
    }

    [Fact]
    public void Capture_LocalConsumerActivity_PrefersItsOwnSpan()
    {
        Assert.True(ActivityContext.TryParse(
            ValidTraceParent, null, out var remoteParent));

        using var activity = new Activity("consumer")
            .SetIdFormat(ActivityIdFormat.W3C)
            .Start();

        var result = InboxTraceContext.Capture(activity, remoteParent);

        Assert.Equal(activity.Id, result.TraceParent);
        Assert.NotEqual(ValidTraceParent, result.TraceParent);
    }

    [Fact]
    public void Capture_DefaultRemoteContext_ReturnsNull()
    {
        var result = InboxTraceContext.Capture(null, default(ActivityContext));

        Assert.Null(result.TraceParent);
        Assert.Null(result.TraceState);
    }

    [Fact]
    public void Capture_OversizedRemoteTraceState_DropsTelemetry()
    {
        Assert.True(ActivityContext.TryParse(
            ValidTraceParent, null, out var validParent));

        var remoteParent = new ActivityContext(
            validParent.TraceId,
            validParent.SpanId,
            ActivityTraceFlags.Recorded,
            new string('a', 513),
            isRemote: true);

        var result = InboxTraceContext.Capture(null, remoteParent);

        Assert.Null(result.TraceParent);
        Assert.Null(result.TraceState);
    }
}
