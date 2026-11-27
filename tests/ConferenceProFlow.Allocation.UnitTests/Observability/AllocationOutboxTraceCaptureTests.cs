using System.Diagnostics;
using ConferenceProFlow.Allocation.Worker.Observability;

namespace ConferenceProFlow.Allocation.UnitTests.Observability;

public sealed class AllocationOutboxTraceCaptureTests
{
    private const string ValidTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    [Fact]
    public void Capture_WithoutActiveActivity_PreservesInboxContext()
    {
        var (traceParent, traceState) = AllocationOutboxTraceContext.Capture(
            null, ValidTraceParent, "vendor=value");

        Assert.Equal(ValidTraceParent, traceParent);
        Assert.Equal("vendor=value", traceState);
    }

    [Fact]
    public void Capture_WithoutTracingData_ReturnsNulls()
    {
        var (traceParent, traceState) = AllocationOutboxTraceContext.Capture(
            null, null, null);

        Assert.Null(traceParent);
        Assert.Null(traceState);
    }

    [Fact]
    public void Capture_WithInvalidInboxParent_DoesNotThrow()
    {
        var (traceParent, traceState) = AllocationOutboxTraceContext.Capture(
            null, "invalid-parent", "vendor=value");

        Assert.Null(traceParent);
        Assert.Null(traceState);
    }

    [Fact]
    public void Capture_WithTooLongTraceState_DoesNotThrow()
    {
        var (traceParent, traceState) = AllocationOutboxTraceContext.Capture(
            null, ValidTraceParent, new string('a', 513));

        Assert.Null(traceParent);
        Assert.Null(traceState);
    }

    [Fact]
    public void Capture_WithActiveW3CActivity_PrefersProcessingSpan()
    {
        using var activity = new Activity("allocation.inbox.process");
        activity.SetIdFormat(ActivityIdFormat.W3C);
        activity.Start();

        var (traceParent, traceState) = AllocationOutboxTraceContext.Capture(
            activity, ValidTraceParent, "vendor=value");

        Assert.Equal(activity.Id, traceParent);
        Assert.Equal(activity.TraceStateString, traceState);
        Assert.NotEqual(ValidTraceParent, traceParent);
    }
}
