using System.Diagnostics;
using ConferenceProFlow.Allocation.Worker.Observability;

namespace ConferenceProFlow.Allocation.UnitTests.Observability;

public sealed class AllocationInboxProcessingTelemetryTests
{
    private const string ValidTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    [Fact]
    public void StartActivity_ValidPersistedContext_ProvidesCausalLink()
    {
        using var listener = CreateListener();
        Assert.True(ActivityContext.TryParse(ValidTraceParent, null, out var expected));

        using var activity = AllocationInboxProcessingTelemetry.StartActivity(
            ValidTraceParent, null);

        Assert.NotNull(activity);
        Assert.Equal(ActivityKind.Internal, activity.Kind);
        Assert.Equal(AllocationInboxProcessingTelemetry.OperationName, activity.DisplayName);

        var link = Assert.Single(activity.Links);
        Assert.Equal(expected.TraceId, link.Context.TraceId);
        Assert.Equal(expected.SpanId, link.Context.SpanId);
    }

    [Fact]
    public void StartActivity_ValidTraceState_IsPreservedOnLink()
    {
        using var listener = CreateListener();

        using var activity = AllocationInboxProcessingTelemetry.StartActivity(
            ValidTraceParent, "vendor=value");

        Assert.NotNull(activity);
        Assert.Equal("vendor=value", Assert.Single(activity.Links).Context.TraceState);
    }

    [Fact]
    public void StartActivity_LegacyMessageWithoutContext_IsUnlinked()
    {
        using var listener = CreateListener();

        using var activity = AllocationInboxProcessingTelemetry.StartActivity(null, null);

        Assert.NotNull(activity);
        Assert.Empty(activity.Links);
    }

    [Fact]
    public void StartActivity_InvalidParent_IsUnlinked()
    {
        using var listener = CreateListener();

        using var activity = AllocationInboxProcessingTelemetry.StartActivity(
            "not-a-valid-traceparent", null);

        Assert.NotNull(activity);
        Assert.Empty(activity.Links);
    }

    [Fact]
    public void StartActivity_TraceStateWithoutParent_IsUnlinked()
    {
        using var listener = CreateListener();

        using var activity = AllocationInboxProcessingTelemetry.StartActivity(
            null, "vendor=value");

        Assert.NotNull(activity);
        Assert.Empty(activity.Links);
    }

    [Fact]
    public void StartActivity_OversizedTraceState_IsUnlinked()
    {
        using var listener = CreateListener();

        using var activity = AllocationInboxProcessingTelemetry.StartActivity(
            ValidTraceParent, new string('a', 513));

        Assert.NotNull(activity);
        Assert.Empty(activity.Links);
    }

    private static ActivityListener CreateListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AllocationTelemetry.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };

        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
