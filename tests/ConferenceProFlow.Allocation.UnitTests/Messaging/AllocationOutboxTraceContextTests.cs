using ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

namespace ConferenceProFlow.Allocation.UnitTests.Messaging;

public sealed class AllocationOutboxTraceContextTests
{
    private const string ValidTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    [Fact]
    public void Create_WithValidContext_StoresTraceMetadata()
    {
        var message = Create(ValidTraceParent, "vendor=value");

        Assert.Equal(ValidTraceParent, message.TraceParent);
        Assert.Equal("vendor=value", message.TraceState);
    }

    [Fact]
    public void Create_LegacyCall_IsSupported()
    {
        var message = AllocationOutboxMessage.Create(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "allocation.decided.v1",
            "{}",
            DateTimeOffset.UtcNow);

        Assert.Null(message.TraceParent);
        Assert.Null(message.TraceState);
    }

    [Fact]
    public void Create_TraceParentOnly_IsSupported()
    {
        var message = Create(ValidTraceParent);

        Assert.Equal(ValidTraceParent, message.TraceParent);
        Assert.Null(message.TraceState);
    }

    [Fact]
    public void Create_InvalidParent_IsRejected()
    {
        Assert.Throws<ArgumentException>(() => Create("invalid"));
    }

    [Fact]
    public void Create_TraceStateWithoutParent_IsRejected()
    {
        Assert.Throws<ArgumentException>(() => Create(traceState: "vendor=value"));
    }

    [Fact]
    public void Create_OversizedTraceState_IsRejected()
    {
        Assert.Throws<ArgumentException>(() =>
            Create(ValidTraceParent, new string('x', 513)));
    }

    [Fact]
    public void RecordFailure_DoesNotModifyTraceContext()
    {
        var message = Create(ValidTraceParent, "vendor=value");

        message.RecordFailure("temporary broker failure", DateTimeOffset.UtcNow.AddSeconds(1));

        Assert.Equal(ValidTraceParent, message.TraceParent);
        Assert.Equal("vendor=value", message.TraceState);
        Assert.Equal(1, message.PublishAttempts);
        Assert.Null(message.PublishedAtUtc);
    }

    [Fact]
    public void MarkPublished_DoesNotModifyTraceContext()
    {
        var message = Create(ValidTraceParent, "vendor=value");

        message.MarkPublished(DateTimeOffset.UtcNow.AddSeconds(1));

        Assert.Equal(ValidTraceParent, message.TraceParent);
        Assert.Equal("vendor=value", message.TraceState);
        Assert.NotNull(message.PublishedAtUtc);
    }

    private static AllocationOutboxMessage Create(
        string? traceParent = null,
        string? traceState = null)
    {
        return AllocationOutboxMessage.Create(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "allocation.decided.v1",
            "{}",
            DateTimeOffset.UtcNow,
            traceParent,
            traceState);
    }
}
