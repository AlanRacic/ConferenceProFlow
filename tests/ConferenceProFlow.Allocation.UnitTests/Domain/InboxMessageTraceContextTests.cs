using ConferenceProFlow.Allocation.Worker.Domain.Entities;

namespace ConferenceProFlow.Allocation.UnitTests.Domain;

public sealed class InboxMessageTraceContextTests
{
    private const string ValidTraceParent =
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    [Fact]
    public void Create_WithValidContext_PersistsBothHeaders()
    {
        var message = Create(traceParent: ValidTraceParent, traceState: "vendor=value");

        Assert.Equal(ValidTraceParent, message.TraceParent);
        Assert.Equal("vendor=value", message.TraceState);
    }

    [Fact]
    public void Create_LegacyCall_LeavesContextNull()
    {
        // Deliberately use the original five-argument factory call.
        var message = InboxMessage.Create(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow);

        Assert.Null(message.TraceParent);
        Assert.Null(message.TraceState);
    }

    [Fact]
    public void Create_WithTraceParentOnly_PreservesParent()
    {
        var message = Create(traceParent: ValidTraceParent);

        Assert.Equal(ValidTraceParent, message.TraceParent);
        Assert.Null(message.TraceState);
    }

    [Fact]
    public void Create_InvalidTraceParent_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            Create(traceParent: "invalid-traceparent"));
    }

    [Fact]
    public void Create_TraceStateWithoutParent_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            Create(traceState: "vendor=value"));
    }

    [Fact]
    public void Create_OversizedTraceState_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            Create(traceParent: ValidTraceParent, traceState: new string('a', 513)));
    }

    private static InboxMessage Create(string? traceParent = null, string? traceState = null)
    {
        return InboxMessage.Create(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            traceParent,
            traceState);
    }
}
