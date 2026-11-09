using AllocationEntity = ConferenceProFlow.Allocation.Worker.Domain.Entities.Allocation;
using ConferenceProFlow.Allocation.Worker.Domain.Enums;

namespace ConferenceProFlow.Allocation.UnitTests.Domain;

public sealed class AllocationTests
{
    [Fact]
    public void CreateConfirmed_WithValidData_CreatesConfirmedAllocation()
    {
        var registrationId = Guid.NewGuid();
        var sessionId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow;

        var allocation = AllocationEntity.CreateConfirmed(
            registrationId,
            sessionId,
            createdAt);

        Assert.NotEqual(Guid.Empty, allocation.Id);
        Assert.Equal(registrationId, allocation.RegistrationId);
        Assert.Equal(sessionId, allocation.SessionId);
        Assert.Equal(AllocationStatus.Confirmed, allocation.Status);
        Assert.Null(allocation.RejectionReason);
        Assert.Equal(createdAt, allocation.CreatedAtUtc);
    }

    [Fact]
    public void CreateRejected_WithValidReason_CreatesRejectedAllocation()
    {
        var allocation = AllocationEntity.CreateRejected(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Session capacity reached.",
            DateTimeOffset.UtcNow);

        Assert.Equal(AllocationStatus.Rejected, allocation.Status);
        Assert.Equal("Session capacity reached.", allocation.RejectionReason);
    }

    [Fact]
    public void CreateRejected_TrimsRejectionReason()
    {
        var allocation = AllocationEntity.CreateRejected(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "  Session is full.  ",
            DateTimeOffset.UtcNow);

        Assert.Equal("Session is full.", allocation.RejectionReason);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void CreateRejected_WithEmptyReason_ThrowsArgumentException(string reason)
    {
        Assert.Throws<ArgumentException>(() =>
            AllocationEntity.CreateRejected(
                Guid.NewGuid(),
                Guid.NewGuid(),
                reason,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void CreateRejected_WithReasonTooLong_ThrowsArgumentException()
    {
        var reason = new string('A', 501);

        Assert.Throws<ArgumentException>(() =>
            AllocationEntity.CreateRejected(
                Guid.NewGuid(),
                Guid.NewGuid(),
                reason,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void CreateConfirmed_WithEmptyRegistrationId_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            AllocationEntity.CreateConfirmed(
                Guid.Empty,
                Guid.NewGuid(),
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void CreateConfirmed_WithEmptySessionId_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            AllocationEntity.CreateConfirmed(
                Guid.NewGuid(),
                Guid.Empty,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void CreateConfirmed_WithNonUtcTimestamp_ThrowsArgumentException()
    {
        var timestamp = new DateTimeOffset(
            2027, 6, 1, 10, 0, 0,
            TimeSpan.FromHours(2));

        Assert.Throws<ArgumentException>(() =>
            AllocationEntity.CreateConfirmed(
                Guid.NewGuid(),
                Guid.NewGuid(),
                timestamp));
    }
}