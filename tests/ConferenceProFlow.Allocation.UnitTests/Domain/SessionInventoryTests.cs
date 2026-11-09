using ConferenceProFlow.Allocation.Worker.Domain.Entities;

namespace ConferenceProFlow.Allocation.UnitTests.Domain;

public sealed class SessionInventoryTests
{
    [Fact]
    public void Create_WithValidData_InitializesInventory()
    {
        var sessionId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow;

        var inventory = SessionInventory.Create(sessionId, 100, createdAt);

        Assert.Equal(sessionId, inventory.SessionId);
        Assert.Equal(100, inventory.Capacity);
        Assert.Equal(0, inventory.ReservedCount);
        Assert.Equal(createdAt, inventory.CreatedAtUtc);
    }

    [Fact]
    public void Create_WithEmptySessionId_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            SessionInventory.Create(Guid.Empty, 100, DateTimeOffset.UtcNow));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Create_WithNonPositiveCapacity_ThrowsArgumentOutOfRangeException(int capacity)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SessionInventory.Create(Guid.NewGuid(), capacity, DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_WithNonUtcTimestamp_ThrowsArgumentException()
    {
        var timestamp = new DateTimeOffset(
            2027, 6, 1, 10, 0, 0,
            TimeSpan.FromHours(2));

        Assert.Throws<ArgumentException>(() =>
            SessionInventory.Create(Guid.NewGuid(), 100, timestamp));
    }
}