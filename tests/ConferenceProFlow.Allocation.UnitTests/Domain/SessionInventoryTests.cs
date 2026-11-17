using ConferenceProFlow.Allocation.Worker.Domain.Entities;

namespace ConferenceProFlow.Allocation.UnitTests.Domain;

public sealed class SessionInventoryTests
{
    [Fact]
    public void Create_WithValidData_InitializesInventory()
    {
        var sessionId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow;

        var inventory = SessionInventory.Create(
            sessionId,
            100,
            createdAt);

        Assert.Equal(sessionId, inventory.SessionId);
        Assert.Equal(100, inventory.Capacity);
        Assert.Equal(0, inventory.ReservedCount);
        Assert.Equal(createdAt, inventory.CreatedAtUtc);
    }

    [Fact]
    public void Create_WithEmptySessionId_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => SessionInventory.Create(
                Guid.Empty,
                100,
                DateTimeOffset.UtcNow));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Create_WithNonPositiveCapacity_ThrowsArgumentOutOfRangeException(int capacity)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => SessionInventory.Create(
                Guid.NewGuid(),
                capacity,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_WithNonUtcTimestamp_ThrowsArgumentException()
    {
        var timestamp = new DateTimeOffset(
            2027, 6, 1, 10, 0, 0,
            TimeSpan.FromHours(2));

        Assert.Throws<ArgumentException>(() => SessionInventory.Create(
                Guid.NewGuid(),
                100,
                timestamp));
    }

    [Fact]
    public void TryReserveSeat_WhenSeatAvailable_IncrementsReservedCount()
    {
        // Arrange
        var inventory = SessionInventory.Create(
            Guid.NewGuid(),
            10,
            DateTimeOffset.UtcNow);

        // Act
        var result = inventory.TryReserveSeat();

        // Assert
        Assert.True(result);
        Assert.Equal(1, inventory.ReservedCount);
        Assert.Equal(10, inventory.Capacity);
    }

    [Fact]
    public void TryReserveSeat_WhenSessionFull_ReturnsFalse()
    {
        // Arrange
        var inventory = SessionInventory.Create(
            Guid.NewGuid(),
            1,
            DateTimeOffset.UtcNow);

        inventory.TryReserveSeat();

        // Act
        var result = inventory.TryReserveSeat();

        // Assert
        Assert.False(result);
        Assert.Equal(1, inventory.ReservedCount);
        Assert.Equal(1, inventory.Capacity);
    }

    [Fact]
    public void TryReserveSeat_LastAvailableSeat_CanBeReservedOnlyOnce()
    {
        // Arrange
        var inventory = SessionInventory.Create(
            Guid.NewGuid(),
            2,
            DateTimeOffset.UtcNow);

        var firstReservation = inventory.TryReserveSeat();

        // Act
        var secondReservation = inventory.TryReserveSeat();

        var thirdReservation = inventory.TryReserveSeat();

        // Assert
        Assert.True(firstReservation);
        Assert.True(secondReservation);
        Assert.False(thirdReservation);

        Assert.Equal(2, inventory.ReservedCount);
        Assert.Equal(2, inventory.Capacity);
    }

    [Fact]
    public void TryReserveSeat_MultipleAttempts_NeverExceedsCapacity()
    {
        // Arrange
        var capacity = 5;

        var inventory = SessionInventory.Create(
            Guid.NewGuid(),
            capacity,
            DateTimeOffset.UtcNow);

        var successfulReservations = 0;

        // Act
        for (var attempt = 0; attempt < 20; attempt++)
        {
            if (inventory.TryReserveSeat())
            {
                successfulReservations++;
            }
        }

        // Assert
        Assert.Equal(capacity, successfulReservations);
        Assert.Equal(capacity, inventory.ReservedCount);
    }
}