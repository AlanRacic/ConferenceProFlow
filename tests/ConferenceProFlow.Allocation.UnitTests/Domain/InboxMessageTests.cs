using ConferenceProFlow.Allocation.Worker.Domain.Entities;

namespace ConferenceProFlow.Allocation.UnitTests.Domain;

public sealed class InboxMessageTests
{
    [Fact]
    public void Create_WithValidData_InitializesInboxMessage()
    {
        // Arrange
        var messageId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();
        var sessionId = Guid.NewGuid();

        var occurredAtUtc = new DateTimeOffset(
            2026, 11, 13, 10, 0, 0,
            TimeSpan.Zero);

        var receivedAtUtc = occurredAtUtc.AddSeconds(5);

        // Act
        var message = InboxMessage.Create(
            messageId,
            registrationId,
            sessionId,
            occurredAtUtc,
            receivedAtUtc);

        // Assert
        Assert.Equal(messageId, message.Id);
        Assert.Equal(registrationId, message.RegistrationId);
        Assert.Equal(sessionId, message.SessionId);

        Assert.Equal(occurredAtUtc, message.OccurredAtUtc);

        Assert.Equal(receivedAtUtc, message.ReceivedAtUtc);

        Assert.Null(message.ProcessedAtUtc);
    }

    [Fact]
    public void Create_WithEmptyMessageId_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            InboxMessage.Create(
                Guid.Empty,
                Guid.NewGuid(),
                Guid.NewGuid(),
                DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_WithNonUtcReceivedAt_ThrowsArgumentException()
    {
        var nonUtcTimestamp = new DateTimeOffset(
            2026, 11, 13, 11, 0, 0,
            TimeSpan.FromHours(1));

        Assert.Throws<ArgumentException>(() =>
            InboxMessage.Create(
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                DateTimeOffset.UtcNow,
                nonUtcTimestamp));
    }
}