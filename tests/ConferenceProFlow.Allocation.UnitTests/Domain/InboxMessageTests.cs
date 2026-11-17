using ConferenceProFlow.Allocation.Worker.Domain.Entities;

namespace ConferenceProFlow.Allocation.UnitTests.Domain;

public sealed class InboxMessageTests
{
    [Fact]
    public void Create_WithValidData_InitializesInboxMessage()
    {
        var messageId = Guid.NewGuid();
        var registrationId = Guid.NewGuid();
        var sessionId = Guid.NewGuid();

        var occurredAtUtc = new DateTimeOffset(
            2026, 11, 13, 10, 0, 0, TimeSpan.Zero);
        var receivedAtUtc = occurredAtUtc.AddSeconds(5);

        var message = InboxMessage.Create(
            messageId,
            registrationId,
            sessionId,
            occurredAtUtc,
            receivedAtUtc);

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

    [Fact]
    public void MarkProcessed_WithUtcTimestamp_SetsProcessedAtUtc()
    {
        var receivedAtUtc = DateTimeOffset.UtcNow;
        var message = CreateMessage(receivedAtUtc);
        var processedAtUtc = receivedAtUtc.AddSeconds(1);

        message.MarkProcessed(processedAtUtc);

        Assert.Equal(processedAtUtc, message.ProcessedAtUtc);
    }

    [Fact]
    public void MarkProcessed_WhenAlreadyProcessed_ThrowsInvalidOperationException()
    {
        var receivedAtUtc = DateTimeOffset.UtcNow;
        var message = CreateMessage(receivedAtUtc);
        message.MarkProcessed(receivedAtUtc.AddSeconds(1));

        Assert.Throws<InvalidOperationException>(() => message.MarkProcessed(receivedAtUtc.AddSeconds(2)));
    }

    [Fact]
    public void MarkProcessed_WithNonUtcTimestamp_ThrowsArgumentException()
    {
        var message = CreateMessage(DateTimeOffset.UtcNow);
        var nonUtcTimestamp = new DateTimeOffset(
            2026, 11, 13, 11, 0, 0,
            TimeSpan.FromHours(1));

        Assert.Throws<ArgumentException>(() => message.MarkProcessed(nonUtcTimestamp));

        Assert.Null(message.ProcessedAtUtc);
    }

    private static InboxMessage CreateMessage(DateTimeOffset receivedAtUtc)
    {
        return InboxMessage.Create(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            receivedAtUtc,
            receivedAtUtc);
    }
}