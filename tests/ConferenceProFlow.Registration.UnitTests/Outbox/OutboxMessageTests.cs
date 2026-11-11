using System.Text.Json;
using ConferenceProFlow.Registration.Api.Features.Registrations.Messages;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Outbox;

public sealed class OutboxMessageTests
{
    [Fact]
    public void Create_WithValidData_InitializesUnpublishedMessage()
    {
        var messageId = Guid.NewGuid();
        var occurredAt = DateTimeOffset.UtcNow;

        var message = OutboxMessage.Create(
            messageId,
            RegistrationRequestedV1.MessageType,
            """{"registrationId":"test"}""",
            occurredAt);

        Assert.Equal(messageId, message.Id);
        Assert.Equal(RegistrationRequestedV1.MessageType, message.MessageType);
        Assert.Equal(occurredAt, message.OccurredAtUtc);

        Assert.Null(message.PublishedAtUtc);
        Assert.Equal(0, message.PublishAttempts);
        Assert.Null(message.LastError);
    }

    [Fact]
    public void Create_WithEmptyMessageId_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            OutboxMessage.Create(
                Guid.Empty,
                RegistrationRequestedV1.MessageType,
                "{}",
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_WithBlankMessageType_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            OutboxMessage.Create(
                Guid.NewGuid(),
                "   ",
                "{}",
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_WithMessageTypeTooLong_ThrowsArgumentException()
    {
        var messageType = new string('A', 161);

        Assert.Throws<ArgumentException>(() =>
            OutboxMessage.Create(
                Guid.NewGuid(),
                messageType,
                "{}",
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_WithEmptyPayload_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            OutboxMessage.Create(
                Guid.NewGuid(),
                RegistrationRequestedV1.MessageType,
                "",
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Create_WithNonUtcTimestamp_ThrowsArgumentException()
    {
        var timestamp = new DateTimeOffset(
            2027, 6, 1, 10, 0, 0,
            TimeSpan.FromHours(2));

        Assert.Throws<ArgumentException>(() =>
            OutboxMessage.Create(
                Guid.NewGuid(),
                RegistrationRequestedV1.MessageType,
                "{}",
                timestamp));
    }

    [Fact]
    public void RegistrationRequestedV1_CanBeSerializedAndDeserialized()
    {
        var integrationEvent = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var json = JsonSerializer.Serialize(integrationEvent, JsonSerializerOptions.Web);

        using var document = JsonDocument.Parse(json);

        Assert.Equal(
            integrationEvent.MessageId,
            document.RootElement
                .GetProperty("messageId")
                .GetGuid());

        var deserialized = JsonSerializer.Deserialize<RegistrationRequestedV1>(
                json,
                JsonSerializerOptions.Web);

        Assert.NotNull(deserialized);
        Assert.Equal(integrationEvent, deserialized);
    }

    [Fact]
    public void MarkPublished_WithValidTimestamp_UpdatesPublishingState()
    {
        var occurredAt = DateTimeOffset.UtcNow;

        var message = OutboxMessage.Create(
            Guid.NewGuid(),
            RegistrationRequestedV1.MessageType,
            "{}",
            occurredAt);

        var publishedAt = occurredAt.AddSeconds(5);

        message.MarkPublished(publishedAt);

        Assert.Equal(publishedAt, message.PublishedAtUtc);
        Assert.Equal(1, message.PublishAttempts);
        Assert.Null(message.LastError);
    }

    [Fact]
    public void RecordFailure_IncrementsAttemptsAndStoresError()
    {
        var message = OutboxMessage.Create(
            Guid.NewGuid(),
            RegistrationRequestedV1.MessageType,
            "{}",
            DateTimeOffset.UtcNow);

        message.RecordFailure("RabbitMQ unavailable.");

        Assert.Equal(1, message.PublishAttempts);
        Assert.Null(message.PublishedAtUtc);
        Assert.Equal("RabbitMQ unavailable.", message.LastError);
    }

    [Fact]
    public void MarkPublished_AfterFailure_ClearsLastError()
    {
        var occurredAt = DateTimeOffset.UtcNow;

        var message = OutboxMessage.Create(
            Guid.NewGuid(),
            RegistrationRequestedV1.MessageType,
            "{}",
            occurredAt);

        message.RecordFailure("Connection failed.");

        message.MarkPublished(occurredAt.AddSeconds(5));

        Assert.Equal(2, message.PublishAttempts);
        Assert.NotNull(message.PublishedAtUtc);
        Assert.Null(message.LastError);
    }

    [Fact]
    public void MarkPublished_Twice_ThrowsInvalidOperationException()
    {
        var occurredAt = DateTimeOffset.UtcNow;

        var message = OutboxMessage.Create(
            Guid.NewGuid(),
            RegistrationRequestedV1.MessageType,
            "{}",
            occurredAt);

        message.MarkPublished(occurredAt.AddSeconds(1));

        Assert.Throws<InvalidOperationException>(() =>
            message.MarkPublished(occurredAt.AddSeconds(2)));
    }
}