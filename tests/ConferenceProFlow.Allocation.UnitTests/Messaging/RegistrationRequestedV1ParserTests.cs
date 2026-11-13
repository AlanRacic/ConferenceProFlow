using System.Text;
using System.Text.Json;
using ConferenceProFlow.Allocation.Worker.Messaging;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;

namespace ConferenceProFlow.Allocation.UnitTests.Messaging;

public sealed class RegistrationRequestedV1ParserTests
{
    [Fact]
    public void TryParse_ValidMessage_ReturnsTrue()
    {
        // Arrange
        var expected = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var json = JsonSerializer.SerializeToUtf8Bytes(
            expected,
            JsonSerializerOptions.Web);

        // Act
        var success = RegistrationRequestedV1Parser.TryParse(
            json,
            out var actual,
            out var error);

        // Assert
        Assert.True(success);
        Assert.Null(error);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void TryParse_InvalidJson_ReturnsFalse()
    {
        // Arrange
        var json = Encoding.UTF8.GetBytes("{ invalid json");

        // Act
        var success = RegistrationRequestedV1Parser.TryParse(
            json,
            out var message,
            out var error);

        // Assert
        Assert.False(success);
        Assert.Null(message);
        Assert.NotNull(error);
    }

    [Fact]
    public void TryParse_EmptyPayload_ReturnsFalse()
    {
        // Act
        var success = RegistrationRequestedV1Parser.TryParse(
            ReadOnlySpan<byte>.Empty,
            out var message,
            out var error);

        // Assert
        Assert.False(success);
        Assert.Null(message);
        Assert.Equal("Message payload is empty.", error);
    }

    [Fact]
    public void TryParse_MissingIdentifiers_ReturnsFalse()
    {
        // Arrange
        var json = Encoding.UTF8.GetBytes("{}");

        // Act
        var success = RegistrationRequestedV1Parser.TryParse(
            json,
            out var message,
            out var error);

        // Assert
        Assert.False(success);
        Assert.Null(message);
        Assert.Contains("MessageId", error);
    }

    [Fact]
    public void TryParse_EmptyRegistrationId_ReturnsFalse()
    {
        // Arrange
        var integrationEvent = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.Empty,
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var json = JsonSerializer.SerializeToUtf8Bytes(
            integrationEvent,
            JsonSerializerOptions.Web);

        // Act
        var success = RegistrationRequestedV1Parser.TryParse(
            json,
            out var message,
            out var error);

        // Assert
        Assert.False(success);
        Assert.Null(message);
        Assert.Contains("RegistrationId", error);
    }

    [Fact]
    public void TryParse_NonUtcTimestamp_ReturnsFalse()
    {
        // Arrange
        var integrationEvent = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            new DateTimeOffset(
                2026, 11, 13, 9, 0, 0,
                TimeSpan.FromHours(1)));

        var json = JsonSerializer.SerializeToUtf8Bytes(
            integrationEvent,
            JsonSerializerOptions.Web);

        // Act
        var success = RegistrationRequestedV1Parser.TryParse(
            json,
            out var message,
            out var error);

        // Assert
        Assert.False(success);
        Assert.Null(message);
        Assert.Contains("UTC", error);
    }

    [Fact]
    public void TryParse_RegistrationApiJsonContract_ReturnsExpectedValues()
    {
        // Arrange
        // Represents the JSON format published by Registration API.
        const string json = """
        {
          "messageId": "d76314db-f2c8-4d84-b40c-b801e2f6be3a",
          "registrationId": "6ed94d51-f3db-434f-9923-d9e27cf7813f",
          "sessionId": "765f56ae-0f5f-4c76-a52c-a4195e1d344c",
          "occurredAtUtc": "2026-11-09T10:00:00+00:00"
        }
        """;

        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var success = RegistrationRequestedV1Parser.TryParse(
            bytes,
            out var message,
            out var error);

        // Assert
        Assert.True(success);
        Assert.Null(error);
        Assert.NotNull(message);

        Assert.Equal(
            Guid.Parse("d76314db-f2c8-4d84-b40c-b801e2f6be3a"),
            message.MessageId);

        Assert.Equal(
            Guid.Parse("6ed94d51-f3db-434f-9923-d9e27cf7813f"),
            message.RegistrationId);

        Assert.Equal(
            Guid.Parse("765f56ae-0f5f-4c76-a52c-a4195e1d344c"),
            message.SessionId);

        Assert.Equal(
            TimeSpan.Zero,
            message.OccurredAtUtc.Offset);
    }
}