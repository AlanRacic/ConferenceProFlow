using System.Text.Json;
using ConferenceProFlow.Registration.Api.Features.Registrations.Messages;
using ConferenceProFlow.Registration.Api.Infrastructure.Outbox;
using Xunit;

namespace ConferenceProFlow.Registration.UnitTests.Outbox;

public sealed class OutboxPayloadValidatorTests
{
    [Fact]
    public void GetPermanentError_ValidMessage_ReturnsNull()
    {
        var integrationEvent = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var payload = JsonSerializer.Serialize(integrationEvent, JsonSerializerOptions.Web);

        var message = OutboxMessage.Create(
            integrationEvent.MessageId,
            RegistrationRequestedV1.MessageType,
            payload,
            integrationEvent.OccurredAtUtc);

        var error = OutboxPayloadValidator.GetPermanentError(message);

        Assert.Null(error);
    }

    [Fact]
    public void GetPermanentError_InvalidJson_ReturnsError()
    {
        var message = OutboxMessage.Create(
            Guid.NewGuid(),
            RegistrationRequestedV1.MessageType,
            "{ invalid json",
            DateTimeOffset.UtcNow);

        var error = OutboxPayloadValidator.GetPermanentError(message);

        Assert.NotNull(error);
    }

    [Fact]
    public void GetPermanentError_MismatchedMessageId_ReturnsError()
    {
        var integrationEvent = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var payload = JsonSerializer.Serialize(integrationEvent, JsonSerializerOptions.Web);

        var message = OutboxMessage.Create(
            Guid.NewGuid(),
            RegistrationRequestedV1.MessageType,
            payload,
            integrationEvent.OccurredAtUtc);

        var error = OutboxPayloadValidator.GetPermanentError(message);

        Assert.NotNull(error);
        Assert.Contains("MessageId", error);
    }

    [Fact]
    public void GetPermanentError_EmptyRegistrationId_ReturnsError()
    {
        var integrationEvent = new RegistrationRequestedV1(
            Guid.NewGuid(),
            Guid.Empty,
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var payload = JsonSerializer.Serialize(
            integrationEvent,
            JsonSerializerOptions.Web);

        var message = OutboxMessage.Create(
            integrationEvent.MessageId,
            RegistrationRequestedV1.MessageType,
            payload,
            integrationEvent.OccurredAtUtc);

        var error = OutboxPayloadValidator.GetPermanentError(message);

        Assert.NotNull(error);
        Assert.Contains("RegistrationId", error);
    }
}