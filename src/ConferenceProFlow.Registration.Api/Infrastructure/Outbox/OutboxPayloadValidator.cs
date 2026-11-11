using System.Text.Json;
using ConferenceProFlow.Registration.Api.Features.Registrations.Messages;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

public static class OutboxPayloadValidator
{
    public static string? GetPermanentError(OutboxMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        // Validate the contract currently owned by this publisher.
        // Other message types are not validated by this rule.
        if (message.MessageType != RegistrationRequestedV1.MessageType)
        {
            return null;
        }

        RegistrationRequestedV1? integrationEvent;

        try
        {
            integrationEvent = JsonSerializer.Deserialize<RegistrationRequestedV1>(
                    message.Payload,
                    JsonSerializerOptions.Web);
        }
        catch (JsonException)
        {
            return "Invalid RegistrationRequestedV1 JSON payload.";
        }

        if (integrationEvent is null)
        {
            return "RegistrationRequestedV1 payload is null.";
        }

        if (integrationEvent.MessageId != message.Id)
        {
            return "Payload MessageId does not match Outbox message ID.";
        }

        if (integrationEvent.RegistrationId == Guid.Empty)
        {
            return "RegistrationId must not be empty.";
        }

        if (integrationEvent.SessionId == Guid.Empty)
        {
            return "SessionId must not be empty.";
        }

        if (integrationEvent.OccurredAtUtc == default ||
            integrationEvent.OccurredAtUtc.Offset != TimeSpan.Zero)
        {
            return "OccurredAtUtc must be a valid UTC timestamp.";
        }

        return null;
    }
}