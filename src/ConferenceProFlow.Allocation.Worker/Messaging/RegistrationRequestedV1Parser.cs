using System.Text.Json;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;

namespace ConferenceProFlow.Allocation.Worker.Messaging;

public static class RegistrationRequestedV1Parser
{
    public static bool TryParse(
        ReadOnlySpan<byte> json,
        out RegistrationRequestedV1? message,
        out string? error)
    {
        message = null;
        error = null;

        if (json.IsEmpty)
        {
            error = "Message payload is empty.";
            return false;
        }

        RegistrationRequestedV1? parsed;

        try
        {
            parsed = JsonSerializer.Deserialize<RegistrationRequestedV1>(
                json,
                JsonSerializerOptions.Web);
        }
        catch (JsonException)
        {
            error = "Invalid RegistrationRequestedV1 JSON payload.";
            return false;
        }

        if (parsed is null)
        {
            error = "Message payload cannot be null.";
            return false;
        }

        if (parsed.MessageId == Guid.Empty)
        {
            error = "MessageId must not be empty.";
            return false;
        }

        if (parsed.RegistrationId == Guid.Empty)
        {
            error = "RegistrationId must not be empty.";
            return false;
        }

        if (parsed.SessionId == Guid.Empty)
        {
            error = "SessionId must not be empty.";
            return false;
        }

        if (parsed.OccurredAtUtc == default ||
            parsed.OccurredAtUtc.Offset != TimeSpan.Zero)
        {
            error = "OccurredAtUtc must be a valid UTC timestamp.";
            return false;
        }

        message = parsed;

        return true;
    }
}