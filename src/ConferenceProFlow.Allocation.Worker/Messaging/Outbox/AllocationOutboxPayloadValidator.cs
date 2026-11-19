using System.Text.Json;
using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;

namespace ConferenceProFlow.Allocation.Worker.Messaging.Outbox;

/// <summary>Validates permanent contract errors before attempting transport.</summary>
public static class AllocationOutboxPayloadValidator
{
    public static string? GetPermanentError(AllocationOutboxMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message.MessageType != AllocationDecidedV1.MessageType)
            return "Unsupported allocation result message type.";

        AllocationDecidedV1? value;
        try
        {
            value = JsonSerializer.Deserialize<AllocationDecidedV1>(
                message.Payload, JsonSerializerOptions.Web);
        }
        catch (JsonException)
        {
            return "Invalid AllocationDecidedV1 JSON payload.";
        }

        if (value is null)
            return "AllocationDecidedV1 payload is null.";
        if (value.MessageId != message.Id)
            return "Payload MessageId does not match Outbox Id.";
        if (value.AllocationId != message.AllocationId)
            return "Payload AllocationId does not match Outbox AllocationId.";
        if (value.RegistrationId == Guid.Empty || value.SessionId == Guid.Empty)
            return "RegistrationId and SessionId must not be empty.";
        if (value.OccurredAtUtc == default || value.OccurredAtUtc.Offset != TimeSpan.Zero)
            return "OccurredAtUtc must be a valid UTC timestamp.";
        if (value.OccurredAtUtc != message.OccurredAtUtc)
            return "Payload OccurredAtUtc does not match Outbox OccurredAtUtc.";

        switch (value.Status)
        {
            case AllocationDecidedV1.ConfirmedStatus:
                return value.RejectionReason is null
                    ? null
                    : "Confirmed allocation must not have a rejection reason.";

            case AllocationDecidedV1.RejectedStatus:
                return string.IsNullOrWhiteSpace(value.RejectionReason)
                    ? "Rejected allocation must have a rejection reason."
                    : null;

            default:
                return "Allocation result status must be 'confirmed' or 'rejected'.";
        }
    }
}
