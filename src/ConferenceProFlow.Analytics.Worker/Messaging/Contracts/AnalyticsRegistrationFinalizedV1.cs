using System.Text.Json.Serialization;

namespace ConferenceProFlow.Analytics.Worker.Messaging.Contracts;

/// <summary>
/// Analytics-owned representation of the RegistrationFinalizedV1 wire contract.
/// Keep JSON field names and status semantics aligned with the Registration API;
/// do not reference the Registration API project from this independent worker.
/// </summary>
public sealed record AnalyticsRegistrationFinalizedV1(
    [property: JsonPropertyName("messageId")] Guid MessageId,
    [property: JsonPropertyName("registrationId")] Guid RegistrationId,
    [property: JsonPropertyName("sessionId")] Guid SessionId,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("occurredAtUtc")] DateTimeOffset OccurredAtUtc)
{
    public const string ConfirmedStatus = "confirmed";
    public const string RejectedStatus = "rejected";

    public void Validate()
    {
        if (MessageId == Guid.Empty)
            throw new ArgumentException("messageId must not be empty.", nameof(MessageId));
        if (RegistrationId == Guid.Empty)
            throw new ArgumentException("registrationId must not be empty.", nameof(RegistrationId));
        if (SessionId == Guid.Empty)
            throw new ArgumentException("sessionId must not be empty.", nameof(SessionId));
        if (Status is not (ConfirmedStatus or RejectedStatus))
            throw new ArgumentException("status must be 'confirmed' or 'rejected'.", nameof(Status));
        if (OccurredAtUtc == default || OccurredAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("occurredAtUtc must be a non-default UTC (+00:00) timestamp.",
                nameof(OccurredAtUtc));
    }
}
