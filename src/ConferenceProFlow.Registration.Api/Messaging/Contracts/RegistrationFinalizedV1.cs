using System.Text.Json.Serialization;

namespace ConferenceProFlow.Registration.Api.Messaging.Contracts;

/// <summary>
/// A versioned integration event describing a committed transition of a Registration from Pending to a final state. This is a transport contract, not an EF Core entity.
/// </summary>
public sealed record RegistrationFinalizedV1(
    [property: JsonPropertyName("messageId")] Guid MessageId,
    [property: JsonPropertyName("registrationId")] Guid RegistrationId,
    [property: JsonPropertyName("sessionId")] Guid SessionId,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("occurredAtUtc")] DateTimeOffset OccurredAtUtc)
{
    public const string EventType = "registration.finalized.v1";
    public const string ConfirmedStatus = "confirmed";
    public const string RejectedStatus = "rejected";

    /// <summary>
    /// Creates one logical event. Persist the resulting MessageId and payload together in the Analytics Outbox; never call Create again when retrying publication.
    /// </summary>
    public static RegistrationFinalizedV1 Create(
        Guid registrationId,
        Guid sessionId,
        string status,
        DateTimeOffset? occurredAtUtc = null)
    {
        var integrationEvent = new RegistrationFinalizedV1(
            MessageId: Guid.NewGuid(),
            RegistrationId: registrationId,
            SessionId: sessionId,
            Status: status,
            OccurredAtUtc: occurredAtUtc ?? DateTimeOffset.UtcNow);

        integrationEvent.Validate();
        return integrationEvent;
    }

    /// <summary>
    /// Validates both newly created events and events reconstructed from JSON.
    /// </summary>
    public void Validate()
    {
        if (MessageId == Guid.Empty)
            throw new ArgumentException("MessageId must not be empty.", nameof(MessageId));

        if (RegistrationId == Guid.Empty)
            throw new ArgumentException("RegistrationId must not be empty.", nameof(RegistrationId));

        if (SessionId == Guid.Empty)
            throw new ArgumentException("SessionId must not be empty.", nameof(SessionId));

        if (Status is not (ConfirmedStatus or RejectedStatus))
            throw new ArgumentException("Status must be 'confirmed' or 'rejected'.", nameof(Status));

        if (OccurredAtUtc == default || OccurredAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("OccurredAtUtc must have a non-default UTC (+00:00) value.", nameof(OccurredAtUtc));
    }
}
