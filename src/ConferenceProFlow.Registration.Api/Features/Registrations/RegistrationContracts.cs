namespace ConferenceProFlow.Registration.Api.Features.Registrations;

public sealed record CreateRegistrationRequest(
    Guid ParticipantId,
    Guid SessionId);

public sealed record RegistrationResponse(
    Guid Id,
    Guid ParticipantId,
    Guid SessionId,
    string Status,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc,
    string? RejectionReason);