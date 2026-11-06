namespace ConferenceProFlow.Registration.Api.Features.Participants;

public sealed record CreateParticipantRequest(string? DisplayName, string? Email);

public sealed record ParticipantResponse(Guid Id, string DisplayName, string Email);