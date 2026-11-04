namespace ConferenceProFlow.Registration.Api.Features.Conferences;

public sealed record CreateConferenceRequest(
    string? Name,
    DateTimeOffset StartsAtUtc,
    DateTimeOffset EndsAtUtc);

public sealed record ConferenceResponse(
    Guid Id,
    string Name,
    DateTimeOffset StartsAtUtc,
    DateTimeOffset EndsAtUtc);

public sealed record CreateSessionRequest(
    string? Title,
    DateTimeOffset StartsAtUtc,
    DateTimeOffset EndsAtUtc);

public sealed record SessionResponse(
    Guid Id,
    Guid ConferenceId,
    string Title,
    DateTimeOffset StartsAtUtc,
    DateTimeOffset EndsAtUtc,
    bool IsPublished);