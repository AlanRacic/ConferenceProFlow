namespace ConferenceProFlow.Registration.Api.Features.Registrations.Messages;

public sealed record RegistrationRequestedV1(
    Guid MessageId,
    Guid RegistrationId,
    Guid SessionId,
    DateTimeOffset OccurredAtUtc)
{
    public const string MessageType = "registration.requested.v1";
}