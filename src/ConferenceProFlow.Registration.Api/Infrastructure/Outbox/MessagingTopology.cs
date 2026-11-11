using ConferenceProFlow.Registration.Api.Features.Registrations.Messages;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

public static class MessagingTopology
{
    public const string ExchangeName = "conferenceproflow.events";

    public const string RegistrationRequestsQueue = "allocation.registration-requests.v1";

    public const string RegistrationRequestedRoutingKey = RegistrationRequestedV1.MessageType;
}