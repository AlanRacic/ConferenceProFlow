using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;

namespace ConferenceProFlow.Allocation.Worker.Messaging;

public static class MessagingTopology
{
    public const string ExchangeName = "conferenceproflow.events";

    public const string RegistrationRequestsQueue = "allocation.registration-requests.v1";

    public const string FailedRegistrationRequestsQueue = "allocation.failed-registration-requests.v1";

    public const string RegistrationRequestedRoutingKey = RegistrationRequestedV1.MessageType;
}