using ConferenceProFlow.Allocation.Worker.Messaging.Contracts;

namespace ConferenceProFlow.Allocation.Worker.Messaging;

public static class MessagingTopology
{
    public const string ExchangeName = "conferenceproflow.events";

    public const string RegistrationRequestsQueue = "allocation.registration-requests.v1";
    public const string FailedRegistrationRequestsQueue = "allocation.failed-registration-requests.v1";
    public const string RegistrationRequestedRoutingKey = RegistrationRequestedV1.MessageType;

    // Registration service owns this queue; the Allocation publisher does not declare it. It will be declared/bound by the consumer in M2.6D.
    public const string RegistrationAllocationResultsQueue = "registration.allocation-results.v1";
    public const string AllocationDecidedRoutingKey = AllocationDecidedV1.MessageType;
}