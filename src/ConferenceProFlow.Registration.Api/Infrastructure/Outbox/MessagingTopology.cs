using ConferenceProFlow.Registration.Api.Features.Registrations.Messages;
using ConferenceProFlow.Registration.Api.Messaging.Contracts;

namespace ConferenceProFlow.Registration.Api.Infrastructure.Outbox;

/// <summary>
/// Logical RabbitMQ topology. Registration declares its own result queue.
/// </summary>
public static class MessagingTopology
{
    public const string ExchangeName = "conferenceproflow.events";

    // Registration requests are consumed by the Allocation Worker.
    public const string RegistrationRequestsQueue = "allocation.registration-requests.v1";
    public const string RegistrationRequestedRoutingKey = RegistrationRequestedV1.MessageType;

    // Allocation decisions are consumed by the Registration API.
    public const string RegistrationAllocationResultsQueue = "registration.allocation-results.v1";
    public const string AllocationDecidedRoutingKey = AllocationDecidedV1.MessageType;

    // Registration-owned parking queue for malformed or conflicting results.
    public const string FailedAllocationResultsQueue = "registration.failed-allocation-results.v1";
}