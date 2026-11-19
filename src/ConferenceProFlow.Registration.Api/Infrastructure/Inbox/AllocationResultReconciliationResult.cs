namespace ConferenceProFlow.Registration.Api.Infrastructure.Inbox;

public enum AllocationResultReconciliationResult
{
    Applied,
    DuplicateMessage,
    DuplicateDecision,
    Conflict,
    UnknownRegistration
}