namespace ConferenceProFlow.Allocation.Worker.Messaging.Inbox;

public enum InboxReceiveResult
{
    Stored = 1,

    DuplicateMessage = 2,

    DuplicateRegistration = 3,

    Conflict = 4
}