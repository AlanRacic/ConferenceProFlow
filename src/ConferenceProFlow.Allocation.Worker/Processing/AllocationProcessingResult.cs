namespace ConferenceProFlow.Allocation.Worker.Processing;

public enum AllocationProcessingResult
{
    Confirmed,
    Rejected,
    AlreadyProcessed,
    ExistingAllocation,
    WaitingForInventory,
    NotFound
}