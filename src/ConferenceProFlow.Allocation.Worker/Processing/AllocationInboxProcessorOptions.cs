namespace ConferenceProFlow.Allocation.Worker.Processing;

public sealed class AllocationInboxProcessorOptions
{
    public const string SectionName = "AllocationInboxProcessor";

    // Opt-in to avoid changing the behavior of existing local environments.
    public bool Enabled { get; set; }

    public int PollIntervalSeconds { get; set; } = 5;

    public int BatchSize { get; set; } = 20;

    public int FailureRetrySeconds { get; set; } = 30;
}
