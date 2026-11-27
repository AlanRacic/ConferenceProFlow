using System.Diagnostics.Metrics;
using ConferenceProFlow.Allocation.Worker.Processing;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>
/// Low-cardinality metrics for individual SQL Inbox processing ATTEMPTS.
/// These are best-effort operational signals, not counts of unique allocations.
/// They never influence retries, SQL writes, acknowledgements or allocation decisions.
/// </summary>
internal static class AllocationInboxMetrics
{
    internal const string MeterName = "ConferenceProFlow.Allocation.Worker.Metrics";

    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> Attempts = Meter.CreateCounter<long>(
        "conferenceproflow.allocation.inbox.processing.attempts",
        unit: "1",
        description: "Completed SQL Inbox processing attempts, including retries, duplicate results and errors; shutdown cancellations excluded.");

    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "conferenceproflow.allocation.inbox.processing.duration",
        unit: "s",
        description: "Elapsed processing time per SQL Inbox attempt in seconds; includes returned results and errors.");

    internal static void RecordResult(AllocationProcessingResult result, TimeSpan elapsed) =>
        Record(result switch
        {
            AllocationProcessingResult.Confirmed => "confirmed",
            AllocationProcessingResult.Rejected => "rejected",
            AllocationProcessingResult.AlreadyProcessed => "already_processed",
            AllocationProcessingResult.ExistingAllocation => "existing_allocation",
            AllocationProcessingResult.WaitingForInventory => "waiting_for_inventory",
            AllocationProcessingResult.NotFound => "not_found",
            _ => "other" // Fixed fallback; never use exception text or IDs as labels.
        }, elapsed);

    internal static void RecordFailure(TimeSpan elapsed) => Record("error", elapsed);

    private static void Record(string result, TimeSpan elapsed)
    {
        var resultTag = new KeyValuePair<string, object?>("result", result);
        Attempts.Add(1, resultTag);

        // A monotonic Stopwatch supplies elapsed; ignore invalid duration rather
        // than emitting a nonsensical negative histogram value.
        if (elapsed >= TimeSpan.Zero)
            Duration.Record(elapsed.TotalSeconds, resultTag);
    }
}
