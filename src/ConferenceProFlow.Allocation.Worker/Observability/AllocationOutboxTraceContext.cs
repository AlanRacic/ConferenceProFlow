using System.Diagnostics;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>
/// Captures a valid trace context for the durable Allocation Result Outbox.
/// Prefer the active Allocation processing span. If it is not being recorded,
/// fall back to the context already stored in the incoming SQL Inbox row.
/// Invalid telemetry must never break business processing.
/// </summary>
internal static class AllocationOutboxTraceContext
{
    internal static (string? TraceParent, string? TraceState) Capture(
        Activity? processingActivity,
        string? inboxTraceParent,
        string? inboxTraceState)
    {
        if (processingActivity is { IdFormat: ActivityIdFormat.W3C } &&
            IsValid(processingActivity.Id, processingActivity.TraceStateString))
        {
            return (processingActivity.Id, processingActivity.TraceStateString);
        }

        if (IsValid(inboxTraceParent, inboxTraceState))
        {
            return (inboxTraceParent, inboxTraceState);
        }

        return (null, null);
    }

    private static bool IsValid(string? traceParent, string? traceState)
    {
        return traceParent is { Length: 55 } &&
               traceState?.Length is not > 512 &&
               ActivityContext.TryParse(traceParent, traceState, out _);
    }
}
