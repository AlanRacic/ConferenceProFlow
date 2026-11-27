using System.Diagnostics;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>
/// Starts an Allocation Inbox processing span. The SQL Inbox is a durable
/// asynchronous boundary, so a valid previously persisted consumer context
/// is represented by an ActivityLink rather than a synchronous parent span.
/// Missing or malformed legacy metadata must not prevent business processing.
/// </summary>
internal static class AllocationInboxProcessingTelemetry
{
    internal const string OperationName = "allocation.inbox.process";

    internal static Activity? StartActivity(string? traceParent, string? traceState)
    {
        ActivityLink[]? links = null;

        if (traceParent is { Length: 55 } &&
            traceState?.Length is not > 512 &&
            ActivityContext.TryParse(traceParent, traceState, out var context) &&
            context.TraceId != default &&
            context.SpanId != default)
        {
            links = [new ActivityLink(context)];
        }

        // No ActivitySource listener? StartActivity returns null; processing still runs.
        return AllocationTelemetry.Source.StartActivity(
            OperationName,
            ActivityKind.Internal,
            parentContext: default,
            links: links);
    }
}
