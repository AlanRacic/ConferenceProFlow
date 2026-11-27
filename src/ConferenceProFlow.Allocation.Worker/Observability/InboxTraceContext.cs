using System.Diagnostics;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>
/// Captures valid W3C context before an asynchronous SQL Inbox boundary.
/// When there is no active Activity listener, retains the extracted remote context.
/// Never returns malformed or oversized metadata to the business persistence layer.
/// </summary>
internal static class InboxTraceContext
{
    internal static (string? TraceParent, string? TraceState) Capture(
        Activity? consumerActivity,
        ActivityContext? extractedRemoteParent)
    {
        if (consumerActivity is { IdFormat: ActivityIdFormat.W3C } &&
            IsValid(consumerActivity.Id, consumerActivity.TraceStateString))
        {
            return (consumerActivity.Id, consumerActivity.TraceStateString);
        }

        if (extractedRemoteParent is { } parent &&
            parent.TraceId != default &&
            parent.SpanId != default)
        {
            var traceParent =
                $"00-{parent.TraceId}-{parent.SpanId}-{(byte)parent.TraceFlags:x2}";

            if (IsValid(traceParent, parent.TraceState))
            {
                return (traceParent, parent.TraceState);
            }
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
