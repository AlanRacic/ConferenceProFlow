using System.Diagnostics;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Captures a bounded W3C context while reconciliation is executing.
/// Telemetry is optional: missing/invalid ambient context yields null metadata
/// and must never block the business transaction.
/// </summary>
internal static class AnalyticsOutboxTraceContext
{
    internal static (string? TraceParent, string? TraceState) Capture()
    {
        var activity = Activity.Current;
        if (activity?.IdFormat != ActivityIdFormat.W3C)
            return (null, null);

        var traceParent = activity.Id;
        var traceState = activity.TraceStateString;

        if (traceParent is not { Length: 55 } ||
            traceState is { Length: > 512 } ||
            !ActivityContext.TryParse(traceParent, traceState, out _))
        {
            return (null, null);
        }

        return (traceParent, traceState);
    }
}
