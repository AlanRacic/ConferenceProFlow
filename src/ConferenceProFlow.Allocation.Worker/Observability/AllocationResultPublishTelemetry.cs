using System.Diagnostics;
using System.Text;

namespace ConferenceProFlow.Allocation.Worker.Observability;

/// <summary>
/// Traces an Allocation Result publication after a durable Outbox boundary.
/// A fresh producer operation LINKS to the persisted business-processing context.
/// Message headers carry the producer context when a span is sampled, otherwise
/// the valid persisted context is forwarded (also supports legacy rows).
/// </summary>
internal static class AllocationResultPublishTelemetry
{
    internal const string OperationName = "rabbitmq.allocation-result.publish";

    internal static Activity? StartActivity(string? traceParent, string? traceState)
    {
        ActivityLink[]? links = null;

        if (TryReadContext(traceParent, traceState, out var context))
        {
            links = [new ActivityLink(context)];
        }

        return AllocationTelemetry.Source.StartActivity(
            OperationName,
            ActivityKind.Producer,
            parentContext: default,
            links: links);
    }

    internal static IDictionary<string, object?>? CreateHeaders(
        Activity? publishActivity,
        string? persistedTraceParent,
        string? persistedTraceState)
    {
        // An active producer span is the preferred context for the RabbitMQ consumer.
        var traceParent = publishActivity?.Id;
        var traceState = publishActivity?.TraceStateString;

        // If OpenTelemetry is disabled (or an Activity has an unsuitable ID),
        // preserve the valid W3C metadata already stored in the durable Outbox.
        if (!TryReadContext(traceParent, traceState, out _))
        {
            traceParent = persistedTraceParent;
            traceState = persistedTraceState;
        }

        if (!TryReadContext(traceParent, traceState, out _))
        {
            return null;
        }

        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.ASCII.GetBytes(traceParent!)
        };

        if (!string.IsNullOrEmpty(traceState))
        {
            headers["tracestate"] = Encoding.ASCII.GetBytes(traceState);
        }

        return headers;
    }

    private static bool TryReadContext(
        string? traceParent,
        string? traceState,
        out ActivityContext context)
    {
        context = default;
        return traceParent is { Length: 55 } &&
               traceState?.Length is not > 512 &&
               ActivityContext.TryParse(traceParent, traceState, out context) &&
               context.TraceId != default &&
               context.SpanId != default;
    }
}
