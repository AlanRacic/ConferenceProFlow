using System.Diagnostics;
using System.Text;
using Confluent.Kafka;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Produces optional W3C trace metadata for the durable Kafka Analytics Outbox.
/// Each publishing attempt is a new operation, linked to the persisted origin;
/// the original business MessageId and JSON payload never change.
/// </summary>
internal static class KafkaAnalyticsPublishTelemetry
{
    internal const string OperationName = "kafka.analytics.publish";
    private const int TraceParentLength = 55;
    private const int MaxTraceStateLength = 512;

    internal static Activity? StartActivity(string? traceParent, string? traceState)
    {
        var links = TryParseContext(traceParent, traceState, out var context)
            ? new[] { new ActivityLink(context) }
            : null;

        // Outbox publication can happen after a process restart. A causal link
        // is more accurate than pretending the old HTTP span is still active.
        var activity = RegistrationTelemetry.Source.StartActivity(
            OperationName,
            ActivityKind.Producer,
            parentContext: default,
            links: links);

        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.operation.name", "send");
        return activity;
    }

    internal static Headers CreateHeaders(
        Guid messageId,
        string messageType,
        Activity? publishActivity,
        string? storedTraceParent,
        string? storedTraceState)
    {
        // These two existing headers are part of the established contract.
        var headers = new Headers
        {
            { "message-id", Encoding.UTF8.GetBytes(messageId.ToString("D")) },
            { "event-type", Encoding.UTF8.GetBytes(messageType) }
        };

        // Prefer the newly created producer span. When no listener is active,
        // propagate a valid durable context rather than dropping correlation.
        var candidateParent = publishActivity?.IdFormat == ActivityIdFormat.W3C
            ? publishActivity.Id
            : storedTraceParent;
        var candidateState = publishActivity?.IdFormat == ActivityIdFormat.W3C
            ? publishActivity.TraceStateString
            : storedTraceState;

        if (!TryParseContext(candidateParent, candidateState, out _))
            return headers;

        headers.Add("traceparent", Encoding.ASCII.GetBytes(candidateParent!));
        if (!string.IsNullOrEmpty(candidateState))
            headers.Add("tracestate", Encoding.ASCII.GetBytes(candidateState));

        return headers;
    }

    private static bool TryParseContext(
        string? traceParent,
        string? traceState,
        out ActivityContext context)
    {
        context = default;
        return traceParent is { Length: TraceParentLength } &&
               IsPrintableAscii(traceParent) &&
               (traceState is null ||
                (traceState.Length <= MaxTraceStateLength && IsPrintableAscii(traceState))) &&
               ActivityContext.TryParse(traceParent, traceState, out context) &&
               context.TraceId != default &&
               context.SpanId != default;
    }

    private static bool IsPrintableAscii(string value)
    {
        foreach (var character in value)
        {
            if (character is < ' ' or > '~')
                return false;
        }
        return true;
    }
}
