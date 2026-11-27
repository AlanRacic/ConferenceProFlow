using System.Diagnostics;
using System.Text;
using Confluent.Kafka;

namespace ConferenceProFlow.Analytics.Worker.Observability;

/// <summary>
/// Extracts optional W3C trace context from a live Kafka record and starts a
/// processing span. Missing or malformed telemetry never rejects business data.
/// </summary>
public static class KafkaAnalyticsConsumerTelemetry
{
    private const int TraceParentMaxBytes = 55;
    private const int TraceStateMaxBytes = 512;

    public static bool TryExtract(Headers? headers, out ActivityContext parentContext)
    {
        parentContext = default;

        if (headers is null ||
            !headers.TryGetLastBytes("traceparent", out var parentBytes) ||
            !TryReadAscii(parentBytes, TraceParentMaxBytes, out var traceParent))
        {
            return false;
        }

        string? traceState = null;
        if (headers.TryGetLastBytes("tracestate", out var stateBytes))
        {
            if (!TryReadAscii(stateBytes, TraceStateMaxBytes, out var parsedState))
            {
                return false;
            }

            traceState = parsedState;
        }

        return ActivityContext.TryParse(traceParent, traceState, out parentContext);
    }

    public static Activity? StartProcessingActivity(Headers? headers)
    {
        var hasParent = TryExtract(headers, out var parentContext);

        var activity = hasParent
            ? AnalyticsTelemetry.Source.StartActivity(
                "kafka.analytics.process",
                ActivityKind.Consumer,
                parentContext)
            : AnalyticsTelemetry.Source.StartActivity(
                "kafka.analytics.process",
                ActivityKind.Consumer);

        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.operation.name", "process");

        return activity;
    }

    private static bool TryReadAscii(byte[]? bytes, int maxLength, out string text)
    {
        text = string.Empty;
        if (bytes is null || bytes.Length is 0 || bytes.Length > maxLength)
        {
            return false;
        }

        // Reject non-ASCII and control bytes instead of silently substituting '?'.
        // Header validation is diagnostic only: a bad header is ignored by TryExtract.
        foreach (var value in bytes)
        {
            if (value is < 0x20 or > 0x7E)
            {
                return false;
            }
        }

        text = Encoding.ASCII.GetString(bytes);
        return true;
    }
}
