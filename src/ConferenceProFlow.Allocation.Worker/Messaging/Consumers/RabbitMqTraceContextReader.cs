using System.Diagnostics;
using System.Text;

namespace ConferenceProFlow.Allocation.Worker.Messaging.Consumers;

/// <summary>
/// Reads W3C trace context from untrusted RabbitMQ headers.
/// Malformed or missing telemetry never prevents business-message processing.
/// </summary>
internal static class RabbitMqTraceContextReader
{
    private const int MaxTraceParentLength = 55;
    private const int MaxTraceStateLength = 512;

    internal static bool TryExtract(
        IDictionary<string, object?>? headers,
        out ActivityContext context)
    {
        context = default;

        if (headers is null ||
            !headers.TryGetValue("traceparent", out var rawTraceParent) ||
            !TryReadAscii(rawTraceParent, MaxTraceParentLength, out var traceParent))
        {
            return false;
        }

        string? traceState = null;
        if (headers.TryGetValue("tracestate", out var rawTraceState))
        {
            if (!TryReadAscii(rawTraceState, MaxTraceStateLength, out traceState))
            {
                return false;
            }
        }

        // An AMQP delivery comes from another process: mark its parent as remote.
        return ActivityContext.TryParse(
            traceParent,
            traceState,
            isRemote: true,
            out context);
    }

    private static bool TryReadAscii(object? value, int maxLength, out string text)
    {
        text = string.Empty;

        switch (value)
        {
            case byte[] bytes when IsPrintableAscii(bytes, maxLength):
                text = Encoding.ASCII.GetString(bytes);
                return true;

            case ReadOnlyMemory<byte> memory
                when IsPrintableAscii(memory.Span, maxLength):
                text = Encoding.ASCII.GetString(memory.Span);
                return true;

            case string valueAsString when IsPrintableAscii(valueAsString, maxLength):
                text = valueAsString;
                return true;

            default:
                return false;
        }
    }

    private static bool IsPrintableAscii(ReadOnlySpan<byte> value, int maxLength)
    {
        if (value.Length is 0 || value.Length > maxLength)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (character is < 0x20 or > 0x7E)
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsPrintableAscii(string value, int maxLength)
    {
        if (value.Length is 0 || value.Length > maxLength)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (character is < ' ' or > '~')
            {
                return false;
            }
        }

        return true;
    }
}
