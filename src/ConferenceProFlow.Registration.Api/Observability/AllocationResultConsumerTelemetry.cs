using System.Diagnostics;
using System.Text;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Creates a Registration-owned RabbitMQ consumer span from optional, untrusted
/// W3C trace headers. A malformed or absent header must not affect delivery.
/// </summary>
internal static class AllocationResultConsumerTelemetry
{
    internal const string OperationName = "rabbitmq.allocation-result.process";
    private const int TraceParentLength = 55;
    private const int MaxTraceStateLength = 512;

    internal static Activity? StartProcessingActivity(
        IDictionary<string, object?>? headers)
    {
        // A remote parent makes this processing span a child of the sending
        // Allocation Result producer span. Legacy messages start independently.
        var hasRemoteParent = TryExtractContext(headers, out var remoteParent);

        var activity = hasRemoteParent
            ? RegistrationTelemetry.Source.StartActivity(
                OperationName,
                ActivityKind.Consumer,
                remoteParent)
            : RegistrationTelemetry.Source.StartActivity(
                OperationName,
                ActivityKind.Consumer);

        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.operation.name", "process");
        return activity;
    }

    internal static bool TryExtractContext(
        IDictionary<string, object?>? headers,
        out ActivityContext context)
    {
        context = default;

        if (headers is null ||
            !headers.TryGetValue("traceparent", out var rawParent) ||
            !TryReadAscii(rawParent, TraceParentLength, out var traceParent) ||
            traceParent.Length != TraceParentLength)
        {
            return false;
        }

        string? traceState = null;
        if (headers.TryGetValue("tracestate", out var rawState))
        {
            if (!TryReadAscii(rawState, MaxTraceStateLength, out traceState))
                return false;
        }

        // The context arrived over RabbitMQ; it is not a local Activity parent.
        return ActivityContext.TryParse(
            traceParent,
            traceState,
            isRemote: true,
            out context) &&
            context.TraceId != default &&
            context.SpanId != default;
    }

    private static bool TryReadAscii(object? raw, int maximumLength, out string text)
    {
        text = string.Empty;

        switch (raw)
        {
            case byte[] bytes when IsPrintableAscii(bytes, maximumLength):
                text = Encoding.ASCII.GetString(bytes);
                return true;

            case ReadOnlyMemory<byte> memory
                when IsPrintableAscii(memory.Span, maximumLength):
                text = Encoding.ASCII.GetString(memory.Span);
                return true;

            case string value when IsPrintableAscii(value, maximumLength):
                text = value;
                return true;

            default:
                return false;
        }
    }

    private static bool IsPrintableAscii(ReadOnlySpan<byte> bytes, int maximumLength)
    {
        if (bytes.Length is 0 || bytes.Length > maximumLength)
            return false;

        foreach (var b in bytes)
        {
            if (b is < 0x20 or > 0x7E)
                return false;
        }
        return true;
    }

    private static bool IsPrintableAscii(string value, int maximumLength)
    {
        if (value.Length is 0 || value.Length > maximumLength)
            return false;

        foreach (var ch in value)
        {
            if (ch is < ' ' or > '~')
                return false;
        }
        return true;
    }
}
