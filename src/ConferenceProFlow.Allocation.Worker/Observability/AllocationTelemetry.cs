using System.Diagnostics;

namespace ConferenceProFlow.Allocation.Worker.Observability;

internal static class AllocationTelemetry
{
    internal const string SourceName = "ConferenceProFlow.Allocation.Worker";

    internal static readonly ActivitySource Source = new(SourceName);
}