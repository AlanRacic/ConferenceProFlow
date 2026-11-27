using System.Diagnostics;

namespace ConferenceProFlow.Analytics.Worker.Observability;

internal static class AnalyticsTelemetry
{
    internal const string SourceName = "ConferenceProFlow.Analytics.Worker";

    internal static readonly ActivitySource Source = new(SourceName);
}