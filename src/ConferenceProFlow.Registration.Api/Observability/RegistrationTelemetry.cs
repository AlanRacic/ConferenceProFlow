using System.Diagnostics;

namespace ConferenceProFlow.Registration.Api.Observability;

internal static class RegistrationTelemetry
{
    internal const string SourceName = "ConferenceProFlow.Registration.Api";

    internal static readonly ActivitySource Source = new(SourceName);
}