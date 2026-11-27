using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Trace;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Adds an opt-in, Development-only OTLP/HTTP trace exporter to the existing
/// OpenTelemetry tracer provider. Does not add ActivitySources or duplicate providers.
/// </summary>
public static class JaegerOtlpExporterRegistration
{
    private const string EnabledSetting = "Observability:Otlp:Enabled";
    private const string EndpointSetting = "Observability:Otlp:Endpoint";
    private const string DefaultLocalEndpoint = "http://127.0.0.1:4318/v1/traces";

    public static void Configure(
        IServiceCollection services,
        IConfiguration configuration,
        IHostEnvironment environment)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(environment);

        // The switch must be explicitly enabled and the host must be in Development.
        if (!environment.IsDevelopment() || !configuration.GetValue<bool>(EnabledSetting))
        {
            return;
        }

        var endpointSetting = configuration[EndpointSetting] ?? DefaultLocalEndpoint;

        if (!Uri.TryCreate(endpointSetting, UriKind.Absolute, out var endpoint) ||
            (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps) ||
            endpoint.AbsolutePath != "/v1/traces" ||
            !string.IsNullOrEmpty(endpoint.UserInfo) ||
            !string.IsNullOrEmpty(endpoint.Query) ||
            !string.IsNullOrEmpty(endpoint.Fragment))
        {
            throw new InvalidOperationException(
                $"{EndpointSetting} must be an absolute HTTP(S) URL ending in '/v1/traces', " +
                "without credentials, query parameters, or a fragment.");
        }

        // A second WithTracing configuration augments the SINGLE existing provider.
        // Existing service.name, ActivitySources, HTTP instrumentation and the optional
        // console exporter remain configured in the application's original Program.cs.
        services.AddOpenTelemetry()
            .WithTracing(tracing => tracing.AddOtlpExporter(options =>
            {
                options.Protocol = OtlpExportProtocol.HttpProtobuf;
                options.Endpoint = endpoint;
            }));
    }
}
