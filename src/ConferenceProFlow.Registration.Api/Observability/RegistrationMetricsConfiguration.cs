using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;

namespace ConferenceProFlow.Registration.Api.Observability;

/// <summary>
/// Adds metrics to the same OpenTelemetry SDK setup as the existing tracing pipeline.
/// Metrics are opt-in and separate from the Jaeger trace exporter.
/// </summary>
internal static class RegistrationMetricsConfiguration
{
    private const string EnabledSetting = "Observability:Metrics:Enabled";
    private const string OtlpEnabledSetting = "Observability:Metrics:Otlp:Enabled";
    private const string OtlpEndpointSetting = "Observability:Metrics:Otlp:Endpoint";
    private const string DefaultOtlpEndpoint =
        "http://127.0.0.1:9090/api/v1/otlp/v1/metrics";

    internal static void Configure(
        IServiceCollection services,
        IConfiguration configuration,
        IHostEnvironment environment)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(environment);

        // The local metric pipeline is explicitly opt-in and Development-only.
        if (!environment.IsDevelopment() || !configuration.GetValue<bool>(EnabledSetting))
            return;

        // Validate before registering the exporter. Jaeger's trace endpoint remains separate.
        Uri? otlpEndpoint = null;
        if (configuration.GetValue<bool>(OtlpEnabledSetting))
        {
            var candidate = configuration[OtlpEndpointSetting] ?? DefaultOtlpEndpoint;
            if (!Uri.TryCreate(candidate, UriKind.Absolute, out otlpEndpoint) ||
                (otlpEndpoint.Scheme != Uri.UriSchemeHttp &&
                 otlpEndpoint.Scheme != Uri.UriSchemeHttps) ||
                otlpEndpoint.AbsolutePath != "/api/v1/otlp/v1/metrics" ||
                !string.IsNullOrEmpty(otlpEndpoint.UserInfo) ||
                !string.IsNullOrEmpty(otlpEndpoint.Query) ||
                !string.IsNullOrEmpty(otlpEndpoint.Fragment))
            {
                throw new InvalidOperationException(
                    $"{OtlpEndpointSetting} must be an absolute HTTP(S) URL with path " +
                    "'/api/v1/otlp/v1/metrics' and no credentials, query, or fragment.");
            }
        }

        // M4.3C3C: SQL polling is hosted independently of broker publishers and gated by metrics enablement.
        services.AddHostedService<RegistrationOutboxBacklogSampler>();

        services.AddOpenTelemetry().WithMetrics(metrics =>
        {
            // HTTP latency measures the synchronous request, NOT async finalization.
            metrics.AddAspNetCoreInstrumentation();
            metrics.AddMeter(RegistrationMetrics.MeterName);
            metrics.AddMeter(RegistrationOutboxPublishMetrics.MeterName);
            metrics.AddMeter(RegistrationOutboxBacklogMetrics.MeterName);

            if (otlpEndpoint is not null)
            {
                metrics.AddOtlpExporter(options =>
                {
                    options.Protocol = OtlpExportProtocol.HttpProtobuf;
                    options.Endpoint = otlpEndpoint;
                });
            }
        });
    }
}
