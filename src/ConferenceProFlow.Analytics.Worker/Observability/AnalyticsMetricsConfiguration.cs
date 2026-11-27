using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;

namespace ConferenceProFlow.Analytics.Worker.Observability;

/// <summary>
/// Opt-in Development-only metrics provider, following the Registration and
/// Allocation metrics configurations. The Jaeger trace pipeline is untouched.
/// </summary>
internal static class AnalyticsMetricsConfiguration
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

        if (!environment.IsDevelopment() || !configuration.GetValue<bool>(EnabledSetting))
            return;

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

        services.AddOpenTelemetry().WithMetrics(metrics =>
        {
            metrics.AddMeter(AnalyticsKafkaConsumerMetrics.MeterName);
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
