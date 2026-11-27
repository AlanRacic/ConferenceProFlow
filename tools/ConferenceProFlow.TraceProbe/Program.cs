// Development-only diagnostic for Jaeger's OTLP/HTTP JSON receiver.
// This sends ONE synthetic span; it does not instrument the real application.
// No third-party NuGet packages, credentials or application business data.
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

const string defaultBaseUrl = "http://127.0.0.1:4318";
if (args.Length > 1 ||
    !Uri.TryCreate(args.Length == 0 ? defaultBaseUrl : args[0],
        UriKind.Absolute, out var baseUri) ||
    (baseUri.Scheme != Uri.UriSchemeHttp && baseUri.Scheme != Uri.UriSchemeHttps))
{
    Console.Error.WriteLine("Usage: dotnet run --project tools/ConferenceProFlow.TraceProbe [http://127.0.0.1:4318]");
    return 2;
}

var target = new Uri(baseUri.ToString().TrimEnd('/') + "/v1/traces");
var traceId = Guid.NewGuid().ToString("N");
var spanId = Convert.ToHexString(RandomNumberGenerator.GetBytes(8)).ToLowerInvariant();

// OTLP timestamps are decimal nanoseconds since Unix epoch, encoded as strings.
var startedAt = checked(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L);
var finishedAt = startedAt + 5_000_000L; // synthetic 5 ms operation

var requestBody = new
{
    resourceSpans = new[]
    {
        new
        {
            resource = new
            {
                attributes = new[]
                {
                    new { key = "service.name", value = new { stringValue = "conferenceproflow-trace-probe" } },
                    new { key = "deployment.environment.name", value = new { stringValue = "development" } }
                }
            },
            scopeSpans = new[]
            {
                new
                {
                    scope = new { name = "conferenceproflow.trace-probe", version = "1.0.0" },
                    spans = new[]
                    {
                        new
                        {
                            traceId,
                            spanId,
                            name = "jaeger.otlp.ingestion.smoke",
                            kind = 1, // INTERNAL, per OTLP SpanKind
                            startTimeUnixNano = startedAt.ToString(CultureInfo.InvariantCulture),
                            endTimeUnixNano = finishedAt.ToString(CultureInfo.InvariantCulture),
                            attributes = new[]
                            {
                                new { key = "conferenceproflow.diagnostic", value = new { stringValue = "true" } }
                            }
                        }
                    }
                }
            }
        }
    }
};

try
{
    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
    using var content = new StringContent(
        JsonSerializer.Serialize(requestBody), Encoding.UTF8, "application/json");
    using var response = await client.PostAsync(target, content);

    if (!response.IsSuccessStatusCode)
    {
        Console.Error.WriteLine($"OTLP export FAILED ({(int)response.StatusCode} {response.ReasonPhrase}).");
        Console.Error.WriteLine(await response.Content.ReadAsStringAsync());
        return 1;
    }

    Console.WriteLine($"OTLP export accepted: HTTP {(int)response.StatusCode}");
    Console.WriteLine("Service: conferenceproflow-trace-probe");
    Console.WriteLine("Operation: jaeger.otlp.ingestion.smoke");
    Console.WriteLine($"TraceId: {traceId}");
    Console.WriteLine("Open http://127.0.0.1:16686 and search for the service (Last 1h).");
    Console.WriteLine("This verifies Jaeger ingestion only, NOT application tracing.");
    return 0;
}
catch (HttpRequestException ex)
{
    Console.Error.WriteLine($"Cannot reach OTLP endpoint {target}: {ex.Message}");
    return 1;
}
catch (TaskCanceledException ex)
{
    Console.Error.WriteLine($"OTLP request timed out or was canceled: {ex.Message}");
    return 1;
}
