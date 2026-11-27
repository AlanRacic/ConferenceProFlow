# OpenTelemetry OTLP Application Export (M4.2D2)

## Purpose

Add **opt-in, Development-only** OpenTelemetry OTLP/HTTP exporter configuration to:

- `ConferenceProFlow.Registration.Api`
- `ConferenceProFlow.Allocation.Worker`
- `ConferenceProFlow.Analytics.Worker`

The local receiver is Jaeger from `infrastructure/compose.observability.yaml` (M4.2D1). This increment extends the *existing* tracer provider in each process rather than configuring a second provider.

## Design

1. **Keep the current telemetry SDK configuration** in each Program.cs: resource service names, ActivitySources, ASP.NET Core / HttpClient instrumentation and conditional console exporter. The new registration calls `AddOpenTelemetry().WithTracing(...AddOtlpExporter(...))`; repeated `WithTracing` configurations contribute to the **same provider**.
2. **Opt-in only** when the application environment is `Development` and `Observability:Otlp:Enabled` is `true`. Disabled by default.
3. Default HTTP/protobuf traces endpoint: `http://127.0.0.1:4318/v1/traces`. Can be overridden through `Observability:Otlp:Endpoint`; validated as a complete HTTP(S) `/v1/traces` URL without embedded credentials or query/fragment.
4. **No runtime business coupling**: the batch OTLP exporter operates asynchronously; Kafka offset commits, RabbitMQ ACKs, outbox records and domain transactions are unchanged.
5. **No PII / secrets** in span attributes. Tracing is for demonstration and troubleshooting, not an audit log.
6. **No production exposure**: local Jaeger uses HTTP, in-memory storage and localhost-only port bindings. This is intentionally not a production observability deployment.

## Configuration

Local terminal setting:

```powershell
$env:Observability__Otlp__Enabled='true'
```

Optional endpoint override:

```powershell
$env:Observability__Otlp__Endpoint='http://127.0.0.1:4318/v1/traces'
```

This works when application configuration loads the ordinary .NET environment variable provider. The deployment/environment setting must separately be `Development`.

Alternatively add the key to a **local development launch profile** (`environmentVariables` in `Properties/launchSettings.json`), without overwriting existing launchSettings, for F5 debugging. Do not commit machine-specific settings or credentials to shared source.

## Build and verification

- `OpenTelemetry.Exporter.OpenTelemetryProtocol` 1.21.0 in each of the three application `.csproj` files, aligned with the previously used OTel 1.21.0 version.
- One call to each local `JaegerOtlpExporterRegistration.Configure(...)` before `builder.Build()`.
- `dotnet build` / VS Build Solution; no EF migrations.
- Start Jaeger, then explicitly enable exporters and start applications with their current working profiles.
- Emit an HTTP `/health` request to verify the Registration API service in Jaeger; emit valid business messages to verify Allocation and Analytics. Service entries appear only after spans are exported; there may be a short batch delay.
- Check parent-child relationships **and** ActivityLinks at asynchronous boundaries. Linked spans can have different TraceIds; one UI waterfall is not necessarily the complete causal graph.
- Exercise existing unit/integration suites to rule out regressions.

## Troubleshooting

| Symptom | Likely explanation / action |
|---|---|
| `OpenTelemetry.Exporter` namespace not found | Install matching OTLP NuGet package into the affected application project. |
| No traces for any application | Check `Development`, explicit `Observability__Otlp__Enabled=true`, and Jaeger port 4318. |
| `/health` trace visible, worker absent | Start actual consumer traffic; workers normally produce no spans while idle. |
| `Export failed` errors | Check Jaeger Docker `ps` and `logs`; confirm port 4318 listening. |
| Compilation fails after Program.cs edit | Ensure call is before the corresponding `builder.Build()` and the new `.cs` was copied under the correct project namespace. |
| Service names appear different | Check existing `.SetResourceBuilder(...AddService(...))` definitions; this increment does not change them. |
| Multiple traces, not one waterfall | Expected for linked async operations; inspect `ActivityLink` fields and individual TraceIds. |

## Sources

- OpenTelemetry .NET exporter: https://opentelemetry.io/docs/languages/dotnet/exporters/
- OpenTelemetry.Extensions.Hosting provider registration: https://www.nuget.org/packages/OpenTelemetry.Extensions.Hosting/
- OpenTelemetry OTLP exporter: https://www.nuget.org/packages/OpenTelemetry.Exporter.OpenTelemetryProtocol/
- Jaeger: https://www.jaegertracing.io/docs/2.23/getting-started/
