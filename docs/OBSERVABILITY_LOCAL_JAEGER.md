# Local Distributed Tracing: Jaeger Backend (M4.2D1)

**Status:** Infrastructure + synthetic OTLP ingestion probe. **Not yet** real ConferenceProFlow trace export.

## Design and boundaries

Registration API, Allocation Worker and Analytics Worker already have local OpenTelemetry activities and messaging context propagation (M4.2A–C8C). This step provides an optional, local **Jaeger v2 all-in-one** backend. The applications are **not configured to export to Jaeger in M4.2D1**. Application OTLP exporters will be enabled explicitly in **M4.2D2**, based on the actual local Program.cs files to preserve the existing configuration.

```text
.NET applications (Visual Studio / Windows)
    [M4.2D2: add opt-in OTLP/HTTP exporter]
                     |
                     v
       http://127.0.0.1:4318/v1/traces
                     |
                     v
              Jaeger v2 container
                     |
                     v
       http://127.0.0.1:16686 (UI)
```

This Docker Compose file runs **independently** of `infrastructure/compose.yaml` so it does not re-create SQL Server, RabbitMQ or Kafka containers. Never combine both compose files with multiple `-f` flags for this step.

## Prerequisites

- Windows, Docker Desktop (Linux containers), Docker Compose v2.
- .NET 10 SDK for the *optional synthetic probe*.
- All commands below run in the repository root (`C:\work\ConferenceProFlow`).
- Ensure ports **16686**, **4317** and **4318** are not occupied. This Compose file binds only to `127.0.0.1`.

## Start backend

```powershell
cd C:\work\ConferenceProFlow
docker compose -f infrastructure/compose.observability.yaml config
docker compose -f infrastructure/compose.observability.yaml up -d
docker compose -f infrastructure/compose.observability.yaml ps
```

Open **http://127.0.0.1:16686**. An initially empty service dropdown is expected: no real application has an OTLP exporter yet.

## Diagnostic smoke test: Jaeger ingestion

The stand-alone .NET probe uses the OTLP/HTTP JSON protocol. It sends exactly **one synthetic span**, with no personal information, no network passwords, and no external NuGet dependencies. It is intentionally **not added to `ConferenceProFlow.slnx`**, keeping the main solution/CI unaffected.

```powershell
dotnet run --project tools/ConferenceProFlow.TraceProbe/ConferenceProFlow.TraceProbe.csproj --configuration Release
```

Expected console fields: `OTLP export accepted: HTTP 200` (2xx acceptance is valid), `Service: conferenceproflow-trace-probe`, and a 32-character `TraceId`.

In Jaeger: select **Service = conferenceproflow-trace-probe**, time range **Last 1h**, then **Find Traces**. Open the trace with operation `jaeger.otlp.ingestion.smoke`. Allow a short moment for the UI to refresh. **This confirms backend/UI/OTLP delivery only**; it does not establish propagation across RabbitMQ or Kafka.

## Troubleshooting

| Problem | Check |
|---|---|
| `port is already allocated` | `docker ps` or `Get-NetTCPConnection -LocalPort 16686,4317,4318`; resolve the collision without stopping unrelated services blindly. |
| Jaeger UI does not load | `docker compose -f infrastructure/compose.observability.yaml ps` and `docker compose -f infrastructure/compose.observability.yaml logs --tail 100 jaeger` |
| Probe cannot connect to 4318 | Container startup, port binding, firewall/loopback access. |
| Probe gets non-2xx | Read printed receiver response and Jaeger logs. |
| Probe accepted but UI has no service | Check selected time range and `conferenceproflow-trace-probe`, then refresh. |
| No Registration/Allocation/Analytics services yet | Expected in D1; those .NET OTLP exporters come in D2. |

## Stop (does not stop core messaging/database infrastructure)

```powershell
docker compose -f infrastructure/compose.observability.yaml down
```

The local Jaeger all-in-one image is configured with **in-memory storage**; its traces do **not** survive container restarts/removal. This is deliberate for a zero-cost development viewer, **not** production durability. There is no authentication or TLS; bind to localhost only. Avoid exporting secrets, personal details or business payloads in span attributes.

## Next: M4.2D2

Add `OpenTelemetry.Exporter.OpenTelemetryProtocol` **version compatible with the project's existing OpenTelemetry packages**, enable `HttpProtobuf` exporter only through an explicit Development configuration flag, and target `http://127.0.0.1:4318/v1/traces` for apps running on Windows. Do not copy an outdated public `Program.cs` over locally instrumented files: use the **current local** Program.cs and .csproj files for all three apps.

Full e2e test will then create real registrations and validate service names, timing, parent relations, **ActivityLinks across durable boundaries**, SQL Inbox/Outbox state, and Kafka Analytics consumer work. Jaeger may visualize linked traces separately: search the recorded `TraceId` and inspect span links rather than assuming every asynchronous segment has one TraceId.

## Sources

- Jaeger 2.23 official getting started: https://www.jaegertracing.io/docs/2.23/getting-started/
- Jaeger v2 OTLP/API ports: https://www.jaegertracing.io/docs/2.23/architecture/apis/
- OTLP protocol: https://opentelemetry.io/docs/specs/otlp/
- OpenTelemetry .NET exporter: https://opentelemetry.io/docs/languages/dotnet/exporters/
