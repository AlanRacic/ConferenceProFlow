# M4.3B — Registration metrics: SQL-backed validation

## Purpose

This increment verifies that Registration finalization metrics are emitted at the
**real SQL reconciliation boundary**, not merely by directly calling a metric helper.
No production code or existing test files are replaced.

## Automated scenario

`RegistrationFinalizationMetricsIntegrationTests` uses the existing
`ResultReconciliationSqlFixture` SQL Server Testcontainers fixture type to:

1. Seed two distinct Pending registrations.
2. Apply a Confirmed decision and a Rejected decision through
   `AllocationResultReconciliationService.ApplyAsync` (real EF Core / SQL).
3. Re-deliver the same message, repeat the same business decision under a new
   MessageId, and send a conflicting decision for an already finalized registration.
4. Verify committed statuses in a fresh DbContext.
5. Capture business instruments through the .NET `MeterListener` API and assert
   **one counter and one duration sample for each actual finalization**.
6. Assert the samples have exactly one label, `outcome`, with values
   `confirmed` or `rejected`.

The test does **not** require Prometheus or Grafana; it verifies the instrumentation
at the application / persistence boundary, not the OTLP transport or dashboard.
The fixture is created for this test class; therefore run this class (or the
Registration integration project) **without xUnit parallelism** on a local machine.

## Local verification

From the repository root, with Docker Desktop running:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Debug
```

```powershell
dotnet test tests/ConferenceProFlow.Registration.IntegrationTests/ConferenceProFlow.Registration.IntegrationTests.csproj --configuration Debug -- --parallel none --filter-class "*RegistrationFinalizationMetricsIntegrationTests"
```

If `--filter-class` is unsupported by the installed runner, use the whole
project with `-- --parallel none` instead; do not switch to VSTest `--filter`.

## Meaning of metric measurements

- `conferenceproflow.registration.finalized` is a best-effort `Counter<long>`
  emitted once per successful transaction **in this process**.
- `conferenceproflow.registration.finalization.duration` is a seconds histogram
  computed from `Registration.UpdatedAtUtc - Registration.CreatedAtUtc`.
- Neither is an exactly-once audit ledger. If a process terminates just after
  SQL commit, measurement may be absent; SQL is the source of truth.
- No message, registration, participant or trace IDs are placed in labels.
- HTTP metrics and direct OTLP exporter wiring are covered separately.

## Next

M4.3C: add Allocation / Analytics worker business metrics and verify their
semantics before enabling Prometheus OTLP and Grafana dashboards.
