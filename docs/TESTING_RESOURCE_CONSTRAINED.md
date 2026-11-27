# ConferenceProFlow — Running Tests on Resource-Constrained Machines

**Milestone:** M4.2 — Observability and distributed tracing  
**Scope:** Local Windows development with Docker Desktop, .NET 10, xUnit v3 / Microsoft.Testing.Platform, and Testcontainers.  
**Validated locally:** 2026-11-27, Debug configuration; the five test projects were run individually and were reported **passed**.  
**Not yet validated by this document:** Complete Release test run, GitHub Actions for the current feature branch, or identical behavior on another machine.

## Why a single all-solution run may be unsuitable locally

An all-tests run from Visual Studio Test Explorer on the local laptop caused substantial resource contention: multiple temporary SQL Server containers were created, disk activity increased, and many integration tests failed during SQL pre-login handshakes or Docker API operations. The run was interrupted, and remaining temporary containers were manually stopped. Therefore, their exit codes must **not** be presented as proof of out-of-memory termination.

A previously failing Allocation integration test passed when run alone. Later, each of the five project-level test suites passed when run separately, with internal test parallelization disabled for the integration projects. These observations **support a local resource-contention hypothesis**; they do not prove every prior failure had that cause.

## Prepare the environment

- Start Docker Desktop and make sure its Linux engine responds (`docker version`).
- Stop Registration API, Allocation Worker, and Analytics Worker before running integration tests.
- Stop the development infrastructure so Testcontainers has more available memory/CPU. Run the following from the repository root (`C:\work\ConferenceProFlow`):

```powershell
# Stop, but do NOT remove, normal development containers and their persisted volumes.
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka stop
# Stop the local trace viewer if it is running.
docker compose -f infrastructure/compose.observability.yaml stop
```

- Do **not** use `docker compose down -v`, remove named volumes, clear test databases indiscriminately, or delete running Testcontainers containers while tests are executing.
- Avoid running multiple test projects simultaneously. Running one test project at a time limits *inter-project* competition; `--parallel none` on integration projects also limits *intra-project* parallelism.

## Run the five test projects, one after another

> These are the commands **reported successful locally** in Debug configuration. Run the next command only after the previous run has fully completed. The separator `--` forwards options to the xUnit v3 / Microsoft.Testing.Platform test application. Do not mix in legacy VSTest `--logger "trx;..."` arguments without checking runner support.

### 1 — Registration unit tests

```powershell
dotnet test tests/ConferenceProFlow.Registration.UnitTests/ConferenceProFlow.Registration.UnitTests.csproj --configuration Debug
```

### 2 — Allocation unit tests

```powershell
dotnet test tests/ConferenceProFlow.Allocation.UnitTests/ConferenceProFlow.Allocation.UnitTests.csproj --configuration Debug
```

### 3 — Allocation integration tests

```powershell
dotnet test tests/ConferenceProFlow.Allocation.IntegrationTests/ConferenceProFlow.Allocation.IntegrationTests.csproj --configuration Debug -- --parallel none
```

### 4 — Registration integration tests

```powershell
dotnet test tests/ConferenceProFlow.Registration.IntegrationTests/ConferenceProFlow.Registration.IntegrationTests.csproj --configuration Debug -- --parallel none
```

### 5 — Analytics integration tests

```powershell
dotnet test tests/ConferenceProFlow.Analytics.IntegrationTests/ConferenceProFlow.Analytics.IntegrationTests.csproj --configuration Debug -- --parallel none
```

### Observed local results (2026-11-27)

| Test project | Configuration / execution | User-reported result | Counts |
| --- | --- | --- | --- |
| Registration.UnitTests | Debug, independent run | Passed | Not recorded |
| Allocation.UnitTests | Debug, independent run | Passed | Not recorded |
| Allocation.IntegrationTests | Debug, `--parallel none` | Passed | Not recorded |
| Registration.IntegrationTests | Debug, `--parallel none` | Passed | Not recorded |
| Analytics.IntegrationTests | Debug, `--parallel none` | Passed | Not recorded |

**Evidence note:** These pass results are based on the completed local runs reported during development. Do not fabricate test counts, durations, screenshots, or CI statuses; add them only from actual output.

## Release build gate (not yet recorded as passed)

A **Release build**, without repeating every expensive integration suite locally, is a reasonable next gate before reviewing the feature branch:

```powershell
dotnet restore ConferenceProFlow.slnx
```

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release --no-restore
```

Review errors and warnings. Running full Release integration tests can be reserved for a suitably provisioned environment / GitHub CI, but should not be claimed as passed unless actually executed. Confirm that GitHub Actions tests complete successfully before merging the PR.

## Troubleshooting guidelines

- **SQL pre-login timeout / Docker API InternalServerError during a broad run:** Inspect active Testcontainers, Docker CPU/memory, and disk usage. Reproduce a failing test individually before changing SQL timeouts or the test's business assertions.
- **`total: 0`, `Handshake failure`, invalid command-line arguments:** Check compatibility of flags with Microsoft.Testing.Platform and xUnit v3. A zero-test runner failure is not a failed business unit test.
- **Persistent failure in a standalone test:** Capture the exact test name, exception, stack trace, and relevant fixture behavior; the resource hypothesis does not explain every independent failure.
- **High disk use / severely unresponsive machine:** Stop the run safely, allow Testcontainers cleanup where possible, and inspect remaining containers once the test process has ended.

## Before the M4.2 Pull Request

1. Confirm Release restore and build succeeded.
2. Review Git diff for tracing, EF migrations/model snapshots, Kafka IPv4 advertised listener, docs and tests.
3. Ensure `.env`, passwords, user secrets, logs, local test artifacts, and `bin/obj` are excluded from commits.
4. Keep independent Debug test pass evidence distinct from pending Release and CI results.
5. Push the feature branch, open a PR against `master`, and require green CI prior to **Create a merge commit**.
