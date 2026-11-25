# ConferenceProFlow — M3.6D Local Reproducibility Review

**Status:** Reviewer walkthrough / verification checklist. Not a claim that the clean-clone test has been executed by an independent reviewer.

**Scope:** Windows + Visual Studio 2026 + .NET 10 + Docker Desktop; operational M2.6 Registration/Allocation, optional M3.3 Kafka publishing and M3.4 live Analytics. M3.5 replay is an explicitly separate, guarded exercise, not a quick-start prerequisite.

**Safety contract:** Do not delete or reset SQL databases, Kafka topics, offsets, Docker named volumes, or existing replay snapshots. Never run `docker compose down -v` or delete the local `.env`/User Secrets. The steps below are non-destructive except for intentionally adding fresh demo conference/participant/registration records to the user's existing **local development** databases during the optional smoke demonstration. Do not run the development inventory seed in production.

## 1. Source inspection — what is already in the repository

- `README.md` links to the local development guide, M2.6 E2E demo, Kafka setup, Kafka publishing, Analytics Worker, and replay documentation.
- `docs/LOCAL_DEVELOPMENT.md` includes requirements, `.env` and User Secrets, EF Core migrations, broker startup and application commands. Read this as the authoritative detailed runbook; the README is only an entry point.
- `infrastructure/compose.yaml` starts SQL Server 2022 Developer and RabbitMQ by default; Kafka 4.4.0 is opt-in via `--profile kafka`. One SQL Server container hosts distinct logical databases.
- `dotnet-tools.json` pins `dotnet-ef` 10.0.14. `global.json` selects the Microsoft.Testing.Platform runner (it does **not** pin a precise .NET SDK version).
- `docs/ALLOCATION_E2E.http` supplies named HTTP requests for the capacity-2 example, and `docs/ALLOCATION_E2E_SEED.sql` requires the *new* SessionId.
- `infrastructure/init-kafka-topics.ps1` initializes versioned topics (three partitions / seven-day retention on the finalization topic).
- `.github/workflows/dotnet-ci.yml` restores, builds and tests the solution on GitHub Actions, using Docker for Testcontainers.

**Documentation checks to resolve in M3.6E/F:**

- The README's short numbered Quick Start currently presents migration application before infrastructure startup, while a new database migration requires the SQL Server container to be available. Reorder the README instructions so the SQL container starts **before** migrations. The detailed Local Development guide already describes this correctly.
- Some milestone-status statements are snapshots from the time the document was written. Check them against the merged PRs and remove any outdated "pending PR/CI" language during final documentation cleanup.
- Explain up front that the full 2/1 allocation demo still requires an explicit **development-only** inventory seed; it is not a fully automatic one-command bootstrap. Do not imply automatic inventory provisioning exists.

These are review findings, not proof of production defects. Do not rewrite the existing large documents before deciding on a minimal targeted update.

## 2. Stage A — Safe, non-destructive preflight of the existing checkout

Run from `C:\work\ConferenceProFlow` in **Visual Studio PowerShell Terminal**. Do not paste secret values into issues, screenshots or logs.

```powershell
pwd
```

```powershell
git status --short
```

```powershell
dotnet --version
```

```powershell
docker version
```

```powershell
docker compose version
```

```powershell
git check-ignore -v .env
```

```powershell
git ls-files -- .env
```

Expect .NET 10 installed, Docker Desktop usable, `.env` ignored and no output from `git ls-files -- .env`. The current checkout is the active M3.6 feature branch and may legitimately contain new documentation changes. There should be no *unexpected* source or secret file changes.

Check reviewer-facing tracked artifacts without moving or deleting files:

```powershell
'README.md','ConferenceProFlow.slnx','.env.example','infrastructure/compose.yaml','infrastructure/init-kafka-topics.ps1','docs/LOCAL_DEVELOPMENT.md','docs/ALLOCATION_E2E.http','docs/ALLOCATION_E2E_SEED.sql','docs/KAFKA_PUBLISHING.md','docs/ANALYTICS_WORKER.md','docs/KAFKA_REPLAY_SETUP.md' | ForEach-Object { '{0}: {1}' -f $_,(Test-Path $_) }
```

All expected values: `True`. Existing configuration should already be set in `.env` and per-project User Secrets; **do not overwrite** working credentials. Validate Compose without printing interpolated secrets:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka config --quiet
```

No output and exit code zero is a successful syntax/configuration check; it does not prove every container or application is healthy.

## 3. Stage B — Optional clean-clone source/build validation

This is the closest non-destructive substitute for an independent reviewer using a new machine. It validates the public `master` source, not the uncommitted M3.6 changes in your current working branch. The clone **must not** run Docker Compose or database migrations; the Compose project has a fixed name and could otherwise share the existing volumes.

Before running, ensure `C:\work\ConferenceProFlow-review` does not already exist. If it does exist, choose a different **new** directory; do not delete an existing copy.

```powershell
Test-Path 'C:\work\ConferenceProFlow-review'
```

Expect `False` before cloning:

```powershell
git clone https://github.com/AlanRacic/ConferenceProFlow.git C:\work\ConferenceProFlow-review
```

```powershell
Push-Location C:\work\ConferenceProFlow-review
```

```powershell
dotnet tool restore
```

```powershell
dotnet restore ConferenceProFlow.slnx
```

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release --no-restore
```

```powershell
Pop-Location
```

Record results: clean clone succeeded [ ], tool restore [ ], solution restore [ ], Release build [ ]. These commands should not access the user's operational SQL databases or start/stop shared Compose services. Do not add a second `.env` or copy User Secrets into the review checkout for this build-only check.

## 4. Stage C — Existing environment runtime walkthrough

Run only from the **original configured checkout**. If you merely want to audit documentation, this runtime step can be omitted with status `not run`.

Start local broker infrastructure (non-destructive; does not remove volumes):

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka ps
```

If Kafka topics have already been initialized, **do not reset them**. On a completely fresh installation, follow `docs/KAFKA_LOCAL_DEVELOPMENT.md` and inspect the versioned initialization script before running it.

For a **new** developer environment only, configure `.env` and individual project User Secrets as described in `docs/LOCAL_DEVELOPMENT.md`, run `dotnet tool restore`, then apply **existing** EF Core migrations (do not generate new migrations):

```powershell
$env:ASPNETCORE_ENVIRONMENT = 'Development'; $env:DOTNET_ENVIRONMENT = 'Development'
```

```powershell
dotnet ef database update --project src/ConferenceProFlow.Registration.Api --startup-project src/ConferenceProFlow.Registration.Api --context RegistrationDbContext
```

```powershell
dotnet ef database update --project src/ConferenceProFlow.Allocation.Worker --startup-project src/ConferenceProFlow.Allocation.Worker --context AllocationDbContext
```

```powershell
dotnet ef database update --project src/ConferenceProFlow.Analytics.Worker --startup-project src/ConferenceProFlow.Analytics.Worker --context AnalyticsDbContext
```

These migration commands are **not required** for a working database whose migrations are already applied. The Replay database is optional and **must not** be initialized or recreated merely for the normal quick-start.

Confirm User Secrets enable the five M2.6 messaging services. For the optional Kafka/Analytics path, enable the Registration Kafka publisher and Analytics durable consumer (with Replay disabled). Use the original user's working configuration; **do not display or paste secrets**.

Open **three Visual Studio terminals**, from the original repository root:

**Terminal 1 — Registration API**

```powershell
dotnet run --project src/ConferenceProFlow.Registration.Api/ConferenceProFlow.Registration.Api.csproj --launch-profile https
```

**Terminal 2 — Allocation Worker**

```powershell
dotnet run --project src/ConferenceProFlow.Allocation.Worker/ConferenceProFlow.Allocation.Worker.csproj --launch-profile ConferenceProFlow.Allocation.Worker
```

**Terminal 3 — Live Analytics Worker (optional Kafka test)**

```powershell
dotnet run --project src/ConferenceProFlow.Analytics.Worker/ConferenceProFlow.Analytics.Worker.csproj --launch-profile ConferenceProFlow.Analytics.Worker
```

The third terminal is for **live durable analytics**, not replay; ensure `AnalyticsReplay:Enabled=false`. The API's Development HTTPS endpoint is `https://localhost:7217`. Its `/health` endpoint is a basic application check, **not** a comprehensive dependency health check.

If conducting the full reviewer demonstration, follow **exactly** `docs/ALLOCATION_E2E_DEMO.md` and `docs/ALLOCATION_E2E.http`: create a new conference and session, seed `Capacity=2` with `docs/ALLOCATION_E2E_SEED.sql` in the **local** Allocation DB, then create three unique participants and registrations. Expect two `Confirmed`, one `Rejected` after asynchronous processing. For live Analytics, check `docs/ANALYTICS_E2E_CHECKS.sql` and `docs/ANALYTICS_E2E_DEMO.md`, with the new SessionId. This inserts new demo data; it does not delete old data.

**Do not rerun the M3.5 replay** during this audit: a fresh replay requires an empty replay database and previously unused replay consumer group. The successful replay from M3.5D is already documented, and reproducing it safely needs a separate explicit plan.

## 5. Stage D — Automated tests and documentation safety

The already-verified GitHub Actions pipeline is a separate machine/runner validation; an independent clean-clone source build strengthens that evidence, but neither proves complete operational onboarding without User Secrets and development-only seeding.

If running regression in the original checkout, with Docker Desktop available:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build --verbosity normal
```

Run with `--no-build` **only after** a successful fresh Release build; otherwise omit it. Integration tests use Testcontainers. Do not run destructive troubleshooting commands against existing project volumes or databases.

On completion, close the .NET processes with **Ctrl+C**. Leave the Docker volumes in place. You may leave Compose running; if stopping services, `docker compose ... down` without `-v` retains volumes, but no infrastructure shutdown is needed for this audit.

## 6. Reviewer audit outcome — fill in only after execution

| Check | Status | Evidence / notes |
| --- | --- | --- |
| Repo structure, no nested docs/docs | NOT RUN | |
| Source/README/runbook links | REVIEWED, manual link walk pending | |
| `.env` ignored and not tracked | NOT RUN in this audit | |
| Compose config `--quiet` | NOT RUN in this audit | |
| Independent clone without secrets | NOT RUN | |
| Restore and Release build from clean clone | NOT RUN | |
| Original configured environment starts | Previously demonstrated in development; M3.6D run pending | |
| New 2/1 reviewer demo | Previously demonstrated; M3.6D rerun optional | |
| Full solution test suite | Previously reported passing in M3.5; M3.6D rerun optional | |

**Provisional decision:** Reproducibility is **plausible and strongly supported** by the existing guides, prior local E2E runs and CI; it is **not yet independently verified as a first-time end-to-end onboarding experience**. Do not mark the audit fully PASS until at least the clean-clone restore/build and first-time configuration instructions have been checked.

## 7. Targeted follow-ups (M3.6E/F, not scope expansion)

1. Correct the README Quick Start ordering: start SQL infrastructure **before** running EF Core migrations.
2. Replace outdated M3.5 "CI pending" statements now that the PR is merged and CI passed.
3. Add a short prominent note that the full allocation demo needs manual local-only inventory provisioning.
4. Keep snapshot-location durability and Kafka publisher permanent-failure visibility as separate scoped hardening decisions from M3.6A/B; do not confuse those with onboarding defects.

**Explicitly out of scope:** complete production deployment, new auth infrastructure, automatic inventory provisioning, Kubernetes, new cloud services, schema changes, replay resets, or deleting local data to make the demo appear clean.
