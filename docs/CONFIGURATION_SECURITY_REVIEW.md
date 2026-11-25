# ConferenceProFlow — Configuration and Security Hygiene Review

**Milestone:** M3 — Kafka & Event Analytics  
**Work item:** M3.6C — Configuration & Security Hygiene  
**Review date:** 2026-11-25  
**Status:** Repository-level static review, with local verification checklist outstanding  
**Feature branch:** `chore/enterprise-integration-hardening`

> **Scope and evidence boundary:** The observations below are based on publicly accessible files on the `master` branch of `AlanRacic/ConferenceProFlow` reviewed on 2026-11-25 and the documented M3.3–M3.5 behavior. The active M3.6 feature branch, the developer's actual `.env` and User Secrets, Git history, local Docker state, and security settings have **not** been inspected. This is not a penetration test, a credential leak scan of all Git history, or a claim of production readiness. Local checks below print **filenames or statuses only** and must not be accompanied by secret values.

## 1. Objective

Check whether a new developer can configure ConferenceProFlow safely for **local, zero-cost development** without exposing credentials or accidentally running Kafka replay against live analytics. Preserve the existing, tested messaging architecture. Do not modify production-oriented behavior under the guise of a configuration review.

## 2. Repository observations

| ID | Reviewed item | Evidence from repository | Assessment / action |
| --- | --- | --- | --- |
| CFG-01 | `.env.example` | Includes `MSSQL_SA_PASSWORD=`, `RABBITMQ_USER=conferenceproflow`, and `RABBITMQ_PASSWORD=`. Credentials are blank placeholders. | **Good.** Use it as the template; never put real secrets in it. |
| CFG-02 | `.gitignore` | Excludes `.env` and `.env.*` while explicitly allowing `.env.example`. | **Good.** Verify that the actual local `.env` is ignored **and not tracked**. `.gitignore` does not undo a previous `git add`. |
| CFG-03 | `infrastructure/compose.yaml` | Requires SQL and RabbitMQ credentials from `.env`, binds SQL `14333`, RabbitMQ `5672`/`15672`, and Kafka `9092` to host `127.0.0.1`; Kafka is optional under the `kafka` profile. | **Appropriate local boundary.** It is not a security boundary for cloud hosting or untrusted users. |
| CFG-04 | Kafka transport | Uses PLAINTEXT listeners for local Kafka KRaft; single-node broker/controller. | **Known local-only limitation.** Do not expose this broker outside trusted development infrastructure. |
| CFG-05 | SQL development | SQL Server 2022 Developer and an `sa` account are configured for local Docker. Documentation uses `Encrypt=True;TrustServerCertificate=True` for development connection strings. | **Acceptable local demo shortcut.** Not a production identity/PKI strategy. |
| CFG-06 | Versioned `appsettings.json` | Reviewed Registration, Allocation and Analytics appsettings. They contain logging and feature defaults rather than real database/RabbitMQ passwords. Allocation worker's inbox/outbox features are disabled by default; Analytics Kafka consumer is disabled by default. | **Good baseline.** Feature flags must be deliberately enabled and credentials supplied outside version control. |
| CFG-07 | User Secrets | Local development guides use each project's **Manage User Secrets** and distinguish the three service-owned connection strings plus separate replay connection string. | **Appropriate for development only.** User Secrets are *not encrypted*; never dump their values into CI logs or tickets. |
| CFG-08 | Live vs. Replay Analytics | `AnalyticsReplay:Enabled` and `AnalyticsKafkaConsumer:Enabled` select mutually exclusive modes with additional replay safety validation; replay uses an isolated SQL database and separate consumer group. | **Positive isolation design.** Preserve distinct connection strings and consumer group IDs. Validate locally before any replay. |
| CFG-09 | GitHub Actions | Workflow checks out source, restores dependencies, builds Release, verifies Docker and runs solution tests on PRs and `master` pushes. | **Good CI baseline.** Green CI is not a security scan or a test of personal User Secrets. |
| CFG-10 | Registration API auth | README explicitly states authentication/authorization is not implemented. | **Known security limitation.** Run as a local/demo API only, not a publicly exposed service. |

### Important nuance: no evidence of a current outbox backlog

On 2026-11-25 the developer ran the read-only M3.6B query against `ConferenceProFlowRegistration` and reported **zero Analytics Outbox rows with `PublishedAtUtc IS NULL`**. This supports *no current unpublished events in that query*, but it is **not** a prediction about future publisher failures or proof that every past event was semantically correct.

## 3. Required local checks — no secret values displayed

Run from `C:\work\ConferenceProFlow` in a **PowerShell** terminal, on `chore/enterprise-integration-hardening`. These operations are read-only and do not start or stop containers.

**A. Confirm correct Git branch and expected changes:**

```powershell
git branch --show-current
```

```powershell
git status --short
```

**B. Confirm local `.env` is ignored:**

```powershell
git check-ignore -v .env
```

Expected: the matching `.gitignore` rule. If `.env` does not exist yet, this command can still show the matching ignore rule.

**C. Check whether `.env` is already tracked (expected: no output):**

```powershell
git ls-files -- .env
```

**D. Inspect the names of tracked secret-like files without printing content:**

```powershell
git ls-files | Select-String -Pattern '(^|/)secrets\.json$|(^|/)\.env($|\.)' | Where-Object { $_.Line -notmatch '(^|/)\.env\.example$' }
```

Expected: no output. Investigate *filenames only* if anything appears; do not publish file contents. This is a narrow check, **not a comprehensive secrets scanner** and it does not search prior Git history.

**E. Validate Docker Compose interpolation without showing resolved credentials:**

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka config --quiet
```

Expected: exit code 0 / no output. This checks Compose syntax and required variables, not broker connectivity or whether passwords are strong. **Do not use `docker compose config` without `--quiet` in screenshots or public logs**: it can expand and print secret values.

**F. Check formatting after adding this document:**

```powershell
git diff --check
```

Expected: no output. New untracked files are not covered until staged.

### User Secrets: manually confirm **keys**, not values

In Visual Studio, right-click each service -> **Manage User Secrets**. Verify that the correct keys exist, but **do not paste or screenshot their values**:

| Application | Key(s) to verify | Local-only guard |
| --- | --- | --- |
| Registration API | `ConnectionStrings:RegistrationDb`, `RabbitMq:*`, `Kafka:BootstrapServers`, `Kafka:RegistrationFinalizedTopic`, `KafkaAnalyticsPublisher:Enabled`, `OutboxPublisher:Enabled`, `AllocationResultConsumer:Enabled` | Use only the Registration database; enable message services only for the corresponding demo. |
| Allocation Worker | `ConnectionStrings:AllocationDb`, `RabbitMq:*`, `RabbitMqConsumer:Enabled`, `AllocationInboxProcessor:Enabled`, `AllocationOutboxPublisher:Enabled` | Use only the Allocation database. |
| Analytics Worker (live) | `ConnectionStrings:AnalyticsDb`, `AnalyticsKafkaConsumer:Enabled`, `AnalyticsKafkaConsumer:ObserveOnly`, `AnalyticsKafkaConsumer:Topic`, `AnalyticsKafkaConsumer:GroupId` | Durable live group: `conferenceproflow.analytics.v1`. |
| Analytics Worker (replay) | `ConnectionStrings:AnalyticsReplayDb`, `AnalyticsReplay:Enabled`, `AnalyticsReplay:GroupId`, `AnalyticsReplay:Topic`, `AnalyticsReplay:Resume` | Must target `ConferenceProFlowAnalyticsReplay` and a *separate* replay group. Never point replay at live SQL. |

The Analytics Worker uses a **single** User Secrets file for live and replay settings. When configuring replay, verify that `AnalyticsKafkaConsumer:Enabled=false`. In normal operation keep `AnalyticsReplay:Enabled=false` unless deliberately running a controlled replay. Preserve both connection strings. Some optional keys may be provided by defaults; do not overwrite valid configuration unnecessarily.

**Do not execute** `dotnet user-secrets list`, copy `.env`, print connection strings, or run plain `docker compose config` when preparing evidence for GitHub: those may expose real values. Never upload local secrets for this review.

## 4. Risks and decisions

| ID | Classification | Risk / decision | Next step |
| --- | --- | --- | --- |
| SEC-01 | Accepted local-demo limitation | Kafka PLAINTEXT, SQL Developer `sa`, self-trusted dev certs, no production-grade secrets management. | Keep clearly documented; do not present as production-ready. |
| SEC-02 | Accepted current limitation | Registration API has no authentication/authorization. | Do not deploy publicly; consider auth as separate future scope, not an M3.6 quick fix. |
| SEC-03 | Local verification required | Actual `.env`, untracked files, Developer User Secrets and past Git commits cannot be inspected from public `master`. | Run safe commands above. If a real secret is discovered in tracked files/history, stop and rotate/revoke it before normal Git cleanup. |
| SEC-04 | Existing safety feature | Replay isolation prevents mixing live and replay database/group by configuration. | Preserve safety tests and add a wrong-target negative test only if a real coverage gap is found. |
| SEC-05 | Operational improvement candidate | Replay snapshot stored under a temporary directory may disappear before an explicit resume. | Decide on configurable durable local snapshot path in M3.6E with targeted tests. |
| SEC-06 | Operational improvement candidate | Permanent Kafka outbox failures are retried without a dedicated operator escalation policy. | Consider visible failed/pending metrics or clear thresholds; do not mark events as published or silently drop them. |
| SEC-07 | Documentation maintenance | README status text still indicates M3.5 PR/CI pending although the developer reported it merged. | Update accurately during M3.6F documentation finalization, once final Git state is confirmed. |

## 5. Recommended outcome for M3.6C

1. Add this review to the repository's **existing** `docs/` directory, not `docs/docs/`.
2. Verify `.env` ignore/tracking, tracked secret-like filenames, and Compose interpolation **without printing actual secrets**.
3. Verify each service's configuration keys and live/replay separation manually.
4. If the checks pass: **do not change the application, Docker Compose, migrations or credentials**. Document successful local verification and move to M3.6D (fresh-machine/local reproducibility), then implement only justified M3.6E hardening.
5. If a check fails: stop; investigate the narrow finding before committing. A wrong `gitignore` rule is a separate issue from a secret already present in Git history.

## 6. Primary repository references (reviewed 2026-11-25)

- [Root `.env.example`](https://github.com/AlanRacic/ConferenceProFlow/blob/master/.env.example)
- [Root `.gitignore`](https://github.com/AlanRacic/ConferenceProFlow/blob/master/.gitignore)
- [Docker Compose infrastructure](https://github.com/AlanRacic/ConferenceProFlow/blob/master/infrastructure/compose.yaml)
- [Registration appsettings](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Registration.Api/appsettings.json)
- [Allocation Worker appsettings](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Allocation.Worker/appsettings.json)
- [Analytics Worker appsettings](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Analytics.Worker/appsettings.json)
- [Analytics Worker Program.cs](https://github.com/AlanRacic/ConferenceProFlow/blob/master/src/ConferenceProFlow.Analytics.Worker/Program.cs)
- [GitHub Actions workflow](https://github.com/AlanRacic/ConferenceProFlow/blob/master/.github/workflows/dotnet-ci.yml)
- [Local development documentation](https://github.com/AlanRacic/ConferenceProFlow/blob/master/docs/LOCAL_DEVELOPMENT.md)
- [Root README](https://github.com/AlanRacic/ConferenceProFlow/blob/master/README.md)

**Evidence to record after local checks:** Date; branch; `.env` ignored?; `.env` untracked?; tracked secret-like paths absent?; Compose `config --quiet` passed?; live/replay keys and groups verified?; any follow-up finding? **Never record actual secret values.**
