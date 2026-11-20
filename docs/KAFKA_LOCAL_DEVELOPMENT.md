# ConferenceProFlow — Kafka Local Development (M3.2)

**Scope:** Apache Kafka infrastructure, topic initialization, CLI smoke tests, and broker persistence/recovery in the existing Docker Compose environment. This guide does **not** describe an implemented Kafka publisher, Analytics Worker, or Analytics SQL database; those are planned for later M3 phases. See [`KAFKA_ANALYTICS_DESIGN.md`](KAFKA_ANALYTICS_DESIGN.md) for the proposed application architecture.

## 1. Prerequisites and boundaries

- Docker Desktop running Linux containers; Docker Compose available.
- Windows PowerShell / Visual Studio 2026 Terminal (commands below are each on one line).
- Repository root: `C:\work\ConferenceProFlow` (adjust if cloned elsewhere).
- A locally configured root `.env` containing the existing SQL Server and RabbitMQ secrets; this file stays out of Git.
- Existing `infrastructure/compose.yaml` with services `sqlserver`, `rabbitmq`, and optional `kafka`.

The optional Kafka service uses the pinned official image `apache/kafka:4.4.0`, single-node **KRaft** (no ZooKeeper), persistent named volume `kafka_data`, and a broker-readiness health check using `kafka-cluster.sh api-versions`. The broker uses plaintext listeners, loopback-only host binding, and replication factor 1: **local development only, not a production cluster**.

### Connection addresses

| Client location | Broker address | Explanation |
|---|---|---|
| Windows host / local .NET process | `localhost:9092` | Host listener mapped to `127.0.0.1:9092` |
| Another container in the same Compose network | `kafka:19092` | Docker network listener |
| Kafka CLI *inside the Kafka container* | `localhost:19092` | CLI is executed via `docker compose exec kafka` |

Do not use `localhost:9092` from a *different* container; there, `localhost` refers to that container, not the Kafka service.

## 2. Start and inspect Kafka

From the **repository root**:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka config --quiet
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka ps
```

Expect SQL Server and RabbitMQ to be running and Kafka eventually to become `healthy`. Kafka can initially report `health: starting` while it initializes. View Kafka logs when necessary:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka logs --tail=100 kafka
```

Confirm Kafka responds using the **non-deprecated** CLI command:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-cluster.sh api-versions --bootstrap-server localhost:19092
```

No Kafka application background service is required to be running for this infrastructure verification.

## 3. Initialize topics reproducibly

Once the broker is ready, run the checked-in, repeatable initialization script:

```powershell
.\infrastructure\init-kafka-topics.ps1
```

If Windows PowerShell reports **`running scripts is disabled on this system`**, the local execution policy is blocking `.ps1` files. After reviewing this repository-owned script, run it in a **separate, one-time PowerShell process**, without changing the machine-wide or user execution policy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\infrastructure\init-kafka-topics.ps1
```

Use this override only for scripts you trust and have inspected; do not change the system's global Execution Policy. On centrally managed machines, follow your organization's security policy.

The script is safe to run again: it uses `--if-not-exists`, **does not delete or recreate topics**, and verifies the expected partition count, replication factor, and retention configuration. It fails explicitly rather than silently accepting a pre-existing topic with different settings.

| Topic | Intended use | Partitions | Replicas | `retention.ms` |
|---|---|---:|---:|---:|
| `conferenceproflow.registration-finalized.v1` | Future `RegistrationFinalizedV1` integration events | 3 | 1 | 604800000 (7 days) |
| `conferenceproflow.kafka-smoke.v1` | **CLI test data only**, isolated from business events | 1 | 1 | 86400000 (1 day) |

**Why a separate smoke topic?** A smoke payload such as `{"kind":"M3.2-smoke"}` is **not** a valid `RegistrationFinalizedV1` contract. Sending new smoke messages to the analytics business topic would contaminate later consumer tests and could trigger malformed-message handling.

> During the initial manual M3.2 exercise, a smoke payload was already sent to `conferenceproflow.registration-finalized.v1`. Do not send further dummy messages there. Before enabling the real Analytics consumer in a later milestone, deliberately decide how to handle those local historical records: the consumer should validate/quarantine invalid events, or the development-only topic may be reset *explicitly*, with informed acceptance that only that topic's records would be removed. Do not silently reset data in this step.

Manually verify topics:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic conferenceproflow.registration-finalized.v1
```

Kafka may warn about metric-name collisions when a topic name contains a period (`.`) or underscore (`_`). This naming convention uses periods and hyphens consistently; avoid introducing another topic that differs only by replacing periods with underscores.

## 4. CLI producer/consumer smoke test

Use the **smoke** topic, not the future business-event topic. Send a keyed JSON message from PowerShell (using `-T` for piped stdin):

```powershell
Write-Output 'session-demo-001:{"kind":"M3.2-smoke","status":"ok"}' | docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic conferenceproflow.kafka-smoke.v1 --reader-property parse.key=true --reader-property key.separator=:
```

Read it, printing key, partition and offset:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic conferenceproflow.kafka-smoke.v1 --from-beginning --max-messages 1 --timeout-ms 10000 --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
```

Expected: the `session-demo-001` key, JSON value, partition 0 (the smoke topic has one partition), and an offset at or above 0. Repeating this command starts a **new CLI read from the beginning**, so the retained record can be read again. This is an elementary replay demonstration, **not** the application's future controlled consumer-group replay mechanism.

For the operational topic, the production-oriented partition key will be the canonical `SessionId` GUID string. Kafka ordering is guaranteed **within a partition**, not globally across all partitions.

## 5. Persistence and recovery demonstration

### Before stopping Kafka

Read the high-watermark (end) offsets of the operational topic:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic conferenceproflow.registration-finalized.v1 --time -1
```

Also record the smoke topic offsets, if desired:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic conferenceproflow.kafka-smoke.v1 --time -1
```

### Stop and recreate *only* Kafka

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka stop kafka
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d --no-deps --force-recreate kafka
```

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka ps
```

Wait for Kafka to report `healthy` and then **repeat topic description, offsets and consumer read**. A normal container recreation should preserve the same topic configuration and previously written messages because the `kafka_data` named Docker volume remains mounted. If no writes or retention cleanup occurred, end offsets should not change. Retention is time-based and cleanup is not immediate, so do not use old records as permanent recovery fixtures.

**What this proves:** local persistence across replacement of the Kafka container. **What it does not prove:** high availability, survival of the host disk's destruction, or multi-broker failover.

**Never use `docker compose down -v`** for this check: it deletes the Compose project's named volumes, including SQL Server, RabbitMQ, and Kafka data.

## 6. Stop, restart, and investigate

Stop only Kafka (other services continue):

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka stop kafka
```

Start Kafka again:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml --profile kafka up -d --no-deps kafka
```

Run the M2 infrastructure without bringing up Kafka:

```powershell
docker compose --env-file .env -f infrastructure/compose.yaml up -d
```

| Symptom | Check |
|---|---|
| Kafka reports `health: starting` | Allow broker/controller initialization; inspect `logs --tail=100 kafka` |
| Host producer cannot connect | Host bootstrap address is `localhost:9092`; check Docker port binding |
| Docker service cannot connect | Inside the Compose network use `kafka:19092`, not `localhost:9092` |
| `exec kafka` fails | Start Kafka with `--profile kafka up -d`; ensure it is running |
| Topic not found | Kafka auto-topic-creation is disabled; run `init-kafka-topics.ps1` |
| PowerShell refuses to load `.ps1` | Review the script, then use the one-process `powershell.exe -NoProfile -ExecutionPolicy Bypass -File ...` command in Section 3; do not change global policy |
| Topic initialization fails on existing topic | Inspect current partitions, replicas, retention: script intentionally avoids destructive corrections |
| Smoke consumer returns no record | Confirm producer succeeded and that the correct smoke topic was used |
| CLI warns about `kafka-broker-api-versions.sh` | Use `kafka-cluster.sh api-versions` instead |
| CLI warns about periods/underscores | Avoid metric-name-colliding topic names; warning alone does not mean creation failed |

## 7. Final checks and implementation boundaries

From the repository root, with Docker Desktop running for Testcontainers:

```powershell
dotnet build ConferenceProFlow.slnx --configuration Release
```

```powershell
dotnet test ConferenceProFlow.slnx --configuration Release --no-build
```

Existing M2 unit and integration tests must still pass. **M3.2 does not yet introduce .NET Kafka integration tests**, because there is no Kafka producer or consumer application code in this milestone. The Kafka CLI smoke and persistence checks are manually reproducible infrastructure verification.

### Explicit limitations

- Single Kafka node, replica factor 1, plaintext listeners, loopback host exposure: **developer workstation only**.
- The `kafka_data` named volume protects against normal container recreation but not machine or volume loss.
- Seven-day retention is not an archival guarantee; replay requires that records still exist within retention.
- `kafka-cluster.sh api-versions` checks broker API readiness, **not** end-to-end event business correctness.
- The M3.1 `RegistrationFinalizedV1` contract, Registration Analytics Outbox, Kafka publisher, Analytics Worker, SQL projections, and controlled consumer-group replay are **not implemented in M3.2**.

Continue with M3.3 only after this M3.2 infrastructure change is documented, validated, and merged.