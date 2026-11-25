# Replay Snapshot Storage — M3.6E1 Hardening

## Scope

The bounded Kafka Replay Runner keeps one JSON manifest per replay consumer group. This manifest records the original partition windows and the completion status. Kafka committed offsets remain authoritative for progress after a restart. The replay SQL database and Kafka group are separate from live analytics.

Previously, `KafkaAnalyticsReplayRunner.GetManifestPath(...)` placed manifests under the OS temporary directory. Temporary storage may be cleaned by the OS or an administrator, making an interrupted replay impossible to resume safely.

## Behavior after M3.6E1

- **New default:** the current operating system user's persistent local application-data folder, under `ConferenceProFlow/ReplaySnapshots`. On Windows this is normally `%LOCALAPPDATA%\ConferenceProFlow\ReplaySnapshots`.
- **Optional override:** `AnalyticsReplay:SnapshotDirectory` must be an **absolute** path, ideally outside the Git repository and outside temporary folders. The same value must be used for the original run and `Resume=true`.
- **Stable naming:** the manifest is named with the existing stable hash derived from the replay `GroupId`; no changes to snapshot JSON format, Kafka offsets, SQL schema, or message processing.
- **Fail closed:** if the local application-data folder cannot be determined and no override is supplied, startup fails. Invalid relative overrides are rejected. There is no automatic fallback to OS temp storage.
- **Atomic writes remain unchanged:** snapshot JSON is written through `ReplaySnapshotFile.SaveAsync` using a temporary sibling file followed by atomic replacement. The local per-group `.lock` remains in the same directory.

### Configuration example (User Secrets)

Add only this property to the existing `AnalyticsReplay` section if you want an explicit Windows path:

```json
"SnapshotDirectory": "C:\\work\\ConferenceProFlow-ReplaySnapshots"
```

The path above is an example; **the default needs no configuration change**. Do not commit local User Secrets or snapshot files.

### Existing manifests from M3.5 (important)

This change does **not** silently migrate or delete legacy manifests. Your earlier M3.5 replay demo may have left a completed manifest in `%TEMP%\ConferenceProFlow\ReplaySnapshots`. That completed historical manifest may remain there; you do not need to rerun the replay or delete your SQL data.

If you have an **unfinished replay** that you need to resume, before enabling `Resume=true`, copy its original `<group-hash>.json` into the new persistent snapshot directory (or choose an explicit `SnapshotDirectory` where you place it). Preserve the **same group ID**, original JSON contents, existing Replay SQL database and Kafka offsets. Do not create a fresh replay using the same group and do not reset the live consumer group. Preserve your original file as backup until verified. Never overwrite a manifest of another run.

For a fresh replay choose a **new** replay consumer `GroupId` and an empty, separate replay database as already documented in `docs/KAFKA_REPLAY_RUNNER.md`.

## Verification

The added `KafkaReplaySnapshotTests` cases check default persistent path, an explicit absolute path, and rejection of relative paths. Existing replay integration tests verify snapshot persistence, bounded replay, resume and retention behavior. Because test code now uses the same default resolver as the runner, disposable Testcontainer scenarios remain isolated by random replay groups.

## Limits

A durable local directory reduces accidental cleanup, but it is **not** a distributed or backed-up manifest store. Loss of the local user profile or disk can still prevent safe continuation. Multi-machine replay coordination remains out of scope. Do not describe this as production high availability.
