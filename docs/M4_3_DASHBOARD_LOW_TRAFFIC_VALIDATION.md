# M4.3D3 — Dashboard verification for low-traffic development demos

**State:** Verification runbook; record observed results before declaring complete.  
**Branch:** `feature/metrics-and-dashboards`  
**Dependency:** M4.3D2 completed with three .NET services exporting to Prometheus and the Registration → Allocation → Analytics E2E business scenario verified.

## Why this change?

The original M4.3 dashboard emphasizes operational `rate()`, `increase()` and p95 time series. Those are useful for sustained traffic but can appear empty or flat after a three-registration demo. This update **preserves all 12 original panels unchanged** and adds a clearly labeled low-volume section (1 explanatory panel and 4 raw-counter panels). No backend, business logic, SQL data, tracing, migrations, NuGet package, or Prometheus configuration is changed.

New panels:

| Panel | PromQL | Interpretation |
| --- | --- | --- |
| Registration finalizations — raw counter | `sum by (outcome) (conferenceproflow_registration_finalized_total)` | Counts exported committed finalizations by outcome; not a business ledger. |
| Outbox publication — raw counter | `sum by (pipeline, outcome) (conferenceproflow_outbox_publish_outcomes_total)` | Counts recorded publication outcomes, including retries/quarantines as separate labels. |
| Allocation inbox — raw attempt counter | `sum by (result) (conferenceproflow_allocation_inbox_processing_attempts_total)` | **Processing attempts**, not unique decisions. |
| Analytics Kafka — raw handling counter | `sum by (result) (conferenceproflow_analytics_kafka_handling_attempts_total)` | **Handling attempts**, not unique business events. |

**Important interpretation limits:** The raw counters are cumulative only during the lifetime of the exporting process; their values can reset when an app restarts. Multiple active instances or prior workload may change sums. A registration demo with 2 Confirmed and 1 Rejected in SQL does **not** imply all displayed counters must equal exactly 2, 1, or 3. Prometheus telemetry is best-effort; the SQL databases remain authoritative. Do not use `or vector(0)` to hide a missing exporter because that would mask missing instrumentation as zero workload.

## 1. Install manually (Visual Studio 2026)

1. Ensure current Git branch is `feature/metrics-and-dashboards`.
2. The package replaces **only** `infrastructure/metrics/grafana/dashboards/conferenceproflow-metrics-overview.json` and adds **one** document `docs/M4_3_DASHBOARD_LOW_TRAFFIC_VALIDATION.md`.
3. If you have manually customized this dashboard in Grafana, compare or export those changes first; Grafana file provisioning treats the repository JSON as the source of truth.
4. Copy the package's `COPY_TO_PROJECT` folder content into `C:\work\ConferenceProFlow`; accept replacement of the *one* dashboard JSON.
5. Add the new Markdown document to **Solution Items → Add → Existing Item**, without moving the file.

## 2. Validate that Grafana reloads the file

- Leave Prometheus, Grafana and the three .NET services running if they are still active from M4.3D2.
- Grafana's existing provider polls the dashboard directory every 30 seconds; reopening or refreshing the dashboard should show the new section at its bottom. Grafana UI: `http://127.0.0.1:3000` → **Dashboards** → **ConferenceProFlow** → **ConferenceProFlow | M4.3 Metrics Overview**.
- If the new section does not appear despite the JSON file being saved, restart **Grafana only** from a new terminal at repository root: `docker compose -f infrastructure/compose.metrics.yaml restart grafana`. This does not delete data/volumes.
- Use **Last 30 minutes** if M4.3D2 was just completed; choose a time range that actually includes the measurements, and Refresh.

## 3. Verify both operational and raw panels

1. Check that the **original 12 panels** are still present and display the same existing information. Some p95/rate charts may legitimately have no significant line with only three events.
2. Scroll down to **Low-traffic verification — cumulative counters**. Verify that the four new raw-counter panels have series for the outcomes/results actually produced. A series absent for an outcome that never happened is normal; `No data` across all four panels is not normal if you previously confirmed fresh OTLP metrics.
3. In `docs/M4_3_PROMETHEUS_QUERIES.http`, rerun requests 05–09 or query the four PromQL expressions from the table directly in Prometheus **Query**. Raw counters should be visible when the corresponding application produced and exported that event. Match labels such as `outcome`, `pipeline`, and `result`.
4. If `No data` appears in one new panel, first test the *same* raw expression directly in Prometheus. If raw Prometheus data exists but Grafana is empty, check the panel's datasource and query; do **not** change business logic or invent additional registrations just to fill charts.
5. Record a short result in the working copy of this Markdown file or your validation notes: date, whether original panels survived, presence of the four new panels, any missing series, and any relevant warnings. Do not paste credentials, User Secrets, database connection strings, or identifiable personal data into screenshots/docs.

## 4. Verification acceptance criteria

- [ ] Existing Grafana dashboard remains provisioned under the same title/UID and the original panels remain intact.
- [ ] New *low-traffic verification* section appears.
- [ ] All four new panel queries are syntactically valid and show corresponding data when that application has emitted events.
- [ ] `No data` vs `0` and attempts vs business outcomes are correctly understood.
- [ ] Underlying M4.3D2 E2E results remain validated independently in SQL.
- [ ] No changes are made to application code or production behavior.

## 5. Next step

M4.3E: run **targeted integration-test projects separately**, then full solution Release Build, documentation/Git review, PR linked to the M4.3 GitHub issue, and CI. Do **not** run all Testcontainers-backed integration projects concurrently on the local machine; this is a test-run orchestration lesson, not a feature-scope limitation.
