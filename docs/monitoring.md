# Monitoring

## Structured logs

Every log line is a single JSON object, because Databricks ships driver stdout to
Log Analytics and grep-able free text does not survive that trip:

```json
{"ts":"2026-08-24T16:15:02","level":"INFO","logger":"lakehouse.io.delta_writer",
 "run_id":"94c4770ea220","msg":"delta.merge","table":"silver.silver_products",
 "mode":"merge","rows_written":197,"rows_inserted":197,"rows_updated":0,"version":3}
```

Every line carries `run_id`, so one correlation id ties together logs, metrics and
lineage for a run.

`safe_extra()` renames keys that collide with Python's `LogRecord` internals
(`message`, `name`, `module`…). Without it, an alert whose payload contains
`message` raises `KeyError: Attempt to overwrite 'message' in LogRecord` — the
alerting path crashes exactly when something is already wrong. That was a real
bug found during the build.

## Metrics

`ops.pipeline_metrics` gets one row per stage execution: rows read/written/
inserted/updated/quarantined, DQ pass rate, duration, table version, status,
error message.

Because it is Delta, the ops dashboard is built in Databricks SQL with the same
engine as the business dashboards — no separate metrics stack to run.

```sql
SELECT job_name, dataset, layer,
       count(*) AS runs,
       sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failures,
       round(avg(duration_s), 1) AS avg_duration_s
FROM ops.pipeline_metrics
WHERE started_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY failures DESC;
```

## Alerts

| Alert | Trigger | Why |
|---|---|---|
| `stale_data` | Newest record older than the dataset's `expected_freshness_hours` | The pipeline stopped and nobody noticed |
| `volume_anomaly` | Row count deviates >50% from the trailing median | Catches a truncated source file, which freshness alone misses |
| `quality_degraded` | DQ pass rate below threshold | The source is getting worse |
| `slow_job` | Duration over budget | Usually the first sign of a cost problem |
| `table_empty` | Zero rows | A failed load that "succeeded" |

Volume anomaly detection deserves emphasis: if a source drops from 6,000 rows to
600 but the file still arrives on time, freshness is green and the dashboard is
silently wrong. Comparing against the trailing median catches it.

## Where alerts go

- **Structured log event** — always, picked up by Log Analytics
- **Webhook** — optional, posts to Teams/Slack/PagerDuty
- **Databricks job email** — `on_failure` recipients from Terraform
- **Azure Monitor action group** — infrastructure alerts (storage capacity, budget)

The webhook path swallows its own exceptions: alerting must never be the reason a
job fails.

## Scheduled SLA check

`.github/workflows/scheduled-quality.yml` runs daily at 06:00 UTC, queries
`ops.dq_results` and `ops.pipeline_metrics` through Databricks SQL, and fails the
build on a breach — a silent data regression becomes a red build.

```bash
python scripts/check_quality_sla.py --env prod --min-pass-rate 0.95
```

## Dashboard queries

`sql/dbsql/02_ops_dashboard.sql` ships seven ready queries: pipeline health,
quality trend, freshness, quarantine analysis, schema evolution history, volume
anomaly detection and storage/file-count health.
