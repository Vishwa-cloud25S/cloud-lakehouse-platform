# Cost considerations

Databricks and ADLS bills are dominated by a handful of decisions. These are the
ones this project makes, and why.

## Compute — the big one

| Decision | Effect |
|---|---|
| **Job clusters, not all-purpose** | Jobs DBUs are roughly half the all-purpose rate, and the cluster terminates when the run ends. |
| **Spot workers, on-demand driver** | 60–80% off worker compute. `first_on_demand = 1` keeps the driver on-demand so an eviction cannot kill the run and force a full retry. |
| **Autoscale 1 → N** | A small incremental batch does not pay for idle workers; the ceiling still absorbs a backfill. |
| **Cluster policy** | Caps `num_workers`, forces `autotermination_minutes = 20`, mandates cost tags. This is the guardrail that stops a test cluster from becoming a bill. |
| **One shared job cluster for the DAG** | Cluster startup is ~3 minutes. Reusing it across tasks is faster *and* cheaper than a cluster per task. |
| **Photon on the warehouse only** | Photon costs ~2× the DBU rate. It pays for itself on BI aggregation scans; it usually does not on a small ingest job. |

## SQL warehouse

`auto_stop_mins = 10` (5 in dev) is the single most important DBSQL setting. An
idle warehouse left running overnight can cost more than the entire pipeline.

Serverless is chosen for BI because start-up is seconds rather than minutes, so
aggressive auto-stop stays invisible to analysts.

## Storage

| Decision | Effect |
|---|---|
| **Bronze lifecycle tiering** | Hot → Cool at 30 days → Archive at 90. Bronze is write-once/read-rarely; Cool is roughly half Hot, Archive an order of magnitude less. |
| **Silver/Gold stay Hot** | They back interactive queries; Cool-tier read penalties would show up as dashboard latency. |
| **`VACUUM RETAIN 168 HOURS`** | Without VACUUM, storage grows forever — every rewritten file is retained. |
| **Blob versioning off** | Delta already versions. Enabling both roughly doubles storage for no benefit. |
| **Soft delete 7 days** | Cheap insurance against an accidental VACUUM or bad overwrite. |
| **LRS in dev, ZRS in prod** | Dev data is reproducible from `make seed`; paying for zone redundancy there is waste. |

## Query and pipeline design

- **Incremental everything** — a run that reads 6,000 new rows instead of 6,000,000
  is three orders of magnitude cheaper, and that gap widens every day
- **Partition-pruned MERGE** — restricting the merge condition to the batch's
  partitions avoids scanning the whole target
- **Pre-aggregated Gold** — `agg_daily_sales` has ~300 rows against ~6,000 fact
  rows. Power BI hitting the aggregate instead of the fact is a proportional
  reduction in warehouse time on every dashboard refresh
- **`update_condition = t.row_hash <> s.row_hash`** — unchanged rows are not
  rewritten, so a re-run costs almost nothing
- **OPTIMIZE with a `WHERE`** — compacting only recent partitions instead of the
  whole table nightly

## Guardrails

```hcl
notification {
  threshold      = 100
  threshold_type = "Forecasted"   # fires before the money is spent
}
```

Plus an actual-spend alert at 80%, storage capacity alerts, and a
`RUN_DURATION_SECONDS` health rule on the job — a pipeline that suddenly takes 3×
longer is usually a cost problem before it is a correctness problem.

## Tagging

Every resource carries `environment`, `cost_center`, `owner` and `project`, and
the cluster policy forces the same tags onto compute. Without this, Azure Cost
Management shows one undifferentiated Databricks line item and nobody can attribute
spend to a team or a pipeline.

## Rough dev-environment envelope

Order-of-magnitude only; actual figures depend on region, data volume and usage.

| Component | Configuration | Approx. monthly |
|---|---|---|
| ADLS Gen2 | ~100 GB, mostly Cool | $3–5 |
| Job cluster | 2 spot workers, ~1 h/day | $25–40 |
| SQL warehouse | 2X-Small serverless, ~2 h/day | $30–60 |
| Log Analytics | 30-day retention, low volume | $5–10 |
| **Total** | | **~$65–115** |

The dev budget is set to $100 with alerts at 80% actual and 100% forecast.
Long-term operational history belongs in `ops.pipeline_metrics` (cheap Delta),
not in Log Analytics at ~$2.30/GB.
