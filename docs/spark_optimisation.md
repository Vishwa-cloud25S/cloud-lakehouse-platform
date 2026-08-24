# Spark optimisation

Every configuration flag in `src/lakehouse/spark_session.py` is deliberate. This
is what each one does and when it matters.

## Adaptive Query Execution

```python
"spark.sql.adaptive.enabled": "true"
"spark.sql.adaptive.coalescePartitions.enabled": "true"
"spark.sql.adaptive.skewJoin.enabled": "true"
"spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m"
```

AQE re-plans using runtime statistics instead of compile-time guesses.

`coalescePartitions` is the one you feel immediately: without it a job with
`shuffle.partitions = 200` writes 200 files regardless of whether the result is
10 GB or 10 KB. With it, small results collapse to a handful of tasks.

`skewJoin` splits an oversized partition automatically — the fix for the classic
"199 tasks finished in 8 seconds, one is still running after 40 minutes" caused
by a single hot key.

## Small files

```python
"spark.databricks.delta.optimizeWrite.enabled": "true"
"spark.databricks.delta.autoCompact.enabled": "true"
```

Micro-batch ingestion is a small-file factory. Every file is a separate remote
read plus a task, so query latency degrades roughly linearly with file count.
`optimizeWrite` shuffles before writing to hit the target file size;
`autoCompact` runs a mini-OPTIMIZE after a commit that produced many small files.

Both are disabled in the local profile — they need the Databricks runtime and
add pure overhead on a laptop.

## Deletion vectors

```python
"spark.databricks.delta.properties.defaults.enableDeletionVectors": "true"
```

Without them, updating one row in a 500 MB Parquet file rewrites all 500 MB.
Deletion vectors mark the row as removed in a side file and let the next OPTIMIZE
do the physical rewrite. For MERGE-heavy workloads this is often a several-fold
improvement.

## Joins

Dimensions are broadcast explicitly with `F.broadcast(...)` rather than relying on
`autoBroadcastJoinThreshold`. Broadcasting sends the small side to every executor
and eliminates the shuffle of the large side entirely — a `dim_product` of a few
hundred rows joined to a billion-row fact should never trigger a shuffle join.
Explicit is better because the estimator gets it wrong after filters and unions.

## Partitioning

The rules encoded in `PartitionAdvisor`:

- Target **~1 GB per partition**. Below ~128 MB, small-file overhead dominates.
- Keep partition count in the **low thousands**.
- **Never** partition on a high-cardinality column. `order_id` as a partition
  column means one directory per order.
- Below ~1 GB total, do not partition at all.

The advisor is a pure function and unit-tested:

```python
recommend_partitions(100_000_000, 50*GB, {"order_date": 1200, "order_id": 100_000_000, "country": 12})
# [info]     country:    Good candidate: 12 partitions averaging 4.17 GiB.
# [warn]     order_date: Average partition would be 42.7 MiB (target ~1 GiB): too small
# [critical] order_id:   100,000,000 distinct values exceeds 10,000: directory explosion
```

`fact_orders` is partitioned by `order_month`, not `order_date`: ~30× fewer
directories, and BI filters are almost always month- or quarter-grained.

## Z-ORDER vs partitioning

Partitioning gives coarse, directory-level pruning. Z-ORDER co-locates rows
sharing values of the chosen columns *within* files so Delta's min/max statistics
can skip most of the table.

Rule of thumb: **partition** on low-cardinality columns you filter on, **Z-ORDER**
on high-cardinality columns you filter or join on. Z-ORDER on the partition
column is wasted work.

```sql
OPTIMIZE gold.fact_orders
  WHERE order_month >= date_format(current_date() - INTERVAL 3 MONTHS, 'yyyy-MM')
  ZORDER BY (customer_key, product_key);
```

The `WHERE` matters: OPTIMIZE without it rewrites the whole table every night.

## Maintenance ordering

`OPTIMIZE → VACUUM → ANALYZE`, nightly, in a job separate from ingestion so
housekeeping can never delay a data SLA. Each step is independent — one table's
failure does not stop the rest, because maintenance is best-effort housekeeping,
not a data-correctness operation.

`VACUUM RETAIN 168 HOURS` (7 days) preserves time travel and protects in-flight
readers. Shortening it is a deliberate cost-vs-recoverability trade, and the
retention check exists precisely to make that deliberate.

## Other settings

| Setting | Reason |
|---|---|
| `spark.sql.session.timeZone = UTC` | Timestamp comparisons across a DST boundary are otherwise subtly wrong. |
| `partitionOverwriteMode = dynamic` | `replaceWhere` rebuilds only touched partitions. |
| `KryoSerializer` | Materially faster than Java serialisation for shuffle. |
| `io.cache.enabled` (cluster only) | Caches remote Parquet on local SSD; large win on repeated Gold scans. |
| `schema.autoMerge = false` globally | Enabled only around a MERGE that expects it, so a typo cannot silently add a column. |
