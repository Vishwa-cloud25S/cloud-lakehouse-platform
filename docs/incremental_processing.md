# Incremental processing

Reprocessing everything on every run is the most common reason a pipeline that
worked at 10 GB collapses at 1 TB. This project is incremental at every layer.

## Watermarks

State lives in `ops.watermarks`, a Delta table keyed by `(dataset, layer)`:

```
dataset  layer   watermark            rows_processed  run_id      updated_at
orders   bronze  2026-08-24 16:14:43  6064            94c4770ea2  2026-08-24 16:14:50
orders   silver  2026-08-24 16:15:12  5848            94c4770ea2  2026-08-24 16:15:20
```

Because it is Delta, the watermark advances transactionally with the data and is
time-travellable — you can answer "what did the pipeline think the position was
last Tuesday?" with `VERSION AS OF`.

`advance()` refuses to move a watermark backwards. A late or duplicated run
therefore cannot cause silent reprocessing of already-committed data.

## Two clock domains

This is the subtle part, and getting it wrong causes data loss.

- **Bronze** filters on *file modification time*, so its watermark must also be a
  wall-clock discovery boundary, captured **before** the read.
- **Silver** filters on `_ingest_timestamp`, a column Bronze itself wrote, so its
  watermark lives in that same domain.

Using the business `updated_at` as the Bronze watermark seems natural and is
wrong: a source that backdates records would have them permanently skipped,
because their `updated_at` is below a watermark set by other rows in the same batch.

The maximum business timestamp is still recorded — as a *metric* for freshness
dashboards, not as the filter.

## Cold start and backfill

A missing watermark returns epoch, so the first run is a full backfill with no
special-casing. `--full-refresh` ignores the watermark deliberately:

```bash
python -m lakehouse.jobs.ingest_bronze --env prod --dataset orders --full-refresh
```

Because writes are MERGEs on business keys, a backfill overwrites rather than
duplicates. That is what makes a full refresh a safe recovery action.

## Idempotency

Every write is either a MERGE on the primary key or a partition-scoped overwrite
(`replaceWhere`). Re-running any job converges to the same state.

Verified in CI: the end-to-end workflow runs the pipeline twice and asserts the
second run reads 0 rows and changes no counts.

## Partition pruning on MERGE

A naive MERGE scans the entire target to find matches. `DeltaWriter.merge_upsert`
collects the distinct partition values present in the incoming batch and appends
them to the merge condition:

```sql
MERGE INTO gold.fact_orders t
USING batch s
  ON t.order_id = s.order_id
 AND t.order_month IN ('2026-08')   -- ← added automatically
```

On a multi-terabyte fact table this is the single largest MERGE optimisation
available: a daily load touches one partition instead of sixty.

## Change Data Feed

Gold tables enable `delta.enableChangeDataFeed`, so a future downstream consumer
can read *only what changed* rather than diffing snapshots:

```sql
SELECT * FROM table_changes('gold.fact_orders', 120, 125);
```
