"""Bronze ingestion job.

Incremental by watermark: only source files modified since the last successful run
are read. Bronze is append-only and untyped by design - the raw record is preserved
so any downstream bug can be fixed by replaying from Bronze rather than re-pulling
from the source system.

    python -m lakehouse.jobs.ingest_bronze --env dev --dataset orders
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from lakehouse.config import load_dataset
from lakehouse.governance.lineage import LineageEdge
from lakehouse.io.delta_writer import DeltaWriter
from lakehouse.io.readers import read_source
from lakehouse.jobs._common import JobContext, base_parser, build_context, finalise
from lakehouse.logging_utils import get_logger, run_id, timed
from lakehouse.monitoring.metrics import PipelineMetrics
from lakehouse.transforms.bronze import standardise_bronze

log = get_logger(__name__)


def ingest(
    ctx: JobContext, dataset_name: str, full_refresh: bool = False, dry_run: bool = False
) -> PipelineMetrics:
    """Read new source files and append them to the Bronze Delta table."""
    from pyspark.sql import functions as F

    ds = load_dataset(dataset_name)
    cfg = ctx.cfg
    target = cfg.fqn("bronze", ds.bronze_table) if not cfg.is_local else f"bronze.{ds.bronze_table}"
    metrics = PipelineMetrics(
        job_name=ctx.job_name, dataset=ds.name, layer="bronze", stage="ingest"
    )

    # The Bronze watermark is a *file discovery* boundary, so it must live in the
    # same clock domain as the thing it is compared against (file modification
    # time) - not the business `updated_at`, which can be backdated or future-dated
    # by the source system. Taking it before the read means a file landing mid-run
    # is simply picked up next time: at-least-once, never lost.
    discovery_boundary = datetime.now(timezone.utc)

    since = None if full_refresh else ctx.watermarks.get(ds.name, "bronze")
    if full_refresh:
        log.info("bronze.full_refresh", extra={"dataset": ds.name})

    with timed("bronze.read", log, dataset=ds.name):
        raw = read_source(ctx.spark, ds, cfg, since=since)
        df = standardise_bronze(
            raw, source_system=ds.tags.get("domain", "unknown"), batch_id=run_id()
        )
        rows = df.count()

    metrics.rows_read = rows
    if rows == 0:
        log.info(
            "bronze.no_new_data",
            extra={"dataset": ds.name, "since": since.isoformat() if since else None},
        )
        return metrics.finish("skipped")

    # Enforce the schema contract before anything is written.
    diff = ctx.schemas.enforce(df, ds.name, "bronze", policy=ds.schema_evolution, run_id=run_id())
    if not diff.is_empty:
        log.info("bronze.schema_evolved", extra={"dataset": ds.name, "diff": diff.summary()})

    if dry_run:
        log.info("bronze.dry_run", extra={"dataset": ds.name, "rows": rows, "target": target})
        return metrics.finish("dry_run")

    writer = DeltaWriter(ctx.spark, auto_optimize=cfg.auto_optimize)
    # Bronze always partitions by ingest date: it bounds every incremental read and
    # makes a "replay just this batch" operation a partition filter.
    writer.create_if_missing(
        df,
        target,
        partition_by=["_ingest_date"],
        location=cfg.layer_path("bronze", ds.bronze_table) if cfg.is_local else None,
        comment=f"Raw append-only landing for {ds.name}. Source fidelity preserved.",
    )
    with timed("bronze.write", log, dataset=ds.name, rows=rows):
        result = writer.append(df, target, partition_by=["_ingest_date"], merge_schema=True)

    # Advance the watermark only after a successful commit.
    ctx.watermarks.advance(ds.name, "bronze", discovery_boundary, rows, run_id())
    # The max business timestamp is still recorded, but as a *metric* for freshness
    # dashboards rather than as the incremental filter.
    max_data_ts = None
    if ds.watermark_column in df.columns:
        max_data_ts = df.agg(F.max(F.col(ds.watermark_column).cast("timestamp"))).collect()[0][0]

    ctx.lineage.record(
        LineageEdge(
            dataset=ds.name,
            source_layer="source",
            target_layer="bronze",
            source_object=ds.source_path,
            target_object=target,
            operation="append",
            rows_in=rows,
            rows_out=result.rows_written,
            metrics={"schema_diff": diff.summary()},
        )
    )

    metrics.rows_written = result.rows_written
    metrics.rows_inserted = result.rows_inserted
    metrics.table_version = result.version
    metrics.watermark = max_data_ts or discovery_boundary
    return metrics.finish("success")


def main(argv: list[str] | None = None) -> int:
    parser = base_parser(__doc__ or "Bronze ingestion")
    args = parser.parse_args(argv)

    ctx = build_context("ingest_bronze", args.env)
    try:
        metrics = ingest(ctx, args.dataset, args.full_refresh, args.dry_run)
        ctx.metrics.record(metrics)
        ctx.alerts.check_volume(args.dataset, "bronze", metrics.rows_written)
        return 0 if metrics.status in ("success", "skipped", "dry_run") else 1
    except Exception as exc:
        failed = PipelineMetrics(
            job_name=ctx.job_name, dataset=args.dataset, layer="bronze", stage="ingest"
        ).finish("failed", str(exc))
        ctx.metrics.record(failed)
        log.error(
            "bronze.job_failed", extra={"dataset": args.dataset, "error": str(exc)}, exc_info=True
        )
        raise
    finally:
        finalise(ctx)


if __name__ == "__main__":
    sys.exit(main())
