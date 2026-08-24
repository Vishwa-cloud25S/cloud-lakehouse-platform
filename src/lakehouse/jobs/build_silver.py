"""Silver build job.

Reads only the Bronze rows newer than the Silver watermark (via the Change Data
Feed when available, otherwise a watermark predicate), conforms and types them,
enforces the quality contract, quarantines bad rows, then MERGEs into Silver.

    python -m lakehouse.jobs.build_silver --env dev --dataset orders
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from lakehouse.config import load_dataset
from lakehouse.governance.lineage import LineageEdge
from lakehouse.io.delta_writer import DeltaWriter
from lakehouse.jobs._common import JobContext, base_parser, build_context, finalise
from lakehouse.logging_utils import get_logger, run_id, timed
from lakehouse.monitoring.metrics import PipelineMetrics
from lakehouse.quality.expectations import load_suite
from lakehouse.transforms.silver import conform

log = get_logger(__name__)


def build(
    ctx: JobContext, dataset_name: str, full_refresh: bool = False, dry_run: bool = False
) -> PipelineMetrics:
    from pyspark.sql import functions as F

    ds = load_dataset(dataset_name)
    cfg = ctx.cfg
    prefix_b = "" if cfg.is_local else f"{cfg.catalog}."
    source = f"{prefix_b}{cfg.schema('bronze')}.{ds.bronze_table}"
    target = f"{prefix_b}{cfg.schema('silver')}.{ds.silver_table}"

    metrics = PipelineMetrics(job_name=ctx.job_name, dataset=ds.name, layer="silver", stage="build")

    if not ctx.spark.catalog.tableExists(source):
        log.warning("silver.source_missing", extra={"source": source})
        return metrics.finish("skipped")

    since = None if full_refresh else ctx.watermarks.get(ds.name, "silver")
    bronze = ctx.spark.table(source)
    if since is not None and not full_refresh:
        # Incremental slice. `_ingest_timestamp` is monotonic per run, so this is
        # safe even when the source watermark column arrives out of order.
        bronze = bronze.where(F.col("_ingest_timestamp") > F.lit(since))

    with timed("silver.read", log, dataset=ds.name):
        rows_in = bronze.count()
    metrics.rows_read = rows_in

    if rows_in == 0:
        log.info("silver.no_new_data", extra={"dataset": ds.name})
        return metrics.finish("skipped")

    # Conform: cast, clean, derive, dedupe.
    with timed("silver.conform", log, dataset=ds.name, rows_in=rows_in):
        conformed = conform(ds.name, bronze)

    # Quality gate.
    report = None
    valid = conformed
    if ds.quality_suite:
        suite = load_suite(ds.quality_suite)
        with timed("silver.quality", log, dataset=ds.name, rules=len(suite.expectations)):
            valid, quarantined, report = ctx.quality.evaluate(conformed, suite, ds.name, "silver")
            if ds.quarantine_on_fail:
                ctx.quality.quarantine(quarantined, ds.name, "silver")
            ctx.quality.persist(report)
        metrics.rows_quarantined = report.quarantined_rows
        metrics.dq_pass_rate = report.pass_rate
        ctx.alerts.check_quality(ds.name, "silver", report.pass_rate, min_pass_rate=0.95)

    ctx.schemas.enforce(valid, ds.name, "silver", policy=ds.schema_evolution, run_id=run_id())

    if dry_run:
        log.info("silver.dry_run", extra={"dataset": ds.name, "rows": valid.count()})
        return metrics.finish("dry_run")

    writer = DeltaWriter(ctx.spark, auto_optimize=cfg.auto_optimize)
    location = cfg.layer_path("silver", ds.silver_table) if cfg.is_local else None

    if ds.scd == "type2":
        from pyspark.sql import functions as F2

        seed = valid.withColumn("is_current", F2.lit(True)).withColumn(
            "effective_to", F2.lit(None).cast("timestamp")
        )
        writer.create_if_missing(
            seed,
            target,
            partition_by=ds.partition_by,
            location=location,
            comment=f"SCD2 dimension for {ds.name}.",
        )
        with timed("silver.merge_scd2", log, dataset=ds.name):
            result = writer.merge_scd2(
                valid,
                target,
                keys=ds.primary_keys,
                tracked_columns=[c for c in valid.columns if not c.startswith("_")],
            )
    else:
        writer.create_if_missing(
            valid,
            target,
            partition_by=ds.partition_by,
            location=location,
            comment=f"Cleansed, quality-enforced {ds.name}.",
        )
        # Partition pruning: restrict the MERGE to the partitions actually present
        # in this batch instead of scanning the whole target.
        prune = None
        if ds.partition_by:
            prune = {}
            for col in ds.partition_by:
                if col in valid.columns:
                    vals = [
                        str(r[0])
                        for r in valid.select(col).distinct().limit(500).collect()
                        if r[0] is not None
                    ]
                    if vals:
                        prune[col] = vals
        with timed("silver.merge", log, dataset=ds.name):
            result = writer.merge_upsert(
                valid,
                target,
                keys=ds.primary_keys,
                # Only rewrite a row when something actually changed.
                update_condition=(
                    "t.row_hash <> s.row_hash" if "row_hash" in valid.columns else None
                ),
                partition_by=ds.partition_by,
                partition_prune_values=prune,
            )

    max_wm = bronze.agg(F.max("_ingest_timestamp")).collect()[0][0] or datetime.now(timezone.utc)
    ctx.watermarks.advance(ds.name, "silver", max_wm, result.rows_written, run_id())

    ctx.lineage.record(
        LineageEdge(
            dataset=ds.name,
            source_layer="bronze",
            target_layer="silver",
            source_object=source,
            target_object=target,
            operation=result.mode,
            rows_in=rows_in,
            rows_out=result.rows_written,
            metrics={
                "dq_pass_rate": report.pass_rate if report else 1.0,
                "quarantined": metrics.rows_quarantined,
            },
        )
    )

    metrics.rows_written = result.rows_written
    metrics.rows_inserted = result.rows_inserted
    metrics.rows_updated = result.rows_updated
    metrics.table_version = result.version
    metrics.watermark = max_wm
    return metrics.finish("success")


def main(argv: list[str] | None = None) -> int:
    parser = base_parser(__doc__ or "Silver build")
    args = parser.parse_args(argv)

    ctx = build_context("build_silver", args.env)
    try:
        metrics = build(ctx, args.dataset, args.full_refresh, args.dry_run)
        ctx.metrics.record(metrics)
        return 0
    except Exception as exc:
        ctx.metrics.record(
            PipelineMetrics(
                job_name=ctx.job_name, dataset=args.dataset, layer="silver", stage="build"
            ).finish("failed", str(exc))
        )
        log.error(
            "silver.job_failed", extra={"dataset": args.dataset, "error": str(exc)}, exc_info=True
        )
        raise
    finally:
        finalise(ctx)


if __name__ == "__main__":
    sys.exit(main())
