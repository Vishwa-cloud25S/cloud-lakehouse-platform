"""Scheduled maintenance: OPTIMIZE + ZORDER, VACUUM, ANALYZE, freshness checks.

Runs on its own schedule (nightly), separate from the ingest DAG, because
maintenance should never block a data SLA.

    python -m lakehouse.jobs.run_maintenance --env dev
"""

from __future__ import annotations

import sys

from lakehouse.config import load_datasets
from lakehouse.jobs._common import JobContext, base_parser, build_context, finalise
from lakehouse.logging_utils import get_logger
from lakehouse.monitoring.metrics import PipelineMetrics
from lakehouse.optimize.maintenance import MaintenanceRunner

log = get_logger(__name__)


def run(ctx: JobContext, dry_run: bool = False, skip_vacuum: bool = False) -> list[PipelineMetrics]:
    cfg = ctx.cfg
    p = "" if cfg.is_local else f"{cfg.catalog}."
    runner = MaintenanceRunner(ctx.spark, dry_run=dry_run)
    collected: list[PipelineMetrics] = []

    for name, ds in load_datasets().items():
        for layer, table_name, zorder in (
            ("bronze", ds.bronze_table, []),
            ("silver", ds.silver_table, ds.zorder_by),
        ):
            table = f"{p}{cfg.schema(layer)}.{table_name}"
            if not ctx.spark.catalog.tableExists(table):
                continue
            m = PipelineMetrics(
                job_name=ctx.job_name, dataset=name, layer=layer, stage="maintenance"
            )
            try:
                results = runner.full_maintenance(
                    table,
                    zorder_by=zorder or None,
                    retention_hours=cfg.vacuum_retention_hours,
                    retention_check=cfg.delta_retention_check,
                    skip_vacuum=skip_vacuum,
                )
                m.rows_written = sum(r.files_removed for r in results)
                collected.append(m.finish())
            except Exception as exc:  # noqa: BLE001 - housekeeping is best-effort
                log.warning(
                    "maintenance.table_failed", extra={"table": table, "reason": str(exc)[:200]}
                )
                collected.append(m.finish("failed", str(exc)))

            # Freshness SLA per dataset contract.
            if layer == "silver" and "updated_at" in ctx.spark.table(table).columns:
                ctx.alerts.check_freshness(table, "updated_at", ds.expected_freshness_hours, name)

    for gold_table, zorder in (
        ("fact_orders", ["customer_key", "product_key"]),
        ("dim_customer", ["customer_id"]),
        ("dim_product", ["product_id"]),
        ("agg_daily_sales", ["order_date"]),
        ("agg_customer_360", ["customer_key"]),
    ):
        table = f"{p}{cfg.schema('gold')}.{gold_table}"
        if not ctx.spark.catalog.tableExists(table):
            continue
        m = PipelineMetrics(job_name=ctx.job_name, layer="gold", stage=f"maintenance_{gold_table}")
        try:
            results = runner.full_maintenance(
                table,
                zorder_by=zorder,
                retention_hours=cfg.vacuum_retention_hours,
                retention_check=cfg.delta_retention_check,
                skip_vacuum=skip_vacuum,
            )
            m.rows_written = sum(r.files_removed for r in results)
            collected.append(m.finish())
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "maintenance.table_failed", extra={"table": table, "reason": str(exc)[:200]}
            )
            collected.append(m.finish("failed", str(exc)))

    log.info(
        "maintenance.complete",
        extra={"tables": len(collected), "alerts": ctx.alerts.summary()["total"]},
    )
    return collected


def main(argv: list[str] | None = None) -> int:
    parser = base_parser(__doc__ or "Delta maintenance")
    parser.add_argument(
        "--skip-vacuum",
        action="store_true",
        help="run OPTIMIZE/ANALYZE only (useful on memory-constrained runners)",
    )
    args = parser.parse_args(argv)
    ctx = build_context("run_maintenance", args.env)
    try:
        for m in run(ctx, args.dry_run, skip_vacuum=getattr(args, "skip_vacuum", False)):
            ctx.metrics.record(m)
        return 1 if ctx.alerts.has_critical else 0
    finally:
        finalise(ctx)


if __name__ == "__main__":
    sys.exit(main())
