"""Gold build job: dimensional model + serving aggregates.

python -m lakehouse.jobs.build_gold --env dev
"""

from __future__ import annotations

import sys

from lakehouse.governance.lineage import LineageEdge
from lakehouse.io.delta_writer import DeltaWriter
from lakehouse.jobs._common import JobContext, base_parser, build_context, finalise
from lakehouse.logging_utils import get_logger, timed
from lakehouse.monitoring.metrics import PipelineMetrics
from lakehouse.transforms.gold import (
    build_agg_customer_360,
    build_agg_daily_sales,
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_fact_orders,
)

log = get_logger(__name__)


def build(ctx: JobContext, dry_run: bool = False) -> list[PipelineMetrics]:
    cfg = ctx.cfg
    p = "" if cfg.is_local else f"{cfg.catalog}."
    s_orders = f"{p}{cfg.schema('silver')}.silver_orders"
    s_customers = f"{p}{cfg.schema('silver')}.silver_customers"
    s_products = f"{p}{cfg.schema('silver')}.silver_products"
    gold = f"{p}{cfg.schema('gold')}"

    writer = DeltaWriter(ctx.spark, auto_optimize=cfg.auto_optimize)
    collected: list[PipelineMetrics] = []

    def _loc(table: str) -> str | None:
        return cfg.layer_path("gold", table) if cfg.is_local else None

    for required in (s_orders, s_customers, s_products):
        if not ctx.spark.catalog.tableExists(required):
            log.warning("gold.source_missing", extra={"table": required})
            return collected

    # ---------------------------------------------------------- dimensions
    m = PipelineMetrics(
        job_name=ctx.job_name, layer="gold", stage="dim_customer", dataset="customers"
    )
    with timed("gold.dim_customer", log):
        dim_customer = build_dim_customer(ctx.spark.table(s_customers))
        if not dry_run:
            writer.create_if_missing(
                dim_customer,
                f"{gold}.dim_customer",
                location=_loc("dim_customer"),
                comment="SCD2 customer dimension (point-in-time correct).",
            )
            r = writer.merge_upsert(
                dim_customer, f"{gold}.dim_customer", keys=["customer_version_key"]
            )
            m.rows_written = r.rows_written
            m.table_version = r.version
    collected.append(m.finish())

    m = PipelineMetrics(
        job_name=ctx.job_name, layer="gold", stage="dim_product", dataset="products"
    )
    with timed("gold.dim_product", log):
        dim_product = build_dim_product(ctx.spark.table(s_products))
        if not dry_run:
            writer.create_if_missing(
                dim_product,
                f"{gold}.dim_product",
                location=_loc("dim_product"),
                comment="Type-1 product dimension.",
            )
            r = writer.merge_upsert(dim_product, f"{gold}.dim_product", keys=["product_key"])
            m.rows_written = r.rows_written
            m.table_version = r.version
    collected.append(m.finish())

    m = PipelineMetrics(job_name=ctx.job_name, layer="gold", stage="dim_date")
    with timed("gold.dim_date", log):
        if not dry_run and not ctx.spark.catalog.tableExists(f"{gold}.dim_date"):
            dim_date = build_dim_date(ctx.spark)
            writer.create_if_missing(
                dim_date,
                f"{gold}.dim_date",
                location=_loc("dim_date"),
                comment="Generated calendar dimension.",
            )
            r = writer.append(dim_date, f"{gold}.dim_date")
            m.rows_written = r.rows_written
    collected.append(m.finish())

    # --------------------------------------------------------------- fact
    m = PipelineMetrics(job_name=ctx.job_name, layer="gold", stage="fact_orders", dataset="orders")
    with timed("gold.fact_orders", log):
        fact = build_fact_orders(
            ctx.spark.table(s_orders),
            ctx.spark.table(f"{gold}.dim_customer") if not dry_run else dim_customer,
            ctx.spark.table(f"{gold}.dim_product") if not dry_run else dim_product,
        )
        m.rows_read = fact.count()
        if not dry_run:
            writer.create_if_missing(
                fact,
                f"{gold}.fact_orders",
                partition_by=["order_month"],
                location=_loc("fact_orders"),
                comment="Order-line fact joined to point-in-time dimension versions.",
            )
            months = [str(r[0]) for r in fact.select("order_month").distinct().collect() if r[0]]
            r = writer.merge_upsert(
                fact,
                f"{gold}.fact_orders",
                keys=["order_id"],
                partition_by=["order_month"],
                partition_prune_values={"order_month": months},
            )
            m.rows_written = r.rows_written
            m.rows_inserted = r.rows_inserted
            m.rows_updated = r.rows_updated
            m.table_version = r.version
            ctx.lineage.record(
                LineageEdge(
                    dataset="orders",
                    source_layer="silver",
                    target_layer="gold",
                    source_object=s_orders,
                    target_object=f"{gold}.fact_orders",
                    operation="merge",
                    rows_in=m.rows_read,
                    rows_out=r.rows_written,
                )
            )
    collected.append(m.finish())

    # -------------------------------------------------------- aggregates
    m = PipelineMetrics(job_name=ctx.job_name, layer="gold", stage="agg_daily_sales")
    with timed("gold.agg_daily_sales", log):
        source_fact = ctx.spark.table(f"{gold}.fact_orders") if not dry_run else fact
        agg = build_agg_daily_sales(source_fact)
        if not dry_run:
            writer.create_if_missing(
                agg,
                f"{gold}.agg_daily_sales",
                partition_by=["order_month"],
                location=_loc("agg_daily_sales"),
                comment="Daily sales KPIs for Power BI.",
            )
            r = writer.overwrite_partitions(
                agg, f"{gold}.agg_daily_sales", partition_by=["order_month"]
            )
            m.rows_written = r.rows_written
            m.table_version = r.version
    collected.append(m.finish())

    m = PipelineMetrics(job_name=ctx.job_name, layer="gold", stage="agg_customer_360")
    with timed("gold.agg_customer_360", log):
        if not dry_run:
            c360 = build_agg_customer_360(
                ctx.spark.table(f"{gold}.fact_orders"), ctx.spark.table(f"{gold}.dim_customer")
            )
            writer.create_if_missing(
                c360,
                f"{gold}.agg_customer_360",
                location=_loc("agg_customer_360"),
                comment="Customer lifetime value, RFM status and tier.",
            )
            r = writer.merge_upsert(c360, f"{gold}.agg_customer_360", keys=["customer_key"])
            m.rows_written = r.rows_written
            m.table_version = r.version
    collected.append(m.finish())

    return collected


def main(argv: list[str] | None = None) -> int:
    parser = base_parser(__doc__ or "Gold build")
    args = parser.parse_args(argv)

    ctx = build_context("build_gold", args.env)
    try:
        for m in build(ctx, args.dry_run):
            ctx.metrics.record(m)
        return 0
    except Exception as exc:
        ctx.metrics.record(
            PipelineMetrics(job_name=ctx.job_name, layer="gold", stage="build").finish(
                "failed", str(exc)
            )
        )
        log.error("gold.job_failed", extra={"error": str(exc)}, exc_info=True)
        raise
    finally:
        finalise(ctx)


if __name__ == "__main__":
    sys.exit(main())
