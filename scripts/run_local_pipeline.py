#!/usr/bin/env python3
"""Run the full medallion pipeline locally on Delta Lake.

This is the demo entry point and the integration smoke test: it proves the whole
chain - incremental Bronze ingest, quality-enforced Silver, SCD2, the Gold star
schema and maintenance - works end to end on a laptop with no cloud dependency.

    python scripts/run_local_pipeline.py --env local
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lakehouse.jobs import build_gold, build_silver, ingest_bronze, run_maintenance  # noqa: E402
from lakehouse.jobs._common import build_context, finalise  # noqa: E402

DATASETS = ["products", "customers", "orders"]


def banner(text: str) -> None:
    print(f"\n{'=' * 78}\n  {text}\n{'=' * 78}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default="local")
    parser.add_argument("--clean", action="store_true", help="delete the local lake first")
    parser.add_argument("--skip-maintenance", action="store_true")
    parser.add_argument(
        "--skip-vacuum",
        action="store_true",
        help="OPTIMIZE/ANALYZE only; VACUUM needs more driver memory",
    )
    args = parser.parse_args()

    if args.clean:
        for path in ("data/lake", "data/_checkpoints", "spark-warehouse", "metastore_db"):
            shutil.rmtree(path, ignore_errors=True)
        print("cleaned local lake")

    started = time.time()
    summary: list[tuple[str, str, int, int, float]] = []

    banner("BRONZE - incremental ingest")
    ctx = build_context("ingest_bronze", args.env)
    try:
        for ds in DATASETS:
            m = ingest_bronze.ingest(ctx, ds)
            ctx.metrics.record(m)
            summary.append(("bronze", ds, m.rows_read, m.rows_written, m.duration_s))
            print(
                f"  {ds:<10} read={m.rows_read:>6,}  written={m.rows_written:>6,}  "
                f"status={m.status}"
            )
    finally:
        finalise(ctx)

    banner("SILVER - conform, quality gate, dedupe, SCD2")
    ctx = build_context("build_silver", args.env)
    try:
        for ds in DATASETS:
            m = build_silver.build(ctx, ds)
            ctx.metrics.record(m)
            summary.append(("silver", ds, m.rows_read, m.rows_written, m.duration_s))
            print(
                f"  {ds:<10} read={m.rows_read:>6,}  written={m.rows_written:>6,}  "
                f"quarantined={m.rows_quarantined:>4,}  dq_pass={m.dq_pass_rate:.2%}"
            )
    finally:
        finalise(ctx)

    banner("GOLD - star schema + serving aggregates")
    ctx = build_context("build_gold", args.env)
    try:
        for m in build_gold.build(ctx):
            ctx.metrics.record(m)
            summary.append(("gold", m.stage, m.rows_read, m.rows_written, m.duration_s))
            print(f"  {m.stage:<18} rows={m.rows_written:>6,}  ({m.duration_s:.1f}s)")
    finally:
        finalise(ctx)

    if not args.skip_maintenance:
        banner("MAINTENANCE - OPTIMIZE / VACUUM / ANALYZE")
        ctx = build_context("run_maintenance", args.env)
        try:
            results = run_maintenance.run(ctx, skip_vacuum=args.skip_vacuum)
            for m in results:
                ctx.metrics.record(m)
            print(f"  maintained {len(results)} tables")
        finally:
            finalise(ctx)

    banner("RESULT")
    from lakehouse.config import load_env
    from lakehouse.spark_session import get_spark

    spark = get_spark("summary", load_env(args.env))
    for table in (
        "gold.fact_orders",
        "gold.dim_customer",
        "gold.dim_product",
        "gold.agg_daily_sales",
        "gold.agg_customer_360",
        "ops.quarantine",
    ):
        try:
            print(f"  {table:<26} {spark.table(table).count():>8,} rows")
        except Exception:
            print(f"  {table:<26}    (absent)")

    print(f"\n  total elapsed: {time.time() - started:.1f}s")
    print("\nTop 5 revenue days:")
    try:
        spark.sql("""
            SELECT order_date, order_count, net_revenue, gross_profit
            FROM gold.agg_daily_sales ORDER BY net_revenue DESC LIMIT 5
        """).show(truncate=False)
    except Exception as exc:
        print(f"  (query failed: {exc})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
