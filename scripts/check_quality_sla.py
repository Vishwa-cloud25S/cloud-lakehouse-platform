#!/usr/bin/env python3
"""Query the ops tables through Databricks SQL and fail if an SLA is breached.

Used by the scheduled GitHub Actions workflow so a silent data regression becomes
a red build instead of a surprise in a Monday dashboard.

    python scripts/check_quality_sla.py --env prod --min-pass-rate 0.95
"""

from __future__ import annotations

import argparse
import os
import sys

QUALITY_SQL = """
SELECT dataset, layer,
       sum(failed_rows) AS failed_rows,
       sum(total_rows)  AS total_rows,
       1 - (sum(failed_rows) / nullif(sum(total_rows), 0)) AS pass_rate
FROM {catalog}.ops.dq_results
WHERE checked_at >= current_timestamp() - INTERVAL 24 HOURS
  AND action IN ('fail', 'quarantine')
GROUP BY dataset, layer
"""

FRESHNESS_SQL = """
SELECT dataset, layer, max(finished_at) AS last_run,
       timestampdiff(HOUR, max(finished_at), current_timestamp()) AS hours_since
FROM {catalog}.ops.pipeline_metrics
WHERE status = 'success'
GROUP BY dataset, layer
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default="prod")
    parser.add_argument("--min-pass-rate", type=float, default=0.95)
    parser.add_argument("--max-age-hours", type=int, default=26)
    args = parser.parse_args()

    try:
        from databricks import sql as dbsql
    except ImportError:
        print("databricks-sql-connector is not installed", file=sys.stderr)
        return 2

    host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    warehouse = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not all([host, token, warehouse]):
        print(
            "missing DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_WAREHOUSE_ID", file=sys.stderr
        )
        return 2

    catalog = f"lakehouse_{args.env}"
    failures: list[str] = []

    with (
        dbsql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse}",
            access_token=token,
        ) as conn,
        conn.cursor() as cur,
    ):
        print("== Data quality (last 24h) ==")
        cur.execute(QUALITY_SQL.format(catalog=catalog))
        for row in cur.fetchall():
            dataset, layer, failed, total, rate = row
            rate = float(rate or 1.0)
            status = "OK " if rate >= args.min_pass_rate else "FAIL"
            print(
                f"  [{status}] {dataset}/{layer}: {rate:.2%} " f"({failed:,} failed of {total:,})"
            )
            if rate < args.min_pass_rate:
                failures.append(f"{dataset}/{layer} pass rate {rate:.2%}")

        print("\n== Freshness ==")
        cur.execute(FRESHNESS_SQL.format(catalog=catalog))
        for row in cur.fetchall():
            dataset, layer, last_run, hours = row
            hours = int(hours or 0)
            status = "OK " if hours <= args.max_age_hours else "STALE"
            print(f"  [{status}] {dataset}/{layer}: last success {hours}h ago")
            if hours > args.max_age_hours:
                failures.append(f"{dataset}/{layer} stale by {hours}h")

    if failures:
        print("\nSLA BREACHES:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1

    print("\nAll SLAs met.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
