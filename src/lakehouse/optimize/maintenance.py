"""Scheduled Delta maintenance.

Why each command matters:

OPTIMIZE   Streaming/micro-batch ingestion produces many small files. Every one is
           a separate remote read + task, so query latency degrades linearly with
           file count. OPTIMIZE bin-packs them to ~1 GB.
ZORDER     Co-locates rows sharing a value of the chosen columns into the same files,
           so Delta's min/max file statistics can skip most of the table for a
           selective predicate. Only useful on high-cardinality *filter* columns.
VACUUM     Deletes files no longer referenced by the log. Without it storage grows
           forever. The 7-day default retention protects in-flight readers and time
           travel; lowering it is a deliberate cost/recovery trade-off.
ANALYZE    Refreshes statistics so the cost-based optimizer picks the right join
           order and broadcast candidates.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from lakehouse.logging_utils import get_logger, safe_extra, timed

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger(__name__)


@dataclass
class MaintenanceResult:
    table: str
    operation: str
    files_before: int = 0
    files_after: int = 0
    size_before: int = 0
    size_after: int = 0
    duration_s: float = 0.0
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def files_removed(self) -> int:
        return max(self.files_before - self.files_after, 0)

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "operation": self.operation,
            "files_before": self.files_before,
            "files_after": self.files_after,
            "files_removed": self.files_removed,
            "size_before": self.size_before,
            "size_after": self.size_after,
            "duration_s": self.duration_s,
            **self.detail,
        }


@dataclass
class MaintenanceRunner:
    """Run and record Delta maintenance operations."""

    spark: SparkSession
    dry_run: bool = False

    def table_detail(self, table: str) -> dict[str, Any]:
        try:
            return self.spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].asDict()
        except Exception:  # pragma: no cover
            return {}

    def optimize(
        self, table: str, zorder_by: list[str] | None = None, where: str | None = None
    ) -> MaintenanceResult:
        """Compact small files, optionally Z-ordering and restricting to partitions."""
        import time

        before = self.table_detail(table)
        sql = f"OPTIMIZE {table}"
        if where:
            # Restricting OPTIMIZE to recent partitions keeps the job O(new data)
            # instead of O(table) - essential once the fact table is large.
            sql += f" WHERE {where}"
        if zorder_by:
            sql += f" ZORDER BY ({', '.join(zorder_by)})"

        if self.dry_run:
            log.info("maintenance.dry_run", extra={"sql": sql})
            return MaintenanceResult(table=table, operation="optimize", detail={"sql": sql})

        start = time.perf_counter()
        with timed("optimize", log, table=table, zorder=zorder_by):
            try:
                self.spark.sql(sql)
            except Exception as exc:  # noqa: BLE001 - OSS Delta lacks ZORDER
                log.warning(
                    "maintenance.optimize_unsupported",
                    extra={"table": table, "reason": str(exc).split("\n", 1)[0][:180]},
                )
                return MaintenanceResult(
                    table=table, operation="optimize", detail={"skipped": True}
                )
        duration = time.perf_counter() - start
        after = self.table_detail(table)
        result = MaintenanceResult(
            table=table,
            operation="optimize",
            files_before=int(before.get("numFiles") or 0),
            files_after=int(after.get("numFiles") or 0),
            size_before=int(before.get("sizeInBytes") or 0),
            size_after=int(after.get("sizeInBytes") or 0),
            duration_s=round(duration, 3),
            detail={"zorder_by": zorder_by, "where": where},
        )
        log.info("maintenance.optimized", extra=safe_extra(result.as_dict()))
        return result

    def vacuum(
        self, table: str, retention_hours: int = 168, retention_check: bool = True
    ) -> MaintenanceResult:
        """Purge unreferenced files older than the retention window."""
        before = self.table_detail(table)
        if self.dry_run:
            self.spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS DRY RUN")
            return MaintenanceResult(table=table, operation="vacuum", detail={"dry_run": True})

        if not retention_check and retention_hours < 168:
            # Only ever disabled in tests; in prod this guard prevents deleting files
            # an in-flight reader still needs.
            self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        try:
            self.spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "maintenance.vacuum_failed",
                extra={"table": table, "reason": str(exc).split("\n", 1)[0][:180]},
            )
            return MaintenanceResult(table=table, operation="vacuum", detail={"failed": True})
        finally:
            # Restoring the guard must never mask the original failure (the session
            # may already be gone if the JVM died mid-VACUUM).
            with contextlib.suppress(Exception):
                self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

        after = self.table_detail(table)
        result = MaintenanceResult(
            table=table,
            operation="vacuum",
            files_before=int(before.get("numFiles") or 0),
            files_after=int(after.get("numFiles") or 0),
            size_before=int(before.get("sizeInBytes") or 0),
            size_after=int(after.get("sizeInBytes") or 0),
            detail={"retention_hours": retention_hours},
        )
        log.info("maintenance.vacuumed", extra=safe_extra(result.as_dict()))
        return result

    def analyze(self, table: str, columns: list[str] | None = None) -> MaintenanceResult:
        """Collect statistics for the cost-based optimizer."""
        target = f"FOR COLUMNS {', '.join(columns)}" if columns else "FOR ALL COLUMNS"
        try:
            self.spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS {target}")
            log.info("maintenance.analyzed", extra={"table": table, "columns": columns})
            return MaintenanceResult(
                table=table, operation="analyze", detail={"columns": columns or "all"}
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "maintenance.analyze_failed",
                extra={"table": table, "reason": str(exc).split("\n", 1)[0][:180]},
            )
            return MaintenanceResult(table=table, operation="analyze", detail={"failed": True})

    def full_maintenance(
        self,
        table: str,
        zorder_by: list[str] | None = None,
        retention_hours: int = 168,
        retention_check: bool = True,
        optimize_where: str | None = None,
        analyze_columns: list[str] | None = None,
        skip_vacuum: bool = False,
    ) -> list[MaintenanceResult]:
        """OPTIMIZE (+ZORDER) -> VACUUM -> ANALYZE, in that order.

        Each step is independent: maintenance is best-effort housekeeping and must
        never fail the pipeline, so one table's problem cannot stop the rest.
        """
        results = [self.optimize(table, zorder_by=zorder_by, where=optimize_where)]
        if not skip_vacuum:
            results.append(
                self.vacuum(table, retention_hours=retention_hours, retention_check=retention_check)
            )
        results.append(self.analyze(table, columns=analyze_columns))
        return results
