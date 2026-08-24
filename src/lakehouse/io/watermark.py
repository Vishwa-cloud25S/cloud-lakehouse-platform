"""High-water-mark state for incremental processing.

Each (dataset, layer) pair keeps the maximum source `updated_at` successfully
committed. The next run reads strictly greater values, so a re-run after a failure
reprocesses only the tail instead of the whole history.

The state table is itself a Delta table, which means the watermark advances inside
the same transactional story as the data and is time-travellable for audits.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from lakehouse.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    dataset        STRING  NOT NULL,
    layer          STRING  NOT NULL,
    watermark      TIMESTAMP,
    rows_processed BIGINT,
    run_id         STRING,
    updated_at     TIMESTAMP
) USING DELTA
COMMENT 'Incremental high-water marks per dataset and medallion layer.'
"""


@dataclass
class WatermarkStore:
    """Read/advance incremental watermarks held in a Delta control table."""

    spark: SparkSession
    table: str
    location: str | None = None

    def ensure(self) -> None:
        ddl = _WATERMARK_DDL.format(table=self.table)
        if self.location:
            ddl += f"\nLOCATION '{self.location}'"
        self.spark.sql(ddl)

    def get(self, dataset: str, layer: str) -> datetime:
        """Last committed watermark, or epoch for a first run (full backfill)."""
        self.ensure()
        row = (
            self.spark.table(self.table)
            .where(f"dataset = '{dataset}' AND layer = '{layer}'")
            .selectExpr("max(watermark) AS wm")
            .collect()
        )
        wm = row[0]["wm"] if row else None
        if wm is None:
            log.info("watermark.cold_start", extra={"dataset": dataset, "layer": layer})
            return EPOCH
        if wm.tzinfo is None:
            wm = wm.replace(tzinfo=timezone.utc)
        log.info("watermark.loaded", extra={"dataset": dataset, "layer": layer, "watermark": wm})
        return wm

    def advance(
        self, dataset: str, layer: str, watermark: datetime, rows: int, run_id: str
    ) -> None:
        """Commit a new watermark. Never moves backwards (guards late re-runs)."""
        from delta.tables import DeltaTable

        self.ensure()
        current = self.get(dataset, layer)
        if watermark.tzinfo is None:
            watermark = watermark.replace(tzinfo=timezone.utc)
        if watermark < current:
            log.warning(
                "watermark.regression_ignored",
                extra={
                    "dataset": dataset,
                    "layer": layer,
                    "attempted": watermark,
                    "current": current,
                },
            )
            return

        updates: DataFrame = self.spark.createDataFrame(
            [(dataset, layer, watermark, int(rows), run_id, datetime.now(timezone.utc))],
            schema="dataset string, layer string, watermark timestamp, "
            "rows_processed bigint, run_id string, updated_at timestamp",
        )
        (
            DeltaTable.forName(self.spark, self.table)
            .alias("t")
            .merge(updates.alias("s"), "t.dataset = s.dataset AND t.layer = s.layer")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info(
            "watermark.advanced",
            extra={"dataset": dataset, "layer": layer, "watermark": watermark, "rows": rows},
        )

    def history(self, dataset: str | None = None) -> DataFrame:
        df = self.spark.table(self.table)
        return df.where(f"dataset = '{dataset}'") if dataset else df
