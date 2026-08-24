"""Lineage capture.

Unity Catalog captures table/column lineage automatically from query plans and
exposes it through `system.access.table_lineage`. That covers *what* happened
inside the SQL engine, but not *why* a job ran or which config produced it, so
this tracker records a job-level edge list (inputs -> outputs, with the run's
metrics) into a Delta table. Together they answer both questions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from lakehouse.logging_utils import get_logger, run_id

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

_LINEAGE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id        STRING,
    job_name      STRING,
    dataset       STRING,
    source_layer  STRING,
    target_layer  STRING,
    source_object STRING,
    target_object STRING,
    operation     STRING,
    rows_in       BIGINT,
    rows_out      BIGINT,
    metrics_json  STRING,
    recorded_at   TIMESTAMP
) USING DELTA
COMMENT 'Job-level lineage edges emitted by every pipeline run.'
"""


@dataclass
class LineageEdge:
    dataset: str
    source_layer: str
    target_layer: str
    source_object: str
    target_object: str
    operation: str
    rows_in: int = 0
    rows_out: int = 0
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageTracker:
    """Collect edges during a run and flush them in one write."""

    spark: SparkSession
    table: str
    job_name: str
    edges: list[LineageEdge] = field(default_factory=list)

    def record(self, edge: LineageEdge) -> None:
        self.edges.append(edge)
        log.info(
            "lineage.edge",
            extra={
                "source": edge.source_object,
                "target": edge.target_object,
                "op": edge.operation,
                "rows_out": edge.rows_out,
            },
        )

    def flush(self) -> int:
        """Persist collected edges. Returns the number written."""
        import json

        if not self.edges:
            return 0
        self.spark.sql(_LINEAGE_DDL.format(table=self.table))
        now = datetime.now(timezone.utc)
        rows = [
            (
                run_id(),
                self.job_name,
                e.dataset,
                e.source_layer,
                e.target_layer,
                e.source_object,
                e.target_object,
                e.operation,
                int(e.rows_in),
                int(e.rows_out),
                json.dumps(e.metrics, default=str),
                now,
            )
            for e in self.edges
        ]
        df: DataFrame = self.spark.createDataFrame(
            rows,
            schema="run_id string, job_name string, dataset string, source_layer string, "
            "target_layer string, source_object string, target_object string, "
            "operation string, rows_in bigint, rows_out bigint, metrics_json string, "
            "recorded_at timestamp",
        )
        df.write.format("delta").mode("append").saveAsTable(self.table)
        count = len(rows)
        log.info("lineage.flushed", extra={"table": self.table, "edges": count})
        self.edges.clear()
        return count

    def graph(self) -> DataFrame:
        """Distinct object-to-object edges - the input for a lineage diagram."""
        return (
            self.spark.table(self.table)
            .select("source_object", "target_object", "operation")
            .distinct()
        )
