"""Pipeline metrics.

Every run appends one row per stage to `ops.pipeline_metrics`. Because it is a Delta
table it is queryable from Databricks SQL, so the operational dashboard is built with
the same engine as the business dashboards - no separate metrics stack to run.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from lakehouse.logging_utils import get_logger, run_id, safe_extra

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id          STRING,
    job_name        STRING,
    dataset         STRING,
    layer           STRING,
    stage           STRING,
    status          STRING,
    rows_read       BIGINT,
    rows_written    BIGINT,
    rows_inserted   BIGINT,
    rows_updated    BIGINT,
    rows_quarantined BIGINT,
    dq_pass_rate    DOUBLE,
    duration_s      DOUBLE,
    watermark       TIMESTAMP,
    table_version   BIGINT,
    error_message   STRING,
    env             STRING,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP
) USING DELTA
PARTITIONED BY (job_name)
COMMENT 'One row per pipeline stage execution; powers the ops dashboard and alerts.'
"""


@dataclass
class PipelineMetrics:
    """Metrics for a single stage execution."""

    job_name: str
    dataset: str = ""
    layer: str = ""
    stage: str = ""
    status: str = "success"
    rows_read: int = 0
    rows_written: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_quarantined: int = 0
    dq_pass_rate: float = 1.0
    duration_s: float = 0.0
    watermark: datetime | None = None
    table_version: int | None = None
    error_message: str = ""
    env: str = "dev"
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None

    def finish(self, status: str = "success", error: str = "") -> PipelineMetrics:
        self.finished_at = datetime.now(timezone.utc)
        self.duration_s = round((self.finished_at - self.started_at).total_seconds(), 3)
        self.status = status
        self.error_message = error[:2000]
        return self

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MetricsCollector:
    """Buffer metrics during a run, then write them in a single Delta commit."""

    spark: SparkSession
    table: str
    env: str = "dev"
    buffer: list[PipelineMetrics] = field(default_factory=list)

    def record(self, metrics: PipelineMetrics) -> None:
        metrics.env = self.env
        if metrics.finished_at is None:
            metrics.finish()
        self.buffer.append(metrics)
        log.info(
            "metrics.recorded",
            extra=safe_extra(
                {k: v for k, v in metrics.as_dict().items() if v not in (None, "", 0)}
            ),
        )

    def flush(self) -> int:
        if not self.buffer:
            return 0
        self.spark.sql(_METRICS_DDL.format(table=self.table))
        rows = [
            (
                run_id(),
                m.job_name,
                m.dataset,
                m.layer,
                m.stage,
                m.status,
                int(m.rows_read),
                int(m.rows_written),
                int(m.rows_inserted),
                int(m.rows_updated),
                int(m.rows_quarantined),
                float(m.dq_pass_rate),
                float(m.duration_s),
                m.watermark,
                int(m.table_version) if m.table_version is not None else None,
                m.error_message,
                m.env,
                m.started_at,
                m.finished_at,
            )
            for m in self.buffer
        ]
        df: DataFrame = self.spark.createDataFrame(
            rows,
            schema="run_id string, job_name string, dataset string, layer string, stage string, "
            "status string, rows_read bigint, rows_written bigint, rows_inserted bigint, "
            "rows_updated bigint, rows_quarantined bigint, dq_pass_rate double, "
            "duration_s double, watermark timestamp, table_version bigint, "
            "error_message string, env string, started_at timestamp, finished_at timestamp",
        )
        df.write.format("delta").mode("append").saveAsTable(self.table)
        count = len(rows)
        self.buffer.clear()
        log.info("metrics.flushed", extra={"table": self.table, "rows": count})
        return count
