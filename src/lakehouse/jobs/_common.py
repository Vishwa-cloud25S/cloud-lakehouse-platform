"""Shared job scaffolding: arg parsing and the per-run service context."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.config import EnvConfig, load_env
from lakehouse.governance.catalog import CatalogManager
from lakehouse.governance.lineage import LineageTracker
from lakehouse.io.schema_registry import SchemaRegistry
from lakehouse.io.watermark import WatermarkStore
from lakehouse.logging_utils import get_logger
from lakehouse.monitoring.alerts import AlertManager
from lakehouse.monitoring.metrics import MetricsCollector
from lakehouse.quality.runner import DataQualityRunner
from lakehouse.spark_session import get_spark

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger(__name__)


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--env", default="dev", help="environment profile (conf/<env>.yaml)")
    parser.add_argument("--dataset", default="orders", help="dataset name from conf/datasets.yaml")
    parser.add_argument(
        "--full-refresh", action="store_true", help="ignore the watermark and reprocess everything"
    )
    parser.add_argument("--dry-run", action="store_true", help="plan only, do not write")
    return parser


@dataclass
class JobContext:
    """Everything a job needs, wired consistently for every entry point."""

    spark: SparkSession
    cfg: EnvConfig
    job_name: str
    ops_schema: str
    watermarks: WatermarkStore
    schemas: SchemaRegistry
    quality: DataQualityRunner
    metrics: MetricsCollector
    alerts: AlertManager
    lineage: LineageTracker
    catalog: CatalogManager

    @property
    def env(self) -> str:
        return self.cfg.env


def build_context(job_name: str, env: str) -> JobContext:
    """Create the Spark session, ensure schemas exist and wire the services."""
    cfg = load_env(env)
    spark = get_spark(app_name=f"lakehouse-{job_name}-{cfg.env}", cfg=cfg)

    catalog = CatalogManager(spark, cfg)
    catalog.create_catalog()
    catalog.create_schemas()

    prefix = f"{cfg.catalog}." if not cfg.is_local else ""
    ops = f"{prefix}{cfg.schemas.get('ops', 'ops')}"

    ctx = JobContext(
        spark=spark,
        cfg=cfg,
        job_name=job_name,
        ops_schema=ops,
        watermarks=WatermarkStore(spark, f"{ops}.watermarks"),
        schemas=SchemaRegistry(spark, f"{ops}.schema_registry"),
        quality=DataQualityRunner(
            spark,
            results_table=f"{ops}.dq_results",
            quarantine_table=f"{ops}.quarantine",
        ),
        metrics=MetricsCollector(spark, f"{ops}.pipeline_metrics", env=cfg.env),
        alerts=AlertManager(
            spark, metrics_table=f"{ops}.pipeline_metrics", emails=cfg.alert_emails
        ),
        lineage=LineageTracker(spark, f"{ops}.lineage", job_name=job_name),
        catalog=catalog,
    )
    log.info("job.context_ready", extra={"job": job_name, "env": cfg.env, "catalog": cfg.catalog})
    return ctx


def finalise(ctx: JobContext) -> None:
    """Flush observability state. Always called, even on failure."""
    try:
        ctx.metrics.flush()
        ctx.lineage.flush()
    except Exception as exc:  # noqa: BLE001 - never mask the original error
        log.warning("job.finalise_failed", extra={"reason": str(exc)[:300]})
