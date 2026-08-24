"""Partitioning advisor.

Bad partitioning is the most common and most expensive Delta mistake. The rules
encoded here are the ones that actually matter in practice:

* A partition should hold **at least ~1 GB**. Smaller and you pay more in file
  listing + task scheduling than you save in scanning.
* Partition **count** should stay in the low thousands. 50k tiny partitions makes
  every query's planning phase slower than the scan it avoids.
* Never partition on a high-cardinality column (id, timestamp, email). Use Z-ORDER
  or liquid clustering for those - they give data skipping without directory explosion.
* Small tables (< ~1 GB) should not be partitioned at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from lakehouse.logging_utils import get_logger, safe_extra

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

TARGET_PARTITION_BYTES = 1024**3  # 1 GiB
MIN_TABLE_BYTES_TO_PARTITION = 1024**3  # below this, don't partition
MAX_REASONABLE_PARTITIONS = 10_000
MAX_PARTITION_CARDINALITY_RATIO = 0.05  # distinct/rows above this => too granular


@dataclass
class PartitionRecommendation:
    column: str | None
    reason: str
    severity: str = "info"  # info | warn | critical
    metrics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "reason": self.reason,
            "severity": self.severity,
            **self.metrics,
        }


def recommend_partitions(
    row_count: int,
    size_bytes: int,
    candidates: dict[str, int],
) -> list[PartitionRecommendation]:
    """Pure function: given row count, size and {column: distinct_count}, advise.

    Kept dependency-free so it is unit-testable without Spark.
    """
    recs: list[PartitionRecommendation] = []

    if size_bytes < MIN_TABLE_BYTES_TO_PARTITION:
        recs.append(
            PartitionRecommendation(
                None,
                f"Table is {size_bytes / 1024**2:.1f} MiB (< 1 GiB): do not partition. "
                "Use Z-ORDER / liquid clustering for skipping instead.",
                "warn",
                {"size_bytes": size_bytes, "row_count": row_count},
            )
        )
        return recs

    for column, distinct in sorted(candidates.items()):
        if distinct <= 0:
            continue
        avg_bytes = size_bytes / distinct
        ratio = distinct / row_count if row_count else 1.0

        if distinct > MAX_REASONABLE_PARTITIONS:
            recs.append(
                PartitionRecommendation(
                    column,
                    f"{distinct:,} distinct values exceeds {MAX_REASONABLE_PARTITIONS:,}: "
                    "directory explosion, slow planning. Prefer a coarser grain (month) or Z-ORDER.",
                    "critical",
                    {"distinct": distinct, "avg_partition_bytes": int(avg_bytes)},
                )
            )
        elif ratio > MAX_PARTITION_CARDINALITY_RATIO:
            recs.append(
                PartitionRecommendation(
                    column,
                    f"Cardinality ratio {ratio:.3f} is too high (near-unique): "
                    "this is a Z-ORDER column, not a partition column.",
                    "critical",
                    {"distinct": distinct, "ratio": round(ratio, 4)},
                )
            )
        elif avg_bytes < TARGET_PARTITION_BYTES / 8:
            recs.append(
                PartitionRecommendation(
                    column,
                    f"Average partition would be {avg_bytes / 1024**2:.1f} MiB (target ~1 GiB): "
                    "too small; small-file overhead will dominate.",
                    "warn",
                    {"distinct": distinct, "avg_partition_bytes": int(avg_bytes)},
                )
            )
        else:
            recs.append(
                PartitionRecommendation(
                    column,
                    f"Good candidate: {distinct:,} partitions averaging "
                    f"{avg_bytes / 1024**3:.2f} GiB.",
                    "info",
                    {"distinct": distinct, "avg_partition_bytes": int(avg_bytes)},
                )
            )
    return recs


@dataclass
class PartitionAdvisor:
    """Spark-backed wrapper that measures a table and applies the rules above."""

    spark: SparkSession

    def analyse(self, table: str, candidate_columns: list[str]) -> list[PartitionRecommendation]:
        detail = self.spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].asDict()
        size_bytes = int(detail.get("sizeInBytes") or 0)
        df: DataFrame = self.spark.table(table)
        row_count = df.count()

        from pyspark.sql import functions as F

        if not candidate_columns:
            return recommend_partitions(row_count, size_bytes, {})
        counts = (
            df.agg(*[F.approx_count_distinct(c).alias(c) for c in candidate_columns])
            .collect()[0]
            .asDict()
        )
        recs = recommend_partitions(row_count, size_bytes, {k: int(v) for k, v in counts.items()})
        for rec in recs:
            log.info(
                "partitioning.recommendation", extra=safe_extra({"table": table, **rec.as_dict()})
            )
        return recs

    def skew_report(self, table: str, partition_column: str, top_n: int = 10) -> DataFrame:
        """Row counts per partition - reveals the hot partition behind a straggler task."""
        from pyspark.sql import functions as F

        return (
            self.spark.table(table)
            .groupBy(partition_column)
            .agg(F.count(F.lit(1)).alias("row_count"))
            .orderBy(F.desc("row_count"))
            .limit(top_n)
        )
