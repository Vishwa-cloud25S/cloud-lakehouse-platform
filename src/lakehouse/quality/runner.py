"""Execute an ExpectationSuite against a DataFrame.

Design choices worth calling out:

* All rules are evaluated in **one pass**. Each expectation becomes a boolean
  column, then a single `agg(sum(...))` collects every failure count - so 20 rules
  cost one scan, not 20.
* Failing rows are *quarantined*, not dropped. They land in a Delta table with the
  rule that rejected them, the run id and the raw payload, which makes "why is
  yesterday's revenue short?" a query rather than an investigation.
* Results are written to a `dq_results` Delta table that powers the monitoring
  dashboard and the freshness/quality alerts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from lakehouse.logging_utils import get_logger, run_id, safe_extra
from lakehouse.quality.expectations import Action, ExpectationSuite

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)


class DataQualityError(RuntimeError):
    """Raised when a blocking (`fail`) expectation is violated beyond tolerance."""


@dataclass
class RuleResult:
    name: str
    action: str
    failed_rows: int
    total_rows: int
    tolerance: float

    @property
    def failure_rate(self) -> float:
        return (self.failed_rows / self.total_rows) if self.total_rows else 0.0

    @property
    def passed(self) -> bool:
        if self.action == "skipped":
            return True
        return self.failure_rate <= self.tolerance

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule": self.name,
            "action": self.action,
            "failed_rows": self.failed_rows,
            "total_rows": self.total_rows,
            "failure_rate": round(self.failure_rate, 6),
            "tolerance": self.tolerance,
            "passed": self.passed,
        }


@dataclass
class QualityReport:
    """Outcome of one suite execution."""

    dataset: str
    layer: str
    total_rows: int
    valid_rows: int
    quarantined_rows: int
    results: list[RuleResult] = field(default_factory=list)
    run_id: str = ""
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def pass_rate(self) -> float:
        return (self.valid_rows / self.total_rows) if self.total_rows else 1.0

    def failures(self) -> list[RuleResult]:
        return [r for r in self.results if not r.passed]

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "layer": self.layer,
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "quarantined_rows": self.quarantined_rows,
            "pass_rate": round(self.pass_rate, 6),
            "passed": self.passed,
            "run_id": self.run_id,
            "rules": [r.as_dict() for r in self.results],
        }


_DQ_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    dataset      STRING,
    layer        STRING,
    rule         STRING,
    action       STRING,
    total_rows   BIGINT,
    failed_rows  BIGINT,
    failure_rate DOUBLE,
    passed       BOOLEAN,
    run_id       STRING,
    checked_at   TIMESTAMP
) USING DELTA
PARTITIONED BY (dataset)
COMMENT 'Per-rule data quality outcomes for every pipeline run.'
"""


class DataQualityRunner:
    """Evaluate suites, split valid/quarantine, persist metrics."""

    def __init__(
        self,
        spark: SparkSession,
        results_table: str | None = None,
        quarantine_table: str | None = None,
    ) -> None:
        self.spark = spark
        self.results_table = results_table
        self.quarantine_table = quarantine_table

    def evaluate(
        self, df: DataFrame, suite: ExpectationSuite, dataset: str, layer: str = "silver"
    ) -> tuple[DataFrame, DataFrame, QualityReport]:
        """Return (valid_df, quarantined_df, report). Single scan over `df`."""
        from pyspark.sql import functions as F

        if not suite.expectations:
            empty = df.limit(0).withColumn("_dq_failed_rules", F.array())
            return df, empty, QualityReport(dataset, layer, df.count(), df.count(), 0, [], run_id())

        # Resolve rules against the actual schema first. A suite must never crash a
        # pipeline because an optional column (e.g. _corrupt_record, which Spark only
        # materialises on a parse failure) is absent from this batch - such rules are
        # reported as "skipped" so the gap is visible in the DQ dashboard.
        applicable = []
        skipped: list[RuleResult] = []
        for exp in suite.expectations:
            try:
                # Force analysis of the predicate; an unresolvable column raises here.
                _ = df.selectExpr(f"({exp.expr}) AS _probe").schema
                applicable.append(exp)
            except Exception as exc:  # noqa: BLE001 - unresolvable rule, not a data error
                log.warning(
                    "dq.rule_skipped",
                    extra={
                        "dataset": dataset,
                        "rule": exp.name,
                        "reason": str(exc).split("\n", 1)[0][:160],
                    },
                )
                skipped.append(
                    RuleResult(
                        name=exp.name,
                        action="skipped",
                        failed_rows=0,
                        total_rows=0,
                        tolerance=exp.tolerance,
                    )
                )

        if not applicable:
            total = df.count()
            return (
                df,
                df.limit(0).withColumn("_dq_failed_rules", F.array()),
                QualityReport(dataset, layer, total, total, 0, skipped, run_id()),
            )

        flagged = df
        flag_cols: list[str] = []
        for exp in applicable:
            col_name = f"_dq_{exp.name}"
            # NULL-safe: a rule that evaluates to NULL counts as a failure.
            flagged = flagged.withColumn(col_name, F.expr(f"coalesce(({exp.expr}), false)"))
            flag_cols.append(col_name)

        flagged = flagged.withColumn(
            "_dq_failed_rules",
            F.array_compact(
                F.array(*[F.when(~F.col(f"_dq_{e.name}"), F.lit(e.name)) for e in applicable])
            ),
        ).cache()

        agg_row = flagged.agg(
            F.count(F.lit(1)).alias("total"),
            *[F.sum(F.when(~F.col(c), 1).otherwise(0)).alias(c) for c in flag_cols],
        ).collect()[0]
        total = int(agg_row["total"])

        results = [
            RuleResult(
                name=exp.name,
                action=exp.action.value,
                failed_rows=int(agg_row[f"_dq_{exp.name}"] or 0),
                total_rows=total,
                tolerance=exp.tolerance,
            )
            for exp in applicable
        ] + skipped

        blocking_failures = [r for r in results if r.action == Action.FAIL.value and not r.passed]

        # Split on the *flag columns*, not the raw expressions. The flags are already
        # NULL-coalesced, so a predicate returning NULL (e.g. length(NULL) > 1) is
        # treated as a failure here too, instead of silently passing the row through
        # as SQL three-valued logic would.
        gating = [
            f"_dq_{e.name}" for e in applicable if e.action in (Action.QUARANTINE, Action.FAIL)
        ]
        quarantine_pred = " AND ".join(gating) if gating else "true"
        valid = flagged.where(F.expr(quarantine_pred)).drop(*flag_cols, "_dq_failed_rules")
        quarantined = flagged.where(~F.expr(quarantine_pred)).drop(*flag_cols)
        q_count = quarantined.count()

        report = QualityReport(
            dataset=dataset,
            layer=layer,
            total_rows=total,
            valid_rows=total - q_count,
            quarantined_rows=q_count,
            results=results,
            run_id=run_id(),
        )
        log.info("dq.evaluated", extra=safe_extra(report.as_dict()))

        if blocking_failures:
            detail = ", ".join(
                f"{r.name}={r.failed_rows}/{r.total_rows}" for r in blocking_failures
            )
            raise DataQualityError(f"[{dataset}/{layer}] blocking expectations failed: {detail}")

        return valid, quarantined, report

    def quarantine(self, df: DataFrame, dataset: str, layer: str, table: str | None = None) -> int:
        """Persist rejected rows with their rule names and the raw payload."""
        from pyspark.sql import functions as F

        target = table or self.quarantine_table
        if not target:
            return 0
        count = df.count()
        if count == 0:
            return 0
        payload = (
            df.withColumn("_dataset", F.lit(dataset))
            .withColumn("_layer", F.lit(layer))
            .withColumn("_run_id", F.lit(run_id()))
            .withColumn("_quarantined_at", F.current_timestamp())
            .withColumn(
                "_raw_payload",
                F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_dq_")])),
            )
            .select(
                "_dataset",
                "_layer",
                "_dq_failed_rules",
                "_run_id",
                "_quarantined_at",
                "_raw_payload",
            )
        )
        (
            payload.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_dataset")
            .saveAsTable(target)
        )
        log.warning(
            "dq.quarantined",
            extra={"dataset": dataset, "layer": layer, "rows": count, "table": target},
        )
        return count

    def persist(self, report: QualityReport) -> None:
        """Append per-rule outcomes to the DQ results Delta table."""
        if not self.results_table:
            return
        self.spark.sql(_DQ_DDL.format(table=self.results_table))
        rows = [
            (
                report.dataset,
                report.layer,
                r.name,
                r.action,
                r.total_rows,
                r.failed_rows,
                float(r.failure_rate),
                r.passed,
                report.run_id,
                report.checked_at,
            )
            for r in report.results
        ]
        if not rows:
            return
        df = self.spark.createDataFrame(
            rows,
            schema="dataset string, layer string, rule string, action string, total_rows bigint, "
            "failed_rows bigint, failure_rate double, passed boolean, run_id string, "
            "checked_at timestamp",
        )
        df.write.format("delta").mode("append").saveAsTable(self.results_table)
        log.info("dq.persisted", extra={"table": self.results_table, "rules": len(rows)})
