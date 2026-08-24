"""Column masking and row-level security.

Unity Catalog implements these as SQL UDFs bound to a column
(`SET MASK`) or a table (`SET ROW FILTER`). The functions are defined in
`sql/unity_catalog/03_masking_policies.sql`; this module registers and attaches
them, and provides a Spark-side fallback so the same masking is applied in
environments without UC (e.g. local integration tests of the PII path).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lakehouse.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

# Masking strategies available to `conf/governance.yaml`.
STRATEGIES = ("hash", "redact", "partial_email", "last4", "nullify")


@dataclass
class MaskingPolicy:
    """Apply masking either as UC policies or as DataFrame transforms."""

    spark: SparkSession
    catalog: str = ""
    privileged_group: str = "data_pii_readers"
    applied: list[str] = field(default_factory=list)

    # ------------------------------------------------------- UC policy path
    def create_functions(self, schema: str = "governance") -> None:
        """Create the masking UDFs in UC. Idempotent."""
        fq = f"{self.catalog}.{schema}" if self.catalog else schema
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {fq}",
            f"""
            CREATE OR REPLACE FUNCTION {fq}.mask_email(email STRING)
            RETURN CASE
                WHEN is_account_group_member('{self.privileged_group}') THEN email
                WHEN email IS NULL THEN NULL
                ELSE concat(left(email, 1), '****@', split_part(email, '@', 2))
            END
            """,
            f"""
            CREATE OR REPLACE FUNCTION {fq}.mask_phone(phone STRING)
            RETURN CASE
                WHEN is_account_group_member('{self.privileged_group}') THEN phone
                WHEN phone IS NULL THEN NULL
                ELSE concat('******', right(phone, 4))
            END
            """,
            f"""
            CREATE OR REPLACE FUNCTION {fq}.mask_name(name STRING)
            RETURN CASE
                WHEN is_account_group_member('{self.privileged_group}') THEN name
                ELSE sha2(name, 256)
            END
            """,
            f"""
            CREATE OR REPLACE FUNCTION {fq}.row_filter_country(country STRING)
            RETURN is_account_group_member('data_global_readers')
                OR country = current_user_region()
            """,
        ]
        for stmt in statements:
            try:
                self.spark.sql(stmt)
                self.applied.append(stmt.strip().split("\n")[0][:80])
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "masking.function_skipped", extra={"reason": str(exc).split("\n", 1)[0][:180]}
                )

    def attach(self, table: str, column: str, function: str) -> None:
        """Bind a mask to a column."""
        try:
            self.spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {function}")
            log.info(
                "masking.attached", extra={"table": table, "column": column, "function": function}
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "masking.attach_skipped",
                extra={
                    "table": table,
                    "column": column,
                    "reason": str(exc).split("\n", 1)[0][:180],
                },
            )

    def attach_row_filter(self, table: str, function: str, columns: list[str]) -> None:
        """Bind a row filter to a table."""
        cols = ", ".join(columns)
        try:
            self.spark.sql(f"ALTER TABLE {table} SET ROW FILTER {function} ON ({cols})")
            log.info("masking.row_filter_attached", extra={"table": table, "function": function})
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "masking.row_filter_skipped",
                extra={"table": table, "reason": str(exc).split("\n", 1)[0][:180]},
            )

    # -------------------------------------------------- DataFrame fallback
    @staticmethod
    def mask_dataframe(df: DataFrame, policies: dict[str, str]) -> DataFrame:
        """Apply masking in Spark - used where UC policies are unavailable.

        `policies` maps column -> strategy, e.g. {"email": "partial_email"}.
        """
        from pyspark.sql import functions as F

        for column, strategy in policies.items():
            if column not in df.columns:
                continue
            col = F.col(column)
            if strategy == "hash":
                masked = F.sha2(col.cast("string"), 256)
            elif strategy == "redact":
                masked = F.lit("[REDACTED]")
            elif strategy == "partial_email":
                masked = F.when(col.isNull(), None).otherwise(
                    F.concat(
                        F.substring(col, 1, 1), F.lit("****@"), F.element_at(F.split(col, "@"), -1)
                    )
                )
            elif strategy == "last4":
                masked = F.when(col.isNull(), None).otherwise(
                    F.concat(F.lit("******"), F.substring(col, -4, 4))
                )
            elif strategy == "nullify":
                masked = F.lit(None).cast(df.schema[column].dataType)
            else:
                raise ValueError(f"unknown masking strategy '{strategy}' for column '{column}'")
            df = df.withColumn(column, masked)
        return df
