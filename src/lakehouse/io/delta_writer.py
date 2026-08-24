"""Delta writes: append, idempotent MERGE upsert, and SCD2 - all schema-aware."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)


@dataclass
class WriteResult:
    """Row counts returned by the Delta commit, used for metrics + alerting."""

    table: str
    mode: str
    rows_written: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    version: int | None = None

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


class DeltaWriter:
    """Thin, opinionated wrapper over Delta writes.

    Responsibilities kept here so no job re-implements them:
      * create-if-missing with the right partitioning, properties and comment
      * `mergeSchema` only when the dataset's evolution policy allows it
      * MERGE key construction, including partition pruning predicates
      * SCD2 close-out + insert in a single transaction
    """

    def __init__(self, spark: SparkSession, auto_optimize: bool = True) -> None:
        self.spark = spark
        self.auto_optimize = auto_optimize

    # ------------------------------------------------------------------ utils
    def table_exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(table)

    def _version(self, table: str) -> int | None:
        try:
            return (
                self.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
                .select("version")
                .collect()[0][0]
            )
        except Exception:  # pragma: no cover - table may not support history
            return None

    def _last_operation_metrics(self, table: str) -> dict[str, str]:
        try:
            row = (
                self.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
                .select("operationMetrics")
                .collect()[0][0]
            )
            return dict(row or {})
        except Exception:  # pragma: no cover
            return {}

    def create_if_missing(
        self,
        df: DataFrame,
        table: str,
        partition_by: list[str] | None = None,
        location: str | None = None,
        comment: str = "",
        properties: dict[str, str] | None = None,
    ) -> bool:
        """Create an empty table with the right layout. Returns True if created."""
        if self.table_exists(table):
            return False
        writer = df.limit(0).write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if location:
            writer = writer.option("path", location)
        writer.saveAsTable(table)

        if comment:
            self.spark.sql(f"COMMENT ON TABLE {table} IS '{comment.replace(chr(39), chr(39) * 2)}'")
        props = {
            # Column mapping is what makes rename/drop possible later without a rewrite.
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
            "delta.enableChangeDataFeed": "true",  # downstream CDC without full scans
            "delta.autoOptimize.optimizeWrite": str(self.auto_optimize).lower(),
            "delta.autoOptimize.autoCompact": str(self.auto_optimize).lower(),
            **(properties or {}),
        }
        pairs = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        self.spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({pairs})")
        log.info(
            "delta.table.created",
            extra={"table": table, "partition_by": partition_by, "location": location},
        )
        return True

    # ----------------------------------------------------------------- writes
    def append(
        self,
        df: DataFrame,
        table: str,
        partition_by: list[str] | None = None,
        merge_schema: bool = True,
        location: str | None = None,
    ) -> WriteResult:
        """Append-only write - the Bronze contract."""
        rows = df.count()
        writer = df.write.format("delta").mode("append")
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if location:
            writer = writer.option("path", location)
        writer.saveAsTable(table)
        result = WriteResult(
            table=table,
            mode="append",
            rows_written=rows,
            rows_inserted=rows,
            version=self._version(table),
        )
        log.info("delta.append", extra=result.as_dict())
        return result

    def merge_upsert(
        self,
        df: DataFrame,
        table: str,
        keys: list[str],
        update_condition: str | None = None,
        partition_by: list[str] | None = None,
        partition_prune_values: dict[str, list[str]] | None = None,
        merge_schema: bool = True,
        delete_condition: str | None = None,
    ) -> WriteResult:
        """Idempotent upsert.

        `partition_prune_values` adds an `AND t.part IN (...)` term to the merge
        predicate. Without it Delta must scan every partition of the target to find
        matches; with it, a daily load touches only today's partitions. On a
        multi-TB fact table this is the single biggest MERGE win available.
        """
        from delta.tables import DeltaTable

        if not self.table_exists(table):
            self.create_if_missing(df, table, partition_by=partition_by)

        if merge_schema:
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        cond_parts = [f"t.{k} = s.{k}" for k in keys]
        if partition_prune_values:
            for col, values in partition_prune_values.items():
                if values:
                    literals = ", ".join(f"'{v}'" for v in values)
                    cond_parts.append(f"t.{col} IN ({literals})")
        condition = " AND ".join(cond_parts)

        target = DeltaTable.forName(self.spark, table)
        builder = target.alias("t").merge(df.alias("s"), condition)
        builder = (
            builder.whenMatchedUpdateAll(condition=update_condition)
            if update_condition
            else builder.whenMatchedUpdateAll()
        )
        if delete_condition:
            builder = builder.whenMatchedDelete(condition=delete_condition)
        builder = builder.whenNotMatchedInsertAll()
        builder.execute()

        if merge_schema:
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

        metrics = self._last_operation_metrics(table)
        result = WriteResult(
            table=table,
            mode="merge",
            rows_written=int(metrics.get("numOutputRows", 0) or 0),
            rows_inserted=int(metrics.get("numTargetRowsInserted", 0) or 0),
            rows_updated=int(metrics.get("numTargetRowsUpdated", 0) or 0),
            rows_deleted=int(metrics.get("numTargetRowsDeleted", 0) or 0),
            version=self._version(table),
        )
        log.info("delta.merge", extra={**result.as_dict(), "condition": condition})
        return result

    def merge_scd2(
        self,
        df: DataFrame,
        table: str,
        keys: list[str],
        tracked_columns: list[str],
        effective_col: str = "effective_from",
        end_col: str = "effective_to",
        current_col: str = "is_current",
        hash_col: str = "row_hash",
    ) -> WriteResult:
        """Type-2 slowly changing dimension in one atomic MERGE.

        Implemented with the standard two-pass trick: rows whose tracked attributes
        changed are staged twice - once with a NULL join key (forcing an INSERT of the
        new version) and once with the real key (closing the old version). Both land
        in a single Delta transaction, so a reader never sees two current rows.
        """
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        if not self.table_exists(table):
            seed = df.withColumn(current_col, F.lit(True)).withColumn(
                end_col, F.lit(None).cast("timestamp")
            )
            self.create_if_missing(seed, table)
            return self.append(seed, table, merge_schema=True)

        target = DeltaTable.forName(self.spark, table)
        target_df = target.toDF().where(F.col(current_col))
        join_cond = [df[k] == target_df[k] for k in keys]

        changed = (
            df.alias("s")
            .join(target_df.alias("t"), join_cond, "inner")
            .where(F.col(f"s.{hash_col}") != F.col(f"t.{hash_col}"))
            .select("s.*")
        )
        # Pass 1: null merge key -> guaranteed INSERT of the new version.
        staged_inserts = changed.withColumn("__merge_key", F.lit(None).cast("string"))
        # Pass 2: real key -> matches the current row so it can be closed.
        staged_updates = df.withColumn("__merge_key", F.concat_ws("||", *[F.col(k) for k in keys]))
        staged = staged_updates.unionByName(staged_inserts)

        merge_condition = (
            "concat_ws('||', " + ", ".join(f"t.{k}" for k in keys) + ") = s.__merge_key"
            f" AND t.{current_col} = true"
        )
        (
            target.alias("t")
            .merge(staged.alias("s"), merge_condition)
            .whenMatchedUpdate(
                condition=f"t.{hash_col} <> s.{hash_col}",
                set={
                    current_col: F.lit(False),
                    end_col: F.col(f"s.{effective_col}"),
                },
            )
            .whenNotMatchedInsert(
                values={
                    **{c: F.col(f"s.{c}") for c in df.columns},
                    current_col: F.lit(True),
                    end_col: F.lit(None).cast("timestamp"),
                }
            )
            .execute()
        )
        metrics = self._last_operation_metrics(table)
        result = WriteResult(
            table=table,
            mode="scd2",
            rows_written=int(metrics.get("numOutputRows", 0) or 0),
            rows_inserted=int(metrics.get("numTargetRowsInserted", 0) or 0),
            rows_updated=int(metrics.get("numTargetRowsUpdated", 0) or 0),
            version=self._version(table),
        )
        log.info("delta.scd2", extra={**result.as_dict(), "tracked": tracked_columns})
        return result

    def overwrite_partitions(
        self, df: DataFrame, table: str, partition_by: list[str], replace_where: str | None = None
    ) -> WriteResult:
        """Dynamic partition overwrite - the idempotent way to rebuild Gold aggregates."""
        rows = df.count()
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "false")
        if replace_where:
            writer = writer.option("replaceWhere", replace_where)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table)
        result = WriteResult(
            table=table, mode="overwrite", rows_written=rows, version=self._version(table)
        )
        log.info("delta.overwrite", extra={**result.as_dict(), "replace_where": replace_where})
        return result
