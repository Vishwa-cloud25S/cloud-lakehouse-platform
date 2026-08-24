"""Reusable, dataset-agnostic column transforms."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

_INVALID = re.compile(r"[^0-9a-zA-Z_]+")


def clean_name(name: str) -> str:
    """snake_case a source column name so it is safe for Delta + SQL + Parquet."""
    name = name.strip().replace("-", "_").replace(" ", "_")
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    name = _INVALID.sub("_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name or "unnamed"


def normalise_columns(df: DataFrame) -> DataFrame:
    """Rename every column to snake_case, de-duplicating collisions."""
    seen: dict[str, int] = {}
    mapping: list[tuple[str, str]] = []
    for col in df.columns:
        new = clean_name(col)
        if new in seen:
            seen[new] += 1
            new = f"{new}_{seen[new]}"
        else:
            seen[new] = 0
        mapping.append((col, new))
    for old, new in mapping:
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def add_row_hash(df: DataFrame, columns: list[str], out_col: str = "row_hash") -> DataFrame:
    """Deterministic SHA-256 over the business columns.

    Used for (a) SCD2 change detection and (b) cheap "did anything actually change?"
    filtering before an expensive MERGE. NULLs are encoded distinctly from empty
    strings so 'NULL -> ""' is still detected as a change.
    """
    from pyspark.sql import functions as F

    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in sorted(columns)]
    return df.withColumn(out_col, F.sha2(F.concat_ws("||", *parts), 256))


def dedupe_by_key(
    df: DataFrame,
    keys: list[str],
    order_by: str = "updated_at",
    tiebreaker: str | None = "_ingest_timestamp",
) -> DataFrame:
    """Keep exactly one row per key - the newest by `order_by`.

    A CDC feed routinely delivers several versions of the same key in one batch;
    MERGE raises `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW` if you skip this.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order_cols = [F.col(order_by).desc_nulls_last()]
    if tiebreaker and tiebreaker in df.columns:
        order_cols.append(F.col(tiebreaker).desc_nulls_last())
    window = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def cast_columns(df: DataFrame, casts: dict[str, str]) -> DataFrame:
    """Apply `{column: sql_type}`, tolerating columns absent from this batch."""
    from pyspark.sql import functions as F

    for col, dtype in casts.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def trim_strings(df: DataFrame, exclude: list[str] | None = None) -> DataFrame:
    """Trim every string column. Leading/trailing spaces are the #1 join-miss cause."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    skip = set(exclude or [])
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType) and field.name not in skip:
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def add_surrogate_key(df: DataFrame, business_keys: list[str], out_col: str = "sk") -> DataFrame:
    """Stable hash surrogate key.

    Hash-based (not monotonically_increasing_id) so the same business key yields the
    same SK on every rebuild - facts stay joinable after a dimension refresh.
    """
    from pyspark.sql import functions as F

    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in business_keys]
    return df.withColumn(out_col, F.xxhash64(F.concat_ws("||", *parts)))
