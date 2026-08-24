"""Bronze: land the source *as it arrived*, plus provenance.

Rules for this layer:
  * never cast, never filter, never dedupe - Bronze is the replayable audit copy
  * every row carries where it came from, when it landed and which run wrote it
  * partition by ingest date so a backfill can target a slice
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse.logging_utils import run_id
from lakehouse.transforms.common import normalise_columns

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

INGEST_COLUMNS = (
    "_ingest_timestamp",
    "_ingest_date",
    "_source_file",
    "_source_system",
    "_run_id",
    "_batch_id",
)


def add_ingest_metadata(df: DataFrame, source_system: str, batch_id: str = "") -> DataFrame:
    """Attach provenance columns used for lineage, replay and incremental filters."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.current_timestamp()))
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path") if "_metadata" in df.columns else F.input_file_name(),
        )
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_run_id", F.lit(run_id()))
        .withColumn("_batch_id", F.lit(batch_id or run_id()))
    )


def standardise_bronze(df: DataFrame, source_system: str, batch_id: str = "") -> DataFrame:
    """Normalise column names then add provenance. No type coercion by design."""
    return add_ingest_metadata(normalise_columns(df), source_system, batch_id)
