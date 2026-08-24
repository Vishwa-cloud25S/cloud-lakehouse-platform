"""Source readers.

Bronze ingestion is *incremental by construction*:

* On Databricks we use **Auto Loader** (`cloudFiles`), which keeps a RocksDB file
  index in the checkpoint so a directory with 10M files is still O(new files).
* Locally / in CI we fall back to a batch reader with modification-time filtering,
  which gives identical semantics for tests without a streaming runtime.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from lakehouse.config import DatasetConfig, EnvConfig
from lakehouse.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

DEFAULT_CSV_OPTIONS: dict[str, str] = {
    "header": "true",
    "inferSchema": "false",  # explicit schema or string-then-cast: never guess in prod
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record",
    "escape": '"',
    "multiLine": "false",
    "emptyValue": "",
    "nullValue": "",
}


def read_source(
    spark: SparkSession,
    dataset: DatasetConfig,
    cfg: EnvConfig,
    since: datetime | None = None,
    use_autoloader: bool = False,
    schema: Any = None,
) -> DataFrame:
    """Read new source files for `dataset`.

    Parameters
    ----------
    since
        Only files modified after this instant are considered (batch mode).
    use_autoloader
        Force Auto Loader; defaults to on for non-local environments.
    """
    fmt = dataset.source_format.lower()
    path = dataset.source_path
    autoloader = use_autoloader or (not cfg.is_local)

    if autoloader:
        return _read_autoloader(spark, dataset, cfg, fmt, path, schema)
    return _read_batch(spark, dataset, fmt, path, since, schema)


def _read_autoloader(
    spark: SparkSession,
    dataset: DatasetConfig,
    cfg: EnvConfig,
    fmt: str,
    path: str,
    schema: Any,
) -> DataFrame:
    """Auto Loader stream: exactly-once file discovery with schema inference cache."""
    schema_location = f"{cfg.checkpoint_path}/{dataset.name}/_schema"
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", schema_location)
        # Absorb new columns instead of failing the stream; the job restarts once
        # and the SchemaRegistry records the diff.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        .option("cloudFiles.useNotifications", "false")  # directory listing; flip to true at scale
        .option("rescuedDataColumn", "_rescued_data")
    )
    if fmt == "csv":
        for key, value in DEFAULT_CSV_OPTIONS.items():
            reader = reader.option(key, value)
    if schema is not None:
        reader = reader.schema(schema)
    log.info("source.autoloader", extra={"dataset": dataset.name, "path": path, "format": fmt})
    return reader.load(path)


def _read_batch(
    spark: SparkSession,
    dataset: DatasetConfig,
    fmt: str,
    path: str,
    since: datetime | None,
    schema: Any,
) -> DataFrame:
    """Batch read with modifiedAfter pushdown - the local/CI equivalent."""
    reader = spark.read.format(fmt)
    if fmt == "csv":
        for key, value in DEFAULT_CSV_OPTIONS.items():
            reader = reader.option(key, value)
    if fmt == "json":
        reader = reader.option("multiLine", "true")
    if schema is not None:
        reader = reader.schema(schema)
    elif fmt == "csv":
        # Without a schema, read everything as STRING and cast in Silver. This makes
        # ingestion immune to a stray "N/A" in an INT column - the row lands in
        # Bronze intact and fails a *quality* check rather than the whole job.
        reader = reader.option("inferSchema", "false")
    if since is not None:
        reader = reader.option("modifiedAfter", since.strftime("%Y-%m-%dT%H:%M:%S"))
    log.info(
        "source.batch",
        extra={
            "dataset": dataset.name,
            "path": path,
            "format": fmt,
            "modified_after": since.isoformat() if since else None,
        },
    )
    try:
        return reader.load(path)
    except Exception as exc:  # noqa: BLE001
        # When `modifiedAfter` filters out every file there is nothing to infer a
        # schema from. That is the normal "no new data" case for an incremental
        # run, so return an empty DataFrame and let the caller record a skip.
        if "UNABLE_TO_INFER_SCHEMA" in str(exc) or "PATH_NOT_FOUND" in str(exc):
            from pyspark.sql.types import StructType

            log.info(
                "source.no_new_files",
                extra={
                    "dataset": dataset.name,
                    "path": path,
                    "modified_after": since.isoformat() if since else None,
                },
            )
            return spark.createDataFrame([], StructType())
        raise
