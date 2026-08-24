"""Spark session factory with the Delta + performance configuration we actually want.

Every tuning flag here is deliberate; see `docs/spark_optimisation.md` for the
reasoning and the measured effect of each one.
"""

from __future__ import annotations

import os
from typing import Any

from lakehouse.config import EnvConfig
from lakehouse.logging_utils import get_logger

log = get_logger(__name__)

# Applied in every environment.
BASE_CONF: dict[str, str] = {
    # --- Delta Lake wiring -------------------------------------------------
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # --- Adaptive Query Execution -----------------------------------------
    # AQE re-plans on real runtime statistics: it collapses the post-shuffle
    # partition count and rescues skewed joins that would otherwise straggle.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    # --- Delta write behaviour --------------------------------------------
    # Compact small files on write; the classic streaming-ingest failure mode.
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    # Deletion vectors turn MERGE/DELETE from rewrite-the-file into mark-the-row.
    "spark.databricks.delta.properties.defaults.enableDeletionVectors": "true",
    # Fail loudly instead of silently corrupting a table with a bad overwrite.
    "spark.databricks.delta.schema.autoMerge.enabled": "false",
    # --- General SQL behaviour --------------------------------------------
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}

# Only meaningful on a real cluster; harmless locally but noisy, so kept separate.
CLUSTER_CONF: dict[str, str] = {
    "spark.databricks.io.cache.enabled": "true",  # local SSD cache of remote Parquet
    "spark.sql.autoBroadcastJoinThreshold": str(64 * 1024 * 1024),
    "spark.sql.broadcastTimeout": "1200",
    "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
}

LOCAL_CONF: dict[str, str] = {
    "spark.master": "local[*]",
    # A *persistent* Derby metastore. Without this the local catalog is in-memory,
    # so table registrations vanish between runs while the Delta files remain -
    # the next run then hits DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION. Databricks
    # has a durable metastore (Unity Catalog); this makes local behave the same.
    "javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={os.path.abspath('./metastore_db')};create=true",
    "spark.driver.memory": os.environ.get("LOCAL_DRIVER_MEMORY", "2g"),
    "spark.ui.enabled": "false",
    "spark.sql.shuffle.partitions": "4",
    "spark.databricks.delta.snapshotPartitions": "2",
    "spark.sql.warehouse.dir": os.path.abspath("./spark-warehouse"),
    # Offline/CI escape hatch: point at pre-downloaded Delta jars instead of Ivy.
    **({"spark.jars": os.environ["DELTA_JARS"]} if os.environ.get("DELTA_JARS") else {}),
    # Local runs have no Databricks service backing these features.
    "spark.databricks.delta.optimizeWrite.enabled": "false",
    "spark.databricks.delta.autoCompact.enabled": "false",
}


def get_spark(
    app_name: str = "lakehouse",
    cfg: EnvConfig | None = None,
    extra_conf: dict[str, Any] | None = None,
):
    """Return a configured SparkSession, reusing the Databricks one when present."""
    from pyspark.sql import SparkSession

    local = cfg.is_local if cfg else True
    builder = SparkSession.builder.appName(app_name)

    conf: dict[str, str] = dict(BASE_CONF)
    conf.update(LOCAL_CONF if local else CLUSTER_CONF)
    if cfg:
        conf["spark.sql.shuffle.partitions"] = str(cfg.shuffle_partitions)
    if extra_conf:
        conf.update({k: str(v) for k, v in extra_conf.items()})

    for key, value in conf.items():
        builder = builder.config(key, value)

    # DELTA_JARS lets an air-gapped/CI runner supply the Delta jars directly and
    # skip Ivy/Maven resolution entirely.
    if local and not os.environ.get("DELTA_JARS"):
        try:  # configure_spark_with_delta_pip resolves the delta jars from Maven
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:  # pragma: no cover - jars provided by the cluster
            pass

    if local:
        # enableHiveSupport() persists table metadata in the Derby metastore above.
        try:
            builder = builder.enableHiveSupport()
        except Exception:  # pragma: no cover - hive jars absent
            log.warning("spark.hive_support_unavailable")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    log.info(
        "spark.session.ready",
        extra={
            "app": app_name,
            "mode": "local" if local else "cluster",
            "shuffle_partitions": conf.get("spark.sql.shuffle.partitions"),
        },
    )
    return spark


def apply_abfss_auth(spark, storage_account: str) -> None:
    """Wire OAuth (service principal) auth for ADLS Gen2.

    On Databricks the preferred path is a Unity Catalog *external location* backed by
    an Access Connector (managed identity) - no secrets in Spark conf at all. This
    helper exists for non-UC clusters and local integration tests against real ADLS.
    """
    if not storage_account:
        return
    host = f"{storage_account}.dfs.core.windows.net"
    tenant = os.environ.get("AZURE_TENANT_ID", "")
    client_id = os.environ.get("ARM_CLIENT_ID", "")
    secret = os.environ.get("ARM_CLIENT_SECRET", "")
    if not all([tenant, client_id, secret]):
        log.warning("abfss.auth.skipped", extra={"reason": "missing SP env vars", "host": host})
        return
    spark.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{host}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{host}", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{host}", secret)
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{host}",
        f"https://login.microsoftonline.com/{tenant}/oauth2/token",
    )
    log.info("abfss.auth.configured", extra={"host": host})
