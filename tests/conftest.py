"""Shared pytest fixtures.

The Spark fixture is session-scoped: starting a JVM costs ~10s, so per-test
sessions would make the suite unusable.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def pytest_configure(config):  # noqa: D103
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("LAKEHOUSE_CONF_DIR", str(ROOT / "conf"))


@pytest.fixture(scope="session")
def spark():
    """Local Delta-enabled SparkSession, or skip the test if Spark is unavailable."""
    pytest.importorskip("pyspark", reason="pyspark not installed")
    pytest.importorskip("delta", reason="delta-spark not installed")

    from lakehouse.config import load_env
    from lakehouse.spark_session import get_spark

    warehouse = tempfile.mkdtemp(prefix="lakehouse-test-")
    cfg = load_env("local")
    try:
        session = get_spark(
            "lakehouse-tests",
            cfg,
            extra_conf={
                "spark.sql.warehouse.dir": warehouse,
                "javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={warehouse}/metastore_db;create=true",
                "spark.sql.shuffle.partitions": "2",
                "spark.driver.memory": os.environ.get("LOCAL_DRIVER_MEMORY", "1g"),
            },
        )
    except Exception as exc:  # pragma: no cover - no JVM in this environment
        pytest.skip(f"could not start Spark: {exc}")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def orders_df(spark):
    """Small orders DataFrame with the defects our rules must catch."""
    return spark.createDataFrame(
        [
            (
                "ORD-00000001",
                "CUST-000001",
                "SKU-00001",
                2,
                100.0,
                10.0,
                "shipped",
                "USD",
                "web",
                "a@example.com",
                "2026-08-01",
                "2026-08-01 10:00:00",
            ),
            (
                "ORD-00000002",
                "CUST-000002",
                "SKU-00002",
                1,
                50.0,
                0.0,
                "delivered",
                "INR",
                "mobile_app",
                "b@example.com",
                "2026-08-01",
                "2026-08-01 11:00:00",
            ),
            (
                "ORD-00000003",
                "CUST-000003",
                "SKU-00003",
                -5,
                20.0,
                0.0,
                "pending",
                "GBP",
                "web",
                "c@example.com",
                "2026-08-02",
                "2026-08-02 09:00:00",
            ),  # bad quantity
            (
                "BADID",
                "CUST-000004",
                "SKU-00004",
                3,
                30.0,
                0.0,
                "shipped",
                "EUR",
                "retail_store",
                "d@example.com",
                "2026-08-02",
                "2026-08-02 10:00:00",
            ),  # bad id
            (
                "ORD-00000005",
                "CUST-000005",
                "SKU-00005",
                1,
                -9.99,
                0.0,
                "confirmed",
                "USD",
                "web",
                "e@example.com",
                "2026-08-03",
                "2026-08-03 08:00:00",
            ),  # negative price
        ],
        schema="order_id string, customer_id string, product_id string, quantity int, "
        "unit_price double, discount_pct double, status string, currency string, "
        "channel string, customer_email string, order_date string, updated_at string",
    )
