"""End-to-end medallion tests against a real local Delta lake.

These are the tests that would have caught every bug found while building this
project: incremental no-ops, schema evolution, SCD2 history and MERGE idempotency.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    """Isolated warehouse + config for the integration run."""
    return tmp_path_factory.mktemp("lake")


@pytest.mark.spark
def test_merge_upsert_is_idempotent(spark, lake):
    """Running the same MERGE twice must not duplicate or change rows."""
    from lakehouse.io.delta_writer import DeltaWriter

    spark.sql("CREATE SCHEMA IF NOT EXISTS itest")
    table = "itest.upsert_target"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    df = spark.createDataFrame(
        [("K1", "a", "h1"), ("K2", "b", "h2")], "id string, val string, row_hash string"
    )
    writer = DeltaWriter(spark, auto_optimize=False)
    writer.create_if_missing(df, table, location=str(lake / "upsert_target"))
    writer.merge_upsert(df, table, keys=["id"])
    first = spark.table(table).count()

    writer.merge_upsert(df, table, keys=["id"])
    assert spark.table(table).count() == first == 2


@pytest.mark.spark
def test_merge_updates_changed_rows_only(spark, lake):
    from lakehouse.io.delta_writer import DeltaWriter

    spark.sql("CREATE SCHEMA IF NOT EXISTS itest")
    table = "itest.upsert_changes"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    writer = DeltaWriter(spark, auto_optimize=False)
    v1 = spark.createDataFrame(
        [("K1", "old", "h1"), ("K2", "keep", "h2")], "id string, val string, row_hash string"
    )
    writer.create_if_missing(v1, table, location=str(lake / "upsert_changes"))
    writer.merge_upsert(v1, table, keys=["id"])

    v2 = spark.createDataFrame(
        [("K1", "new", "h1-changed"), ("K2", "keep", "h2")],
        "id string, val string, row_hash string",
    )
    result = writer.merge_upsert(
        v2, table, keys=["id"], update_condition="t.row_hash <> s.row_hash"
    )
    rows = {r["id"]: r["val"] for r in spark.table(table).collect()}
    assert rows == {"K1": "new", "K2": "keep"}
    assert result.rows_updated == 1  # only the changed row was rewritten


@pytest.mark.spark
def test_schema_evolution_additive_is_absorbed(spark, lake):
    """A new source column must flow through without a manual migration."""
    from lakehouse.io.schema_registry import SchemaRegistry

    spark.sql("CREATE SCHEMA IF NOT EXISTS itest")
    spark.sql("DROP TABLE IF EXISTS itest.schema_registry")
    registry = SchemaRegistry(
        spark, "itest.schema_registry", location=str(lake / "schema_registry")
    )

    v1 = spark.createDataFrame([("a", 1)], "id string, qty int")
    assert registry.enforce(v1, "ds", "bronze", "additive").is_empty

    v2 = spark.createDataFrame([("a", 1, "PROMO")], "id string, qty int, promo string")
    diff = registry.enforce(v2, "ds", "bronze", "additive")
    assert diff.added == {"promo": "string"}
    assert diff.is_additive


@pytest.mark.spark
def test_schema_evolution_rejects_breaking_change(spark, lake):
    from lakehouse.io.schema_registry import SchemaEvolutionError, SchemaRegistry

    spark.sql("CREATE SCHEMA IF NOT EXISTS itest")
    spark.sql("DROP TABLE IF EXISTS itest.schema_registry_strict")
    registry = SchemaRegistry(
        spark, "itest.schema_registry_strict", location=str(lake / "schema_registry_strict")
    )

    v1 = spark.createDataFrame([("a", 1, "x")], "id string, qty int, dropme string")
    registry.enforce(v1, "ds2", "bronze", "additive")

    v2 = spark.createDataFrame([("a", 1)], "id string, qty int")  # column dropped
    with pytest.raises(SchemaEvolutionError):
        registry.enforce(v2, "ds2", "bronze", "additive")


@pytest.mark.spark
def test_watermark_advances_and_never_regresses(spark, lake):
    from datetime import datetime, timezone

    from lakehouse.io.watermark import WatermarkStore

    spark.sql("CREATE SCHEMA IF NOT EXISTS itest")
    spark.sql("DROP TABLE IF EXISTS itest.watermarks")
    store = WatermarkStore(spark, "itest.watermarks", location=str(lake / "watermarks"))

    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2026, 6, 1, tzinfo=timezone.utc)
    store.advance("ds", "bronze", t1, 10, "run1")
    assert store.get("ds", "bronze").date() == t1.date()

    store.advance("ds", "bronze", t2, 20, "run2")
    assert store.get("ds", "bronze").date() == t2.date()

    store.advance("ds", "bronze", t1, 5, "run3")  # late/duplicate run
    assert store.get("ds", "bronze").date() == t2.date()


@pytest.mark.spark
def test_scd2_closes_old_version_and_opens_new(spark, lake):
    """The core SCD2 guarantee: exactly one current row, history preserved."""
    from datetime import datetime, timezone

    from lakehouse.io.delta_writer import DeltaWriter

    spark.sql("CREATE SCHEMA IF NOT EXISTS itest")
    table = "itest.scd2_dim"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    writer = DeltaWriter(spark, auto_optimize=False)

    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    v1 = spark.createDataFrame(
        [("C1", "Consumer", "h1", t1)],
        "customer_id string, segment string, row_hash string, effective_from timestamp",
    )
    writer.merge_scd2(v1, table, keys=["customer_id"], tracked_columns=["segment"])
    assert spark.table(table).count() == 1

    t2 = datetime(2026, 6, 1, tzinfo=timezone.utc)
    v2 = spark.createDataFrame(
        [("C1", "Enterprise", "h2", t2)],
        "customer_id string, segment string, row_hash string, effective_from timestamp",
    )
    writer.merge_scd2(v2, table, keys=["customer_id"], tracked_columns=["segment"])

    rows = spark.table(table).collect()
    assert len(rows) == 2, "old version must be retained as history"
    current = [r for r in rows if r["is_current"]]
    assert len(current) == 1, "exactly one row may be current"
    assert current[0]["segment"] == "Enterprise"
    closed = [r for r in rows if not r["is_current"]][0]
    assert closed["effective_to"] is not None


@pytest.mark.spark
def test_gold_fact_joins_point_in_time_customer_version(spark):
    """Revenue must attach to the segment that was current at order time."""
    from datetime import datetime, timezone

    from lakehouse.transforms.gold import build_fact_orders

    orders = spark.createDataFrame(
        [
            (
                "O1",
                "C1",
                "P1",
                2,
                10.0,
                20.0,
                0.0,
                20.0,
                False,
                "2026-03-01",
                "2026-03",
                "shipped",
                "web",
                "USD",
                datetime(2026, 3, 1, tzinfo=timezone.utc),
            )
        ],
        "order_id string, customer_id string, product_id string, quantity int, "
        "unit_price double, gross_amount double, discount_amount double, net_amount double, "
        "is_cancelled boolean, order_date string, order_month string, status string, "
        "channel string, currency string, updated_at timestamp",
    )
    dim_customer = spark.createDataFrame(
        [
            (
                1,
                10,
                "C1",
                "Consumer",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 2, 1, tzinfo=timezone.utc),
                False,
            ),
            (2, 10, "C1", "Enterprise", datetime(2026, 2, 1, tzinfo=timezone.utc), None, True),
        ],
        "customer_version_key long, customer_key long, customer_id string, segment string, "
        "effective_from timestamp, effective_to timestamp, is_current boolean",
    )
    dim_product = spark.createDataFrame(
        [(100, "P1", 4.0)], "product_key long, product_id string, unit_cost double"
    )
    fact = build_fact_orders(orders, dim_customer, dim_product).collect()[0]
    assert fact["customer_version_key"] == 2  # the version valid on 2026-03-01
    assert float(fact["cogs_amount"]) == 8.0
    assert float(fact["gross_profit"]) == 12.0


@pytest.mark.spark
def test_unmatched_dimension_becomes_unknown_member(spark):
    from datetime import datetime, timezone

    from lakehouse.transforms.gold import build_fact_orders

    orders = spark.createDataFrame(
        [
            (
                "O1",
                "GHOST",
                "GHOST",
                1,
                10.0,
                10.0,
                0.0,
                10.0,
                False,
                "2026-03-01",
                "2026-03",
                "shipped",
                "web",
                "USD",
                datetime(2026, 3, 1, tzinfo=timezone.utc),
            )
        ],
        "order_id string, customer_id string, product_id string, quantity int, "
        "unit_price double, gross_amount double, discount_amount double, net_amount double, "
        "is_cancelled boolean, order_date string, order_month string, status string, "
        "channel string, currency string, updated_at timestamp",
    )
    empty_cust = spark.createDataFrame(
        [],
        "customer_version_key long, customer_key long, customer_id string, "
        "effective_from timestamp, effective_to timestamp, is_current boolean",
    )
    empty_prod = spark.createDataFrame([], "product_key long, product_id string, unit_cost double")
    fact = build_fact_orders(orders, empty_cust, empty_prod).collect()[0]
    assert fact["customer_key"] == -1
    assert fact["has_unknown_customer"] is True
