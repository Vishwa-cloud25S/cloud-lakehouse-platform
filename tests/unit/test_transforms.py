"""Transform behaviour that business logic depends on."""

from __future__ import annotations

import pytest

from lakehouse.transforms.common import (
    add_row_hash,
    add_surrogate_key,
    clean_name,
    dedupe_by_key,
    normalise_columns,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Order ID", "order_id"),
        ("customer-email", "customer_email"),
        ("UnitPrice", "unit_price"),
        ("  spaced  ", "spaced"),
        ("weird!!chars##", "weird_chars"),
        ("", "unnamed"),
    ],
)
def test_clean_name(raw, expected):
    assert clean_name(raw) == expected


@pytest.mark.spark
def test_normalise_columns_deduplicates_collisions(spark):
    df = spark.createDataFrame([(1, 2)], "`Order ID` int, `order-id` int")
    out = normalise_columns(df)
    assert out.columns == ["order_id", "order_id_1"]


@pytest.mark.spark
def test_row_hash_is_stable_and_change_sensitive(spark):
    a = spark.createDataFrame([("x", 1)], "k string, v int")
    b = spark.createDataFrame([("x", 1)], "k string, v int")
    c = spark.createDataFrame([("x", 2)], "k string, v int")
    h = lambda d: add_row_hash(d, ["k", "v"]).collect()[0]["row_hash"]  # noqa: E731
    assert h(a) == h(b)
    assert h(a) != h(c)


@pytest.mark.spark
def test_row_hash_distinguishes_null_from_empty(spark):
    a = spark.createDataFrame([("x", None)], "k string, v string")
    b = spark.createDataFrame([("x", "")], "k string, v string")
    ha = add_row_hash(a, ["k", "v"]).collect()[0]["row_hash"]
    hb = add_row_hash(b, ["k", "v"]).collect()[0]["row_hash"]
    assert ha != hb


@pytest.mark.spark
def test_dedupe_keeps_newest_version(spark):
    df = spark.createDataFrame(
        [
            ("K1", "old", "2026-01-01 00:00:00"),
            ("K1", "new", "2026-01-02 00:00:00"),
            ("K2", "only", "2026-01-01 00:00:00"),
        ],
        "id string, val string, updated_at string",
    )
    out = dedupe_by_key(df, ["id"], order_by="updated_at").collect()
    assert len(out) == 2
    assert {r["id"]: r["val"] for r in out}["K1"] == "new"


@pytest.mark.spark
def test_surrogate_key_is_deterministic(spark):
    df = spark.createDataFrame([("CUST-1",)], "customer_id string")
    k1 = add_surrogate_key(df, ["customer_id"], "sk").collect()[0]["sk"]
    k2 = add_surrogate_key(df, ["customer_id"], "sk").collect()[0]["sk"]
    assert k1 == k2


@pytest.mark.spark
def test_conform_orders_computes_money_correctly(spark, orders_df):
    from lakehouse.transforms.silver import conform_orders

    out = conform_orders(orders_df).where("order_id = 'ORD-00000001'").collect()[0]
    # 2 x 100.00 with 10% discount -> gross 200, discount 20, net 180
    assert float(out["gross_amount"]) == 200.0
    assert float(out["discount_amount"]) == 20.0
    assert float(out["net_amount"]) == 180.0


@pytest.mark.spark
def test_conform_orders_normalises_case(spark, orders_df):
    from lakehouse.transforms.silver import conform_orders

    rows = conform_orders(orders_df).collect()
    assert all(r["status"] == r["status"].lower() for r in rows)
    assert all(r["currency"] == r["currency"].upper() for r in rows)
