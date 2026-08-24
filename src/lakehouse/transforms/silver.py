"""Silver: typed, deduplicated, business-conformed entities.

This is where the string-typed Bronze payload becomes trustworthy data:
casting, trimming, currency normalisation, derived columns and dedupe.
Quality enforcement runs *after* these transforms so rules can assume real types.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse.transforms.common import add_row_hash, cast_columns, dedupe_by_key, trim_strings

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

ORDER_CASTS = {
    "quantity": "int",
    "unit_price": "decimal(18,4)",
    "discount_pct": "decimal(9,4)",
    "order_date": "date",
    "updated_at": "timestamp",
}

CUSTOMER_CASTS = {
    "signup_date": "date",
    "updated_at": "timestamp",
    "lifetime_value": "decimal(18,2)",
}

PRODUCT_CASTS = {
    "list_price": "decimal(18,4)",
    "unit_cost": "decimal(18,4)",
    "updated_at": "timestamp",
    "is_active": "boolean",
}

ORDER_BUSINESS_COLS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "status",
    "currency",
    "order_date",
    "channel",
]

CUSTOMER_BUSINESS_COLS = [
    "customer_id",
    "full_name",
    "email",
    "phone",
    "country",
    "city",
    "segment",
    "signup_date",
]

PRODUCT_BUSINESS_COLS = [
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "list_price",
    "unit_cost",
    "is_active",
]


def conform_orders(df: DataFrame) -> DataFrame:
    """Type, clean and enrich the orders stream."""
    from pyspark.sql import functions as F

    df = trim_strings(df, exclude=["_raw_payload"])
    df = cast_columns(df, ORDER_CASTS)
    df = (
        df.withColumn("status", F.lower(F.col("status")))
        .withColumn("currency", F.upper(F.col("currency")))
        .withColumn("channel", F.lower(F.coalesce(F.col("channel"), F.lit("unknown"))))
        .withColumn(
            "discount_pct", F.coalesce(F.col("discount_pct"), F.lit(0).cast("decimal(9,4)"))
        )
    )
    # Money is computed once, here, so every downstream consumer agrees.
    df = (
        df.withColumn(
            "gross_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,4)")
        )
        .withColumn(
            "discount_amount",
            (F.col("quantity") * F.col("unit_price") * F.col("discount_pct") / F.lit(100)).cast(
                "decimal(18,4)"
            ),
        )
        .withColumn(
            "net_amount", (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(18,4)")
        )
        .withColumn("is_cancelled", F.col("status").isin("cancelled", "returned"))
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    )
    df = add_row_hash(df, [c for c in ORDER_BUSINESS_COLS if c in df.columns])
    return dedupe_by_key(df, ["order_id"], order_by="updated_at")


def conform_customers(df: DataFrame) -> DataFrame:
    """Type and clean the customer master; prepare SCD2 effective dating."""
    from pyspark.sql import functions as F

    df = trim_strings(df)
    df = cast_columns(df, CUSTOMER_CASTS)
    df = (
        df.withColumn("email", F.lower(F.col("email")))
        .withColumn("country", F.upper(F.col("country")))
        .withColumn("segment", F.initcap(F.lower(F.col("segment"))))
        .withColumn(
            "phone", F.regexp_replace(F.coalesce(F.col("phone"), F.lit("")), r"[^0-9+]", "")
        )
        .withColumn("email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1))
    )
    df = add_row_hash(df, [c for c in CUSTOMER_BUSINESS_COLS if c in df.columns])
    df = dedupe_by_key(df, ["customer_id"], order_by="updated_at")
    return df.withColumn("effective_from", F.col("updated_at"))


def conform_products(df: DataFrame) -> DataFrame:
    """Type and clean the product catalogue; derive margin."""
    from pyspark.sql import functions as F

    df = trim_strings(df)
    df = cast_columns(df, PRODUCT_CASTS)
    df = (
        df.withColumn("category", F.initcap(F.lower(F.col("category"))))
        .withColumn("subcategory", F.initcap(F.lower(F.coalesce(F.col("subcategory"), F.lit("")))))
        .withColumn("brand", F.coalesce(F.col("brand"), F.lit("Unbranded")))
        .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True)))
        .withColumn(
            "margin_pct",
            F.when(
                (F.col("list_price").isNotNull()) & (F.col("list_price") > 0),
                ((F.col("list_price") - F.col("unit_cost")) / F.col("list_price") * 100).cast(
                    "decimal(9,4)"
                ),
            ).otherwise(F.lit(None).cast("decimal(9,4)")),
        )
    )
    df = add_row_hash(df, [c for c in PRODUCT_BUSINESS_COLS if c in df.columns])
    return dedupe_by_key(df, ["product_id"], order_by="updated_at")


CONFORMERS = {
    "orders": conform_orders,
    "customers": conform_customers,
    "products": conform_products,
}


def conform(dataset: str, df: DataFrame) -> DataFrame:
    """Dispatch to the dataset's conformer, defaulting to a generic clean-up."""
    fn = CONFORMERS.get(dataset)
    if fn is None:
        return trim_strings(df)
    return fn(df)
