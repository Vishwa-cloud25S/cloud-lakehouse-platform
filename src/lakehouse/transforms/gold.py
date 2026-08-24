"""Gold: Kimball star schema + serving aggregates for Databricks SQL / Power BI.

Modelling decisions:
  * Facts carry surrogate keys to the dimensions, plus the natural keys for drill-through.
  * `dim_customer` is SCD2, so the fact joins on the version that was current at
    order time - point-in-time correct revenue by segment.
  * Aggregates are partitioned by month and rebuilt with `replaceWhere`, which keeps
    a daily refresh incremental instead of a full-table overwrite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse.transforms.common import add_surrogate_key

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


def build_dim_customer(silver_customers: DataFrame) -> DataFrame:
    """SCD2 customer dimension with surrogate + version keys."""
    from pyspark.sql import functions as F

    df = silver_customers
    if "is_current" not in df.columns:
        df = df.withColumn("is_current", F.lit(True))
    if "effective_to" not in df.columns:
        df = df.withColumn("effective_to", F.lit(None).cast("timestamp"))

    df = add_surrogate_key(df, ["customer_id"], "customer_key")
    # Version key identifies one *row version*, which is what the fact points at.
    df = add_surrogate_key(df, ["customer_id", "effective_from"], "customer_version_key")
    return df.select(
        "customer_version_key",
        "customer_key",
        "customer_id",
        "full_name",
        "email",
        "email_domain",
        "phone",
        "country",
        "city",
        "segment",
        "signup_date",
        "effective_from",
        "effective_to",
        "is_current",
    )


def build_dim_product(silver_products: DataFrame) -> DataFrame:
    """Type-1 product dimension."""
    from pyspark.sql import functions as F

    df = add_surrogate_key(silver_products, ["product_id"], "product_key")
    return df.select(
        "product_key",
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "list_price",
        "unit_cost",
        "margin_pct",
        "is_active",
        F.col("updated_at").alias("last_updated_at"),
    )


def build_dim_date(spark, start: str = "2020-01-01", end: str = "2030-12-31") -> DataFrame:
    """Generated calendar dimension - no source system needed."""
    from pyspark.sql import functions as F

    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date_day"
        )
        .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date_day"))
        .withColumn("quarter", F.quarter("date_day"))
        .withColumn("month", F.month("date_day"))
        .withColumn("month_name", F.date_format("date_day", "MMMM"))
        .withColumn("year_month", F.date_format("date_day", "yyyy-MM"))
        .withColumn("day_of_month", F.dayofmonth("date_day"))
        .withColumn("day_of_week", F.dayofweek("date_day"))
        .withColumn("day_name", F.date_format("date_day", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("date_day"))
        .withColumn("is_weekend", F.dayofweek("date_day").isin(1, 7))
        .select(
            "date_key",
            "date_day",
            "year",
            "quarter",
            "month",
            "month_name",
            "year_month",
            "day_of_month",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend",
        )
    )


def build_fact_orders(
    silver_orders: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Order-line fact joined to the point-in-time-correct dimension versions."""
    from pyspark.sql import functions as F

    orders = silver_orders.alias("o")

    # Point-in-time join: pick the customer version whose validity window contains
    # the order timestamp. Broadcast the dimension - it is orders of magnitude smaller.
    cust = dim_customer.alias("c")
    joined = orders.join(
        F.broadcast(cust),
        (F.col("o.customer_id") == F.col("c.customer_id"))
        & (F.col("c.effective_from") <= F.col("o.updated_at"))
        & (F.col("c.effective_to").isNull() | (F.col("c.effective_to") > F.col("o.updated_at"))),
        "left",
    )

    prod = dim_product.alias("p")
    joined = joined.join(F.broadcast(prod), F.col("o.product_id") == F.col("p.product_id"), "left")

    fact = (
        joined.select(
            F.col("o.order_id").alias("order_id"),
            F.date_format(F.col("o.order_date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.coalesce(F.col("c.customer_version_key"), F.lit(-1)).alias("customer_version_key"),
            F.coalesce(F.col("c.customer_key"), F.lit(-1)).alias("customer_key"),
            F.coalesce(F.col("p.product_key"), F.lit(-1)).alias("product_key"),
            F.col("o.customer_id").alias("customer_id"),
            F.col("o.product_id").alias("product_id"),
            F.col("o.order_date").alias("order_date"),
            F.col("o.order_month").alias("order_month"),
            F.col("o.status").alias("status"),
            F.col("o.channel").alias("channel"),
            F.col("o.currency").alias("currency"),
            F.col("o.quantity").alias("quantity"),
            F.col("o.unit_price").alias("unit_price"),
            F.col("o.gross_amount").alias("gross_amount"),
            F.col("o.discount_amount").alias("discount_amount"),
            F.col("o.net_amount").alias("net_amount"),
            F.col("o.is_cancelled").alias("is_cancelled"),
            (F.col("o.quantity") * F.coalesce(F.col("p.unit_cost"), F.lit(0)))
            .cast("decimal(18,4)")
            .alias("cogs_amount"),
            F.col("o.updated_at").alias("updated_at"),
        )
        .withColumn(
            "gross_profit", (F.col("net_amount") - F.col("cogs_amount")).cast("decimal(18,4)")
        )
        # Late-arriving / unmatched dimension members are visible, not hidden.
        .withColumn("has_unknown_customer", F.col("customer_key") == F.lit(-1))
        .withColumn("has_unknown_product", F.col("product_key") == F.lit(-1))
    )
    return fact


def build_agg_daily_sales(fact_orders: DataFrame) -> DataFrame:
    """Pre-aggregated daily KPIs - the table Power BI actually queries."""
    from pyspark.sql import functions as F

    return (
        fact_orders.where(~F.col("is_cancelled"))
        .groupBy("order_date", "order_month", "channel", "currency")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_key").alias("unique_customers"),
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount").cast("decimal(18,2)").alias("gross_revenue"),
            F.sum("discount_amount").cast("decimal(18,2)").alias("total_discount"),
            F.sum("net_amount").cast("decimal(18,2)").alias("net_revenue"),
            F.sum("gross_profit").cast("decimal(18,2)").alias("gross_profit"),
            F.avg("net_amount").cast("decimal(18,4)").alias("avg_order_value"),
        )
        .withColumn(
            "margin_pct",
            F.when(
                F.col("net_revenue") > 0,
                (F.col("gross_profit") / F.col("net_revenue") * 100).cast("decimal(9,4)"),
            ),
        )
    )


def build_agg_customer_360(fact_orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Customer lifetime metrics + RFM segmentation for the BI layer."""
    from pyspark.sql import functions as F

    current = dim_customer.where(F.col("is_current"))
    metrics = (
        fact_orders.where(~F.col("is_cancelled"))
        .groupBy("customer_key")
        .agg(
            F.countDistinct("order_id").alias("lifetime_orders"),
            F.sum("net_amount").cast("decimal(18,2)").alias("lifetime_value"),
            F.avg("net_amount").cast("decimal(18,4)").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.sum("gross_profit").cast("decimal(18,2)").alias("lifetime_profit"),
        )
        .withColumn("recency_days", F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn("tenure_days", F.datediff(F.col("last_order_date"), F.col("first_order_date")))
    )
    return (
        current.join(metrics, "customer_key", "left")
        .withColumn("lifetime_orders", F.coalesce(F.col("lifetime_orders"), F.lit(0)))
        .withColumn(
            "lifetime_value", F.coalesce(F.col("lifetime_value"), F.lit(0).cast("decimal(18,2)"))
        )
        .withColumn(
            "customer_status",
            F.when(F.col("recency_days").isNull(), "never_ordered")
            .when(F.col("recency_days") <= 30, "active")
            .when(F.col("recency_days") <= 90, "lapsing")
            .when(F.col("recency_days") <= 365, "dormant")
            .otherwise("churned"),
        )
        .withColumn(
            "value_tier",
            F.when(F.col("lifetime_value") >= 10000, "platinum")
            .when(F.col("lifetime_value") >= 5000, "gold")
            .when(F.col("lifetime_value") >= 1000, "silver")
            .otherwise("bronze"),
        )
        .select(
            "customer_key",
            "customer_id",
            "full_name",
            "email",
            "country",
            "city",
            "segment",
            "signup_date",
            "lifetime_orders",
            "lifetime_value",
            "lifetime_profit",
            "avg_order_value",
            "first_order_date",
            "last_order_date",
            "recency_days",
            "tenure_days",
            "customer_status",
            "value_tier",
        )
    )
