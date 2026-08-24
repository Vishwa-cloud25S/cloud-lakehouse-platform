-- Maintenance reference. The `run_maintenance` job issues these automatically;
-- this file documents what runs and why.

USE CATALOG lakehouse_dev;

-- Z-ORDER on the *join and filter* keys, never on the partition column.
-- Z-ordering co-locates related rows so Delta's file statistics can skip most
-- of the table for a selective predicate.
OPTIMIZE gold.fact_orders
  WHERE order_month >= date_format(current_date() - INTERVAL 3 MONTHS, 'yyyy-MM')
  ZORDER BY (customer_key, product_key);

OPTIMIZE gold.dim_customer   ZORDER BY (customer_id);
OPTIMIZE gold.dim_product    ZORDER BY (product_id);
OPTIMIZE gold.agg_daily_sales ZORDER BY (order_date);

OPTIMIZE silver.silver_orders
  WHERE order_date >= current_date() - INTERVAL 7 DAYS
  ZORDER BY (customer_id, order_id);

-- VACUUM reclaims storage from files no longer referenced by the log.
-- 168 hours (7 days) preserves time travel and protects in-flight readers.
VACUUM gold.fact_orders     RETAIN 168 HOURS;
VACUUM gold.dim_customer    RETAIN 168 HOURS;
VACUUM silver.silver_orders RETAIN 168 HOURS;
VACUUM bronze.bronze_orders RETAIN 168 HOURS;

-- Statistics for the cost-based optimizer: drives join order and broadcast choice.
ANALYZE TABLE gold.fact_orders  COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE gold.dim_customer COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE gold.dim_product  COMPUTE STATISTICS FOR ALL COLUMNS;

-- Liquid clustering is the modern alternative to static partitioning +
-- Z-ORDER; it re-clusters incrementally and removes the partition-size guesswork.
-- Migration path for a future version of this project:
--   ALTER TABLE gold.fact_orders CLUSTER BY (order_date, customer_key);
