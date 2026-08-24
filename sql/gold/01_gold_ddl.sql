-- Gold layer DDL, expressed declaratively.
-- The Python jobs create these tables automatically; this file is the reviewable
-- contract and the reference for anyone reading the model.

USE CATALOG lakehouse_dev;

CREATE TABLE IF NOT EXISTS gold.dim_customer (
  customer_version_key BIGINT    COMMENT 'Surrogate key of this row version (SCD2).',
  customer_key         BIGINT    COMMENT 'Stable surrogate key of the customer.',
  customer_id          STRING    COMMENT 'Business key from the CRM source.',
  full_name            STRING,
  email                STRING,
  email_domain         STRING,
  phone                STRING,
  country              STRING,
  city                 STRING,
  segment              STRING,
  signup_date          DATE,
  effective_from       TIMESTAMP COMMENT 'Inclusive start of this version.',
  effective_to         TIMESTAMP COMMENT 'Exclusive end; NULL when current.',
  is_current           BOOLEAN   COMMENT 'TRUE for the active version.'
)
USING DELTA
COMMENT 'Type-2 customer dimension enabling point-in-time correct joins.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.columnMapping.mode'   = 'name',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

CREATE TABLE IF NOT EXISTS gold.dim_product (
  product_key     BIGINT,
  product_id      STRING,
  product_name    STRING,
  category        STRING,
  subcategory     STRING,
  brand           STRING,
  list_price      DECIMAL(18,4),
  unit_cost       DECIMAL(18,4),
  margin_pct      DECIMAL(9,4),
  is_active       BOOLEAN,
  last_updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Type-1 product dimension.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS gold.fact_orders (
  order_id             STRING,
  date_key             INT,
  customer_version_key BIGINT,
  customer_key         BIGINT,
  product_key          BIGINT,
  customer_id          STRING,
  product_id           STRING,
  order_date           DATE,
  order_month          STRING,
  status               STRING,
  channel              STRING,
  currency             STRING,
  quantity             INT,
  unit_price           DECIMAL(18,4),
  gross_amount         DECIMAL(18,4),
  discount_amount      DECIMAL(18,4),
  net_amount           DECIMAL(18,4) COMMENT 'The single agreed definition of revenue.',
  cogs_amount          DECIMAL(18,4),
  gross_profit         DECIMAL(18,4),
  is_cancelled         BOOLEAN,
  has_unknown_customer BOOLEAN,
  has_unknown_product  BOOLEAN,
  updated_at           TIMESTAMP
)
USING DELTA
-- Monthly partitions: ~30x fewer directories than daily, and BI filters are
-- almost always month- or quarter-grained. Daily partitioning on this volume
-- would produce files far below the 1 GB target.
PARTITIONED BY (order_month)
COMMENT 'Order-line grain fact table.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

CREATE TABLE IF NOT EXISTS gold.agg_daily_sales (
  order_date       DATE,
  order_month      STRING,
  channel          STRING,
  currency         STRING,
  order_count      BIGINT,
  unique_customers BIGINT,
  units_sold       BIGINT,
  gross_revenue    DECIMAL(18,2),
  total_discount   DECIMAL(18,2),
  net_revenue      DECIMAL(18,2),
  gross_profit     DECIMAL(18,2),
  avg_order_value  DECIMAL(18,4),
  margin_pct       DECIMAL(9,4)
)
USING DELTA
PARTITIONED BY (order_month)
COMMENT 'Pre-aggregated daily KPIs; the table Power BI actually queries.';
