-- Tags make the catalog searchable and drive policy.
-- "Find every table containing PII" becomes a query, not an audit meeting.

USE CATALOG lakehouse_dev;

ALTER SCHEMA bronze SET TAGS ('layer' = 'bronze', 'data_quality' = 'raw');
ALTER SCHEMA silver SET TAGS ('layer' = 'silver', 'data_quality' = 'validated');
ALTER SCHEMA gold   SET TAGS ('layer' = 'gold',   'data_quality' = 'certified');

ALTER TABLE silver.silver_customers
  SET TAGS ('domain' = 'crm', 'contains_pii' = 'true', 'classification' = 'confidential');

ALTER TABLE silver.silver_customers ALTER COLUMN email     SET TAGS ('pii' = 'true', 'pii_type' = 'email');
ALTER TABLE silver.silver_customers ALTER COLUMN phone     SET TAGS ('pii' = 'true', 'pii_type' = 'phone');
ALTER TABLE silver.silver_customers ALTER COLUMN full_name SET TAGS ('pii' = 'true', 'pii_type' = 'name');

ALTER TABLE gold.fact_orders
  SET TAGS ('domain' = 'sales', 'certified' = 'true', 'refresh' = 'daily', 'sla_hours' = '6');

ALTER TABLE gold.agg_daily_sales
  SET TAGS ('domain' = 'sales', 'certified' = 'true', 'bi_ready' = 'true');

-- Column documentation surfaces directly in Catalog Explorer and Power BI.
ALTER TABLE gold.fact_orders ALTER COLUMN net_amount
  COMMENT 'Gross amount minus discount. The single agreed definition of revenue.';
ALTER TABLE gold.fact_orders ALTER COLUMN customer_version_key
  COMMENT 'FK to the dim_customer version that was current at order time.';
ALTER TABLE gold.fact_orders ALTER COLUMN gross_profit
  COMMENT 'net_amount minus cogs_amount (quantity x product unit_cost).';

-- ------------------------------------------------------------------ lineage
-- UC captures table and column lineage automatically from query plans.
-- These are the queries you actually run when answering "what breaks if I
-- change this column?"

-- Downstream consumers of a Silver table:
--   SELECT * FROM system.access.table_lineage
--   WHERE source_table_full_name = 'lakehouse_dev.silver.silver_orders'
--     AND event_time >= current_date() - INTERVAL 30 DAYS;

-- Column-level lineage for a revenue figure:
--   SELECT * FROM system.access.column_lineage
--   WHERE target_table_full_name = 'lakehouse_dev.gold.agg_daily_sales'
--     AND target_column_name = 'net_revenue';
