-- Databricks SQL serving layer.
-- Power BI connects to these views, never to the physical tables: the view is the
-- contract, so a Gold refactor does not break every report.

USE CATALOG lakehouse_dev;
CREATE SCHEMA IF NOT EXISTS bi COMMENT 'Curated, BI-facing view layer.';

-- Executive KPI view. Import this into Power BI, not the raw fact table.
CREATE OR REPLACE VIEW bi.v_sales_summary
  COMMENT 'Daily sales KPIs with month-over-month and prior-year comparisons.'
AS
WITH daily AS (
  SELECT
    order_date,
    order_month,
    channel,
    currency,
    order_count,
    unique_customers,
    units_sold,
    gross_revenue,
    total_discount,
    net_revenue,
    gross_profit,
    margin_pct
  FROM gold.agg_daily_sales
)
SELECT
  d.*,
  -- Window functions here rather than in DAX: pushing the calculation into the
  -- warehouse keeps the Power BI model small and the refresh fast.
  sum(d.net_revenue) OVER (
    PARTITION BY d.channel ORDER BY d.order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS net_revenue_7d,
  avg(d.net_revenue) OVER (
    PARTITION BY d.channel ORDER BY d.order_date
    ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
  ) AS net_revenue_28d_avg,
  lag(d.net_revenue, 1) OVER (PARTITION BY d.channel ORDER BY d.order_date)
    AS net_revenue_prev_day
FROM daily d;

-- Star-schema view for Power BI's model: pre-joined, business-named columns.
CREATE OR REPLACE VIEW bi.v_orders_enriched
  COMMENT 'Order lines joined to customer, product and date attributes.'
AS
SELECT
  f.order_id,
  f.order_date,
  f.order_month,
  dd.year        AS order_year,
  dd.quarter     AS order_quarter,
  dd.month_name  AS order_month_name,
  dd.day_name    AS order_day_name,
  dd.is_weekend,
  c.customer_id,
  c.segment      AS customer_segment,
  c.country      AS customer_country,
  c.city         AS customer_city,
  p.product_id,
  p.product_name,
  p.category     AS product_category,
  p.subcategory  AS product_subcategory,
  p.brand        AS product_brand,
  f.channel,
  f.currency,
  f.status,
  f.quantity,
  f.unit_price,
  f.gross_amount,
  f.discount_amount,
  f.net_amount,
  f.cogs_amount,
  f.gross_profit,
  f.is_cancelled
FROM gold.fact_orders f
LEFT JOIN gold.dim_customer c ON f.customer_version_key = c.customer_version_key
LEFT JOIN gold.dim_product  p ON f.product_key          = p.product_key
LEFT JOIN gold.dim_date     dd ON f.date_key            = dd.date_key;

CREATE OR REPLACE VIEW bi.v_customer_360
  COMMENT 'Customer lifetime metrics, RFM status and value tier.'
AS
SELECT
  customer_id,
  full_name,
  email,
  country,
  city,
  segment,
  signup_date,
  lifetime_orders,
  lifetime_value,
  lifetime_profit,
  avg_order_value,
  first_order_date,
  last_order_date,
  recency_days,
  customer_status,
  value_tier,
  CASE
    WHEN lifetime_orders >= 10 AND recency_days <= 30 THEN 'champion'
    WHEN lifetime_orders >= 5  AND recency_days <= 90 THEN 'loyal'
    WHEN lifetime_orders <= 2  AND recency_days <= 30 THEN 'new'
    WHEN recency_days > 180                            THEN 'at_risk'
    ELSE 'regular'
  END AS rfm_segment
FROM gold.agg_customer_360;

-- Product performance with contribution ranking.
CREATE OR REPLACE VIEW bi.v_product_performance
  COMMENT 'Revenue, margin and rank per product over the trailing 90 days.'
AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  p.list_price,
  p.margin_pct AS catalogue_margin_pct,
  count(DISTINCT f.order_id)                       AS order_count,
  sum(f.quantity)                                  AS units_sold,
  sum(f.net_amount)                                AS net_revenue,
  sum(f.gross_profit)                              AS gross_profit,
  CASE WHEN sum(f.net_amount) > 0
       THEN sum(f.gross_profit) / sum(f.net_amount) * 100 END AS realised_margin_pct,
  rank() OVER (ORDER BY sum(f.net_amount) DESC)    AS revenue_rank,
  rank() OVER (PARTITION BY p.category ORDER BY sum(f.net_amount) DESC)
                                                   AS revenue_rank_in_category
FROM gold.fact_orders f
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE f.order_date >= current_date() - INTERVAL 90 DAYS
  AND NOT f.is_cancelled
GROUP BY ALL;
