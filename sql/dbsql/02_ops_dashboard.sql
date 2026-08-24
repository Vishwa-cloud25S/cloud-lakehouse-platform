-- Operational dashboard queries.
-- The pipeline's own telemetry is Delta, so the ops dashboard is built with the
-- same engine as the business dashboards - no separate metrics stack to operate.

USE CATALOG lakehouse_dev;

-- 1. Pipeline health over the last 7 days.
SELECT
  job_name,
  dataset,
  layer,
  count(*)                                            AS runs,
  sum(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes,
  sum(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) AS failures,
  round(avg(duration_s), 1)                           AS avg_duration_s,
  round(max(duration_s), 1)                           AS max_duration_s,
  sum(rows_written)                                   AS total_rows_written
FROM ops.pipeline_metrics
WHERE started_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL
ORDER BY failures DESC, avg_duration_s DESC;

-- 2. Data quality trend - is the source getting worse?
SELECT
  date_trunc('DAY', checked_at) AS day,
  dataset,
  rule,
  sum(failed_rows)                                       AS failed_rows,
  sum(total_rows)                                        AS total_rows,
  round(1 - sum(failed_rows) / nullif(sum(total_rows), 0), 4) AS pass_rate
FROM ops.dq_results
WHERE checked_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY day DESC, pass_rate ASC;

-- 3. Freshness: which table is late right now?
SELECT
  dataset,
  layer,
  max(finished_at)                                                AS last_success,
  timestampdiff(HOUR, max(finished_at), current_timestamp())      AS hours_since,
  CASE WHEN timestampdiff(HOUR, max(finished_at), current_timestamp()) > 24
       THEN 'STALE' ELSE 'OK' END                                 AS freshness_status
FROM ops.pipeline_metrics
WHERE status = 'success'
GROUP BY ALL
ORDER BY hours_since DESC;

-- 4. Quarantine analysis - what is actually being rejected, and why?
SELECT
  _dataset                       AS dataset,
  explode(_dq_failed_rules)      AS failed_rule,
  count(*)                       AS rows_rejected,
  max(_quarantined_at)           AS most_recent
FROM ops.quarantine
WHERE _quarantined_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL
ORDER BY rows_rejected DESC;

-- 5. Schema evolution history - what changed, and when?
SELECT
  dataset,
  layer,
  version,
  policy,
  diff_json,
  created_at
FROM ops.schema_registry
WHERE created_at >= current_timestamp() - INTERVAL 90 DAYS
ORDER BY created_at DESC;

-- 6. Volume anomaly detection against the trailing median.
WITH daily AS (
  SELECT dataset, layer, date_trunc('DAY', finished_at) AS day, sum(rows_written) AS rows_written
  FROM ops.pipeline_metrics
  WHERE status = 'success' AND finished_at >= current_timestamp() - INTERVAL 30 DAYS
  GROUP BY ALL
),
stats AS (
  SELECT dataset, layer, day, rows_written,
         median(rows_written) OVER (
           PARTITION BY dataset, layer ORDER BY day
           ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
         ) AS median_prior
  FROM daily
)
SELECT *,
       round(abs(rows_written - median_prior) / nullif(median_prior, 0) * 100, 1) AS deviation_pct
FROM stats
WHERE median_prior IS NOT NULL
  AND abs(rows_written - median_prior) / nullif(median_prior, 0) > 0.5
ORDER BY day DESC;

-- 7. Table storage + file-count health (small-file detection).
--    Run DESCRIBE DETAIL per table; anything with numFiles high relative to
--    sizeInBytes needs OPTIMIZE.
-- DESCRIBE DETAIL gold.fact_orders;
