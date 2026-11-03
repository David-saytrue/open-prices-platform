-- Separate BI sources preserve the different mart grains.
CREATE SCHEMA IF NOT EXISTS bi;

CREATE OR REPLACE VIEW bi.store_price_trends AS
SELECT m.observed_date, m.location_id,
       CASE WHEN m.location_id IS NULL THEN 'Missing store reference'
            WHEN d.location_id IS NULL THEN 'Unmatched store #' || m.location_id::text
            ELSE COALESCE(NULLIF(d.store_name, ''), 'Store #' || m.location_id::text) END AS store_label,
       d.store_brand, d.store_city, d.store_country_code,
       m.currency_code, m.price_unit, m.price_unit <> 'UNKNOWN' AS unit_is_known,
       m.observation_count, m.distinct_product_count, m.discounted_observation_count,
       m.discounted_observation_count::numeric / NULLIF(m.observation_count, 0) AS discounted_share,
       m.min_observed_price, m.median_observed_price, m.max_observed_price
FROM mart.daily_store_prices m
LEFT JOIN dwh.location d ON m.location_id = d.location_id;

CREATE OR REPLACE VIEW bi.product_price_comparison AS
WITH named_products AS (
    SELECT product_code, product_name,
           ROW_NUMBER() OVER (PARTITION BY product_code ORDER BY source_updated_at DESC, observation_id DESC) AS position
    FROM dwh.price_observation
    WHERE product_code IS NOT NULL AND NULLIF(BTRIM(product_name), '') IS NOT NULL
)
SELECT m.observed_date, m.product_code,
       COALESCE(n.product_name, 'Product #' || m.product_code) AS product_label,
       m.currency_code, m.price_unit, m.unit_is_known, m.observation_count,
       m.matched_store_count, m.min_observed_price, m.median_observed_price,
       m.max_observed_price, m.comparable_store_price_gap
FROM mart.daily_product_prices m
LEFT JOIN named_products n ON m.product_code = n.product_code AND n.position = 1;

CREATE OR REPLACE VIEW bi.pipeline_quality AS
SELECT q.load_id, q.source_snapshot_id, l.curated_id,
       l.source_captured_at, l.published_at,
       q.source_rows, q.accepted_rows, q.quarantined_rows, q.location_rows,
       q.unknown_unit_rows, q.missing_location_rows, q.unmatched_location_rows,
       q.quarantined_rows::numeric / NULLIF(q.source_rows, 0) AS quarantine_rate,
       q.unknown_unit_rows::numeric / NULLIF(q.accepted_rows, 0) AS unknown_unit_rate
FROM mart.source_quality q
JOIN ops.loads l ON q.load_id = l.load_id;
