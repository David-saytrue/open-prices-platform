-- Grain: observation date, store reference (including NULL), currency and unit.
-- Ranked NUMERIC averages preserve precision: PERCENTILE_CONT returns double.
INSERT INTO mart.daily_store_prices
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER w AS position,
           COUNT(*) OVER (PARTITION BY observed_date, location_id, currency_code, price_unit) AS group_size
    FROM dwh.price_observation
    WINDOW w AS (PARTITION BY observed_date, location_id, currency_code, price_unit
                 ORDER BY price_amount, observation_id)
)
SELECT observed_date, location_id, currency_code, price_unit,
       COUNT(*), COUNT(DISTINCT NULLIF(BTRIM(product_code), '')),
       COUNT(*) FILTER (WHERE is_discounted IS TRUE),
       MIN(price_amount),
       AVG(price_amount) FILTER (WHERE position IN ((group_size+1)/2, (group_size+2)/2)),
       MAX(price_amount)
FROM ranked
GROUP BY observed_date, location_id, currency_code, price_unit;

-- Grain: observation date, product code, currency and unit.
-- First compute one median per matched store to avoid calling a within-store
-- price change a cross-store gap. UNKNOWN units retain descriptive statistics
-- but do not yield an asserted comparable price gap.
INSERT INTO mart.daily_product_prices
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER w AS position,
           COUNT(*) OVER (PARTITION BY observed_date, product_code, currency_code, price_unit) AS group_size
    FROM dwh.price_observation
    WHERE product_code IS NOT NULL AND BTRIM(product_code) <> ''
    WINDOW w AS (PARTITION BY observed_date, product_code, currency_code, price_unit
                 ORDER BY price_amount, observation_id)
), store_ranked AS (
    SELECT *, ROW_NUMBER() OVER w AS position,
           COUNT(*) OVER (PARTITION BY observed_date, product_code, currency_code, price_unit, location_id) AS group_size
    FROM dwh.price_observation
    WHERE product_code IS NOT NULL AND BTRIM(product_code) <> '' AND location_match_status = 'MATCHED'
    WINDOW w AS (PARTITION BY observed_date, product_code, currency_code, price_unit, location_id
                 ORDER BY price_amount, observation_id)
), store_prices AS (
    SELECT observed_date, product_code, currency_code, price_unit, location_id,
           AVG(price_amount) FILTER (WHERE position IN ((group_size+1)/2, (group_size+2)/2)) AS store_median
    FROM store_ranked
    GROUP BY observed_date, product_code, currency_code, price_unit, location_id
), store_spreads AS (
    SELECT observed_date, product_code, currency_code, price_unit,
           COUNT(*) AS store_count, MAX(store_median) - MIN(store_median) AS store_gap
    FROM store_prices
    GROUP BY observed_date, product_code, currency_code, price_unit
), product_prices AS (
    SELECT observed_date, product_code, currency_code, price_unit,
           COUNT(*) AS observation_count, MIN(price_amount) AS min_price,
           AVG(price_amount) FILTER (WHERE position IN ((group_size+1)/2, (group_size+2)/2)) AS median_price,
           MAX(price_amount) AS max_price
    FROM ranked
    GROUP BY observed_date, product_code, currency_code, price_unit
)
SELECT p.observed_date, p.product_code, p.currency_code, p.price_unit,
       p.observation_count, COALESCE(s.store_count, 0),
       p.min_price, p.median_price, p.max_price,
       p.price_unit <> 'UNKNOWN',
       CASE WHEN p.price_unit <> 'UNKNOWN' AND s.store_count >= 2 THEN s.store_gap END
FROM product_prices p
LEFT JOIN store_spreads s
  ON p.observed_date = s.observed_date AND p.product_code = s.product_code
 AND p.currency_code = s.currency_code AND p.price_unit = s.price_unit;
