CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS ops.schema_version (
    version INTEGER PRIMARY KEY
) DISTRIBUTED BY (version);
INSERT INTO ops.schema_version
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM ops.schema_version);

CREATE TABLE IF NOT EXISTS ops.publication (
    slot INTEGER PRIMARY KEY CHECK (slot = 1),
    load_id TEXT,
    source_captured_at TIMESTAMPTZ
) DISTRIBUTED BY (slot);
INSERT INTO ops.publication (slot)
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM ops.publication WHERE slot = 1);

CREATE TABLE IF NOT EXISTS ops.loads (
    load_id TEXT PRIMARY KEY,
    curated_id TEXT NOT NULL,
    source_snapshot_id TEXT NOT NULL,
    source_captured_at TIMESTAMPTZ NOT NULL,
    sql_sha256 TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    accepted_rows BIGINT NOT NULL,
    quarantined_rows BIGINT NOT NULL,
    location_rows BIGINT NOT NULL
) DISTRIBUTED BY (load_id);

CREATE TABLE IF NOT EXISTS dwh.location (
    location_id BIGINT PRIMARY KEY,
    location_type TEXT,
    store_name TEXT,
    store_brand TEXT,
    store_city TEXT,
    store_country_code TEXT,
    location_source_updated TEXT
) DISTRIBUTED BY (location_id);

CREATE TABLE IF NOT EXISTS dwh.price_observation (
    observation_id BIGINT PRIMARY KEY,
    source_snapshot_id TEXT NOT NULL,
    product_code TEXT,
    product_name TEXT,
    category_tag TEXT,
    location_id BIGINT,
    price_amount NUMERIC(20,6) NOT NULL CHECK (price_amount >= 0),
    price_before_discount NUMERIC(20,6),
    currency_code VARCHAR(3) NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    price_unit TEXT NOT NULL CHECK (price_unit IN ('UNIT', 'KILOGRAM', 'LITER', 'UNKNOWN')),
    observed_date DATE NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    is_discounted BOOLEAN,
    discount_type TEXT,
    location_match_status TEXT NOT NULL CHECK (location_match_status IN ('MATCHED', 'MISSING', 'UNMATCHED'))
) DISTRIBUTED BY (observation_id);

CREATE TABLE IF NOT EXISTS mart.daily_store_prices (
    observed_date DATE NOT NULL,
    location_id BIGINT,
    currency_code VARCHAR(3) NOT NULL,
    price_unit TEXT NOT NULL,
    observation_count BIGINT NOT NULL,
    distinct_product_count BIGINT NOT NULL,
    discounted_observation_count BIGINT NOT NULL,
    min_observed_price NUMERIC(20,6),
    median_observed_price NUMERIC(20,6),
    max_observed_price NUMERIC(20,6)
) DISTRIBUTED BY (location_id);

CREATE TABLE IF NOT EXISTS mart.daily_product_prices (
    observed_date DATE NOT NULL,
    product_code TEXT NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    price_unit TEXT NOT NULL,
    observation_count BIGINT NOT NULL,
    matched_store_count BIGINT NOT NULL,
    min_observed_price NUMERIC(20,6),
    median_observed_price NUMERIC(20,6),
    max_observed_price NUMERIC(20,6),
    unit_is_known BOOLEAN NOT NULL,
    comparable_store_price_gap NUMERIC(20,6)
) DISTRIBUTED BY (product_code);

CREATE TABLE IF NOT EXISTS mart.source_quality (
    load_id TEXT NOT NULL,
    source_snapshot_id TEXT NOT NULL,
    source_rows BIGINT NOT NULL,
    accepted_rows BIGINT NOT NULL,
    quarantined_rows BIGINT NOT NULL,
    location_rows BIGINT NOT NULL,
    unknown_unit_rows BIGINT NOT NULL,
    missing_location_rows BIGINT NOT NULL,
    unmatched_location_rows BIGINT NOT NULL
) DISTRIBUTED BY (load_id);
