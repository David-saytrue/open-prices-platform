# Open Prices Analytics Platform design

## Intent

Build an independent, English-documented Data Engineering portfolio project
using recent real observations and the Spark / Airflow / Greenplum stack.
The user approved starting implementation and milestone commits/pushes on
2026-11-03. No sales or customer behavior is inferred from price observations.

## Approach

Three source approaches were considered: shop collection, historical clickstream,
and current open price exports. Shop automation was not established for Alta;
the REES46 events cover 2019-2020. Open Prices was selected because it publishes
current observations and explicit reuse terms.

Use a filesystem-backed Data Lake for the first independently runnable milestone.
Keep immutable compressed raw exports and curated Parquet in separate layers.
A later storage adapter may use S3-compatible object storage; no S3 integration
is claimed before testing it. Source exports are independently regenerated and
do not guarantee one transactionally consistent cross-entity snapshot.

## Source contract

- Download only `prices.jsonl.gz` and `locations.jsonl.gz` from the official host.
- Preserve source bytes privately in ignored lake storage; publish no source rows.
- Validate gzip integrity, JSON objects, required source keys and integer IDs.
- Publish a manifest only after both files pass validation.
- Include source URL, SHA-256, byte size, row count and response metadata.
- Identical pairs of files map to the same content-addressed snapshot.
- Treat additive fields as compatible; missing required keys fail ingestion.
- Full export capture avoids an unproven incremental API watermark.
- Capture metadata proves local byte integrity, not source authenticity or atomicity.

## Processing and warehouse

The PySpark job reads only completed, verified manifests. It normalizes
decimal prices, dates and units, and preserves source IDs and update timestamps.
Rows with invalid business values enter a quarantine output. Reconcile accepted
and rejected counts to raw input. Do not silently discard legitimate equal-price
observations or infer stockouts from absence.

Fact grain is one source price observation (`price.id`). Within a source snapshot,
the latest parseable source update for an ID is the candidate. Older/repeated rows
go to quarantine, and conflicting payloads at the same update time stop publication.
An invalid latest correction is quarantined; it must not resurrect an older value.
Every source snapshot is processed separately; no cross-snapshot CDC is claimed.
Dimensions include store/location and product references. Missing product
enrichment stays unknown. Join dimensions at unique keys; count missing
references rather than dropping unmatched facts.

Greenplum receives a complete candidate dataset in transaction-local staging.
SQL marts are built and validated before transactional publication. Mart grains:

- `daily_store_prices`: date, store, currency and price unit.
- `daily_product_prices`: date, product code, currency and price unit.
- `source_quality`: snapshot and entity, with coverage and rejection metrics.

Cross-product averages are descriptive of the observed mix, not an inflation
index. Compare identical product codes and units for store-level price gaps.
Do not sum repeated receipt observations as revenue. Product category aggregation
waits for a verified category-enrichment source.

## Orchestration and operations

One Airflow DAG will capture exports, submit the Spark job, stage warehouse data,
execute mart SQL, validate results and publish. Serialize publication and keep
the previous published version after failure. Replays use stored snapshots;
requesting a past DAG date must not imply reconstruction of unavailable exports.

## Constraints and open work

- Python 3.10+ for ingestion; standard library only for the first milestone.
- No credentials required to read the official bulk exports.
- Initial source has hundreds of thousands of observations, not billions.
- English field names do not establish English product descriptions.
- Native Windows Java is old; Spark runs in a digest-pinned official Linux image.
- Greenplum 7.0.0 build dev is verified in a pinned community local-only image.
- Airflow orchestration remains to be implemented and verified.
- Docker Desktop runtime sockets were recovered; Engine 28.5.1 responded.
