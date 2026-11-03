# Greenplum warehouse runbook

## Local deployment

The project uses actual Greenplum 7.0.0 build dev, not PostgreSQL emulation.
The community image `andruche/greenplum:7` is pinned by digest. Its publicly
inspectable Dockerfile builds archived Greenplum source. This is a legacy local
learning runtime, not a supported Broadcom production distribution. It has one
coordinator and two primary segments in one container, without segment mirrors.

Compose stores the cluster in its own `greenplum-data` named volume. The source
image's development trust authentication is retained. The host port is bound
only to `127.0.0.1:55433`; do not expose this configuration on a public server.
No unrelated Compose services or databases are modified.

```powershell
docker compose up -d --wait greenplum
docker compose build warehouse
docker compose run --rm warehouse init
docker compose run --rm warehouse load --curated data/lake/curated/open_prices/<curated-id> --source-snapshot data/lake/raw/open_prices/<source-id>
```

Use database `open_prices`, user `gpadmin`, host `127.0.0.1`, port `55433` in a
local SQL client. The image allows the local connection without a password.
Avoid inserting source rows manually into the managed schemas.

The loader reads libpq environment variables, not command-line credentials.
Dependencies are pinned PyArrow 22.0.0 and psycopg2-binary 2.9.11 in an isolated
Python 3.11.14 image. No additional host Python installation is required.

## SQL and publication contract

`sql/001_schema.sql` initializes schema version 1. Future schema changes require
an explicit migration; `CREATE TABLE IF NOT EXISTS` is not a schema upgrade.
The target is restricted to `open_prices` or generated test database names.
The connection also checks that the server identifies itself as Greenplum.

- `dwh.price_observation`: one accepted observation ID, distributed by that ID.
- `dwh.location`: one location ID, distributed by that ID.
- `mart.daily_store_prices`: observation date, location reference (including
  NULL), currency and price unit.
- `mart.daily_product_prices`: observation date, nonempty product code,
  currency and price unit.
- `mart.source_quality`: one row describing the current publication.
- `ops.loads`: completed recipe identities, lineage, counts and first publication
  time. This is not a log of failed or repeated attempts.
- `ops.publication`: the currently published load ID and source capture time.

Verify both raw and curated manifests before loading. Stream selected Parquet
columns in 10,000-row batches through coordinator COPY into transaction-local
staging tables. COPY quotes every non-NULL CSV field, distinguishing a missing
value, an empty string and a literal `\N`. No contributor/proof fields are loaded.

One transaction takes an EXCLUSIVE lock on `ops.publication` to serialize writers.
It validates staging counts, uniqueness, source lineage and reference status,
replaces managed fact/dimension/mart rows using transactional DELETE/INSERT,
executes `sql/002_marts.sql`, reconciles mart and quality counts, then switches
the publication pointer. An exception rolls back all these changes. There is no
TRUNCATE, schema drop or independently committed table refresh.

The load identity includes the curated ID and hashes of warehouse code and SQL.
An identical current recipe validates counts and returns `created: false`.
No new load ledger row is created. This is aggregate replay validation, not an
audit of arbitrary manual edits to every value. A different recipe rebuilds marts.
An older source capture cannot replace newer data unless `--allow-older` is
explicitly supplied. Zero accepted facts cannot replace the warehouse.

## Metric interpretation

Store statistics describe the observed assortment, not revenue, demand or an
inflation index. Product statistics are weighted by submitted observations, not
by purchases. NULL product codes are excluded from product marts; inspect source
profiling for missing product-code coverage instead of inventing identities.

Currencies and units remain separate. UNKNOWN units retain descriptive price
statistics but have no asserted comparable store gap. For known units and at
least two matched stores, compare one median per store; a changing price within
one store is not a cross-store spread. The metric is descriptive of the submitted
sample and does not establish complete market coverage.

Medians are computed by ranking NUMERIC values and averaging the middle one/two
rows. Greenplum's `PERCENTILE_CONT` returns double precision for this input, so it
is intentionally avoided. Arithmetic remains NUMERIC and final mart values round
to six decimal places; large values do not silently lose fractional precision.

## Tests and operations

```powershell
docker compose run --rm -e OPEN_PRICES_TEST_DB=1 --entrypoint python warehouse tests/warehouse_checks.py
docker compose stop greenplum
```

Integration tests create randomly named synthetic `open_prices_test_<id>`
databases and remove only the exact databases they created. Tests cover mart
grain, currencies/units, NULLs, CSV escaping, replay, source-age protection,
dimension duplicates, concurrent publication, decimal precision and rollback
after an injected post-mart validation failure. They never reset `open_prices`.

Stopping preserves the named volume. Do not run `docker compose down -v` as
routine cleanup. Raw and curated history remains in the local lake; the warehouse
holds the current full snapshot, not all observation versions across captures.

## Image provenance

- [Community image build source](https://github.com/andruche/docker-greenplum/blob/master/7/DockerFile)
- [Community image entrypoint](https://github.com/andruche/docker-greenplum/blob/master/entrypoint.sh)
- [Archived Greenplum source used by the image](https://github.com/greenplum-db/gpdb-archive/tree/0a7a3566873325aca1789ae6f818c80f17a9402d)
