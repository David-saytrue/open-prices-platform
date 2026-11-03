# Open Prices Analytics Platform

A Data Engineering portfolio project using real, regularly updated retail price
observations from [Open Prices](https://prices.openfoodfacts.org/), maintained by
Open Food Facts.

The target architecture combines a Data Lake, PySpark transformations, one
Airflow DAG and SQL marts in Greenplum. It studies observed prices and coverage,
not sales, shopper conversion or market-wide popularity.

## Delivery status

- Design and implementation milestones are documented.
- Source ingestion, immutable snapshots, checksum verification and source profiling
  are implemented using the Python standard library.
- Spark curation writes typed facts, reason-coded quarantine and location Parquet.
- Airflow and Greenplum remain planned; their execution is not yet verified.

See [the design](docs/design.md) and [the implementation plan](docs/implementation-plan.md).

## Run the first milestone

Python 3.10+ is required. Docker, an API key and third-party Python packages are
not needed for source capture. From the repository root:

```powershell
python -m open_prices_platform fetch --lake-root data/lake
```

The command prints the snapshot directory. Use that path in the next commands:

```powershell
python -m open_prices_platform verify data/lake/raw/open_prices/<snapshot-id>
python -m open_prices_platform profile data/lake/raw/open_prices/<snapshot-id> --as-of 2026-11-03
python -m unittest discover -s tests -v
```

Downloads are streamed with size limits, timeouts and bounded retries. The two
official bulk files currently total about 24 MB compressed. Identical downloaded
bytes reuse the existing verified snapshot and return `created: false`.
Only directories containing a complete manifest are ready for downstream work.
The source generates files independently; the manifest does not imply a
transactionally consistent source snapshot. Process termination may leave an
ignored `.capture-*` directory; it must never be treated as a completed snapshot.

The profile reports aggregate anomalies and coverage. It does not clean or
silently remove source records. See [recorded validation](docs/validation.md).

## Run Spark curation

Start Docker Desktop, then run this command from the repository root, replacing
the snapshot placeholder with the path printed by `fetch`:

```powershell
docker compose run --rm spark jobs/curate_prices.py --snapshot data/lake/raw/open_prices/<snapshot-id> --lake-root data/lake
docker compose run --rm spark tests/spark_checks.py
```

Compose uses an official Apache Spark 4.0.1 image pinned by SHA-256, with Python
3.10.12 and Java 17.0.16. It runs one local Spark driver with two worker threads,
not a distributed cluster. The initial image download is much larger than the
source files. The local job runs as root to write bind-mounted development files;
this is not a production container deployment.

The job produces `accepted_prices/`, `quarantine_prices/`, `locations/` and a
completion `manifest.json` under `data/lake/curated/open_prices/<curated-id>/`.
Verify a stored output without Spark using
`python -m open_prices_platform verify-curated data/lake/curated/open_prices/<curated-id>`.
Both facts and quarantine reconcile to the raw row count. Parquet files are
read back to verify row counts and types before publication. File checksums
protect subsequent reuse. Raw and curated data remain excluded from Git.

The output identity includes the source snapshot, transformation code, Spark
version and validation date. The default validation date is the source capture's
UTC date, making replay independent of the wall clock. `--as-of YYYY-MM-DD`
explicitly changes that date and creates a separate output identity.

See [the curation contract](docs/curation.md) for rejection and comparison rules.

## Data and language

Open Prices publishes compressed JSONL exports and a Parquet dataset. The
`prices` and `locations` exports were reachable on 2026-11-03. The documented
`products.jsonl.gz` endpoint returned HTTP 404 on that date, so it is not a
dependency of the first milestone.

Source schemas use English field names. Product and place names are multilingual
and may be missing. We preserve original values and do not label them English
without evidence. All authored project material is in English.

## Licensing

Project code is licensed under [MIT](LICENSE). Source data is **not** covered by
the code license. Open Prices data is available under ODbL 1.0. Attribute
Open Prices / Open Food Facts and comply with the applicable ODbL requirements
when publishing a derived database. Downloaded source rows are excluded from Git.

Sources:

- [Official data access and export documentation](https://github.com/openfoodfacts/open-prices/blob/main/docs/guides/data.md)
- [Official dataset and licensing information](https://huggingface.co/datasets/openfoodfacts/open-prices)
- [Open Database License 1.0](https://opendatacommons.org/licenses/odbl/1-0/)
