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
- Spark, Airflow and Greenplum are planned; their execution is not yet verified.

See [the design](docs/design.md) and [the implementation plan](docs/implementation-plan.md).

## Run the first milestone

Python 3.11+ is required. Docker, an API key and third-party Python packages are
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
