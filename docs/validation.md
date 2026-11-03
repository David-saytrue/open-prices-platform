# Validation record

## Source ingestion milestone: 2026-11-03

Environment: Windows, CPython 3.13.9. No external Python dependencies.
All 18 offline regression tests passed locally. A real second capture returned
`created: false` and reused the same verified snapshot without changing it.
All four GitHub Actions jobs passed on Windows/Linux with Python 3.11/3.13 for
commit `0f6fe60`. Each job ran all 18 tests and the CLI help check.
[Verified CI run](https://github.com/David-saytrue/open-prices-platform/actions/runs/41626769497).

Live official bulk exports were captured on 2026-11-03. The completed local
snapshot ID is
`0623ea634d9d3cf1e65218bc00170e01d7324de580bc5d96d48b075ccbb3327c`.
Both compressed files passed SHA-256, gzip EOF/CRC and JSONL structure validation.

Aggregate results from the captured files:

- Price observations: 340,038.
- Unique location IDs: 7,781.
- Duplicate source IDs: zero in both files.
- Latest parseable observation date: 2026-11-02.
- Invalid or missing observation dates: 185.
- Invalid, non-finite or negative prices: zero.
- Missing location references: 2; non-null unmatched location references: zero.
- Missing product codes: 10,561.
- Missing top-level product names: 118,819 (not a language classification).
- Source-marked duplicate observations: 9,495; retained in raw capture.
- Observations marked discounted: 34,131.
- Missing or unspecified price unit: 329,477; never assume UNIT without a rule.

The live API included newer records than this daily bulk export. These counts
describe the pinned files, not the API's current total. Old dates also exist in
the source; calling the dataset current does not mean every row is recent.
Original rows remain in ignored local storage. This document contains aggregates.

## Limits

- No automated language classification or product enrichment has run.
- No retailer sales, stock counts, conversion or population coverage is available.
- The Airflow DAG has not yet been implemented or tested.
- Spark and Greenplum are validated below; scheduled orchestration is not complete.

## Local Docker recovery

Docker Desktop 4.48.0 initially failed to create `dockerInference` and then the
Secrets Engine `engine.sock` because stale filesystem reparse points could not
be removed. With Docker processes stopped, the two runtime directories were
renamed to timestamped recovery directories. No volumes, containers, images or
secrets database were deleted. The Secrets Engine directory contained only the
socket. After restart, Docker Desktop reported running and Docker Engine 28.5.1
responded. The inference-disable command had failed before recovery; do not
assume Model Runner was disabled.

## Spark curation milestone: 2026-11-03

Verified runtime: Apache Spark 4.0.1, Python 3.10.12 and Java 17.0.16, using
`apache/spark:4.0.1-python3` pinned to digest
`sha256:acfe7e06e95dd13aa32ee03c464766900d52fba15f58e421f8e1213ec041bb94`.
This is a local two-thread Spark job in Docker, not a multi-node benchmark.

All 23 offline tests passed locally on CPython 3.13.9. The original 18 ingestion
tests also passed on the container's CPython 3.10.12. Five Spark integration tests
passed in the pinned container. These cover corrections, equal-version conflicts,
duplicate IDs, invalid dates/prices/units, NULL references, empty output schemas,
Parquet readback, failed publication, idempotent reuse and artifact tampering.
GitHub Actions now includes the same Spark checks; check its run before claiming
remote success for this milestone.

The real pinned raw snapshot was processed with the final transformation code.
Curated output ID:
`137fec51830e4b7116490e3a95cff274cf349101b6dc71f3213f722855f4763d`.
The standard-library `verify-curated` CLI also verified this container-produced
artifact on the Windows host.

- Input price rows: 340,038.
- Accepted price observations: 330,358.
- Quarantined rows: 9,680. Accepted plus quarantined exactly equals input.
- Locations: 7,781; accepted fact IDs are unique.
- Rejection reasons: 185 invalid dates, 170 invalid/missing currency codes and
  9,495 source-marked duplicates. Reasons overlap; do not sum them as row counts.
- Accepted observations with UNKNOWN price units: 319,938.
- Missing location references: 2; unmatched non-null references: zero.

All three Parquet tables were read back and checked before publication.
The real replay returned `created: false` and reused the same verified output.
Downloaded/curated rows and container logs remain excluded from Git.

## Greenplum warehouse milestone: 2026-11-03

Verified server: `PostgreSQL 12.12 (Greenplum Database 7.0.0 build dev)`, with one
coordinator and two up primary segments. This is a pinned community local image,
not PostgreSQL-only testing or a supported commercial distribution.

All 27 dependency-free tests passed on the Windows host. Five integration tests
passed against actual Greenplum using new, randomly named synthetic databases.
Those tests cover independent currency/unit groups, median/spread grain, NULL
references, literal `\N`, quoted multiline text, duplicate dimension rejection,
older-source protection, idempotent replay, concurrent publication and rollback
after an injected failure following mart construction. The failing transaction
left both prior fact values and the publication pointer unchanged; a subsequent
valid correction succeeded. Temporary test databases were removed by the suite.

A precision regression test uses values near `99999999999999.000001`. Actual
Greenplum reported `double precision` for `PERCENTILE_CONT` on NUMERIC input.
The implementation instead ranks NUMERIC inputs and averages the middle rows,
preserving the expected median `99999999999999.000002` and gap `0.000002`.

The real source and curated snapshots from prior milestones were loaded into
the new project database `open_prices` and reconciled:

- `dwh.price_observation`: 330,358 rows.
- `dwh.location`: 7,781 rows.
- `mart.daily_store_prices`: 21,979 rows.
- `mart.daily_product_prices`: 307,460 rows.
- `mart.source_quality`: one publication row.

The schema/mart/code recipe load ID is
`4c872f6786f71806f9bbca07c0bace2ded821c3db5bfa611bf813218d0d484b2`.
The final real replay returned `created: false` with unchanged fact and mart counts.
GitHub Actions now includes actual Greenplum integration checks. Remote CI success
must be checked on the published commit before reporting it.

Docker Desktop's stale runtime sockets recurred before this milestone. Only
runtime directories were renamed to recovery backups while Docker was stopped;
no volumes were reset. After startup the official Model Runner disable command
succeeded, and `EnableInference` was verified false. A future full Docker
shutdown/restart has not yet been exercised to establish a permanent fix.

## Tableau source preparation: 2026-11-03

The user chose Desktop instead of Public. Desktop installation and activation
are not complete. The official Free Edition form was opened for the user to
provide registration details; no personal details were submitted by the agent.
Direct shell retrieval returned HTTP 403. The winget package was an older 2024
release and was not installed. No installer was found in Downloads at the check.

`sql/003_tableau_views.sql` prepared three BI sources. The sixth actual Greenplum
integration test verifies that joins for store/product labels preserve mart grain
and that the latest available product name does not multiply rows.
All six Greenplum tests passed locally. The three `.tds` connection templates
passed XML validation; this does not establish Desktop compatibility.

A real connection as `open_prices_tableau` verified:

- `transaction_read_only`: on.
- Direct usage permission on schema `mart`: false.
- `bi.store_price_trends`: 21,979 rows.
- `bi.product_price_comparison`: 307,460 rows.
- `bi.pipeline_quality`: 1 row.

No Tableau workbook, live Desktop connection or screenshot of a completed
dashboard has been produced at this preparation milestone.

### Desktop template correction: 2026-11-03

The user subsequently installed Tableau Desktop 2026.3. Opening Pipeline Quality
revealed a missing required `inline` datasource attribute in the hand-authored
XML. All three templates now set `inline='true'`. The corrected Pipeline Quality
template was reopened through Desktop's File Open dialog and reached the
Greenplum Sign In dialog with `open_prices_tableau` prefilled. Authentication and
data retrieval remain pending; this is not yet a verified live BI connection.

### Windows ODBC prerequisite: 2026-11-03

The user submitted the Sign In dialog, after which Desktop reported `A73B8869`
because no PostgreSQL/Greenplum ODBC driver was registered. Installed the official
PostgreSQL `psqlODBC 17.00.0010` x64 MSI after Windows administrator approval.
The installer completed with exit code 0 and registered PostgreSQL Unicode(x64).
A host-side .NET ODBC query through that driver returned user
`open_prices_tableau`, `transaction_read_only=on` and one pipeline-quality row.
The MSI SHA-256 was
`e36628657d5f91968058a560085abd23647d75c7c51ab60f3329ead8c7b640df`.
The installer is not Authenticode-signed and was downloaded directly over HTTPS
from `ftp.postgresql.org`. Installers and local logs remain excluded from Git.
This establishes driver connectivity, not yet a successful Desktop worksheet.

### Live Desktop dashboard: 2026-11-03

After the user completed the empty-password Sign In dialog, Tableau Desktop
2026.3 loaded the Pipeline Quality fields and executed live Greenplum queries.
The UI rendered 330,358 accepted rows and 9,680 quarantined rows. The detailed
worksheet displayed all nine BI numeric measures, excluding Tableau's generated
record count. Quarantine and unknown-unit rates were explicitly formatted as
percentages with two decimal places: 2.85% and 96.85% respectively.

Saved `artifacts/tableau/Pipeline Health.twb` locally, with worksheets Accepted
Rows, Quarantined and Quality Details, and dashboard Pipeline Health. Verified
the actual dashboard in Desktop presentation mode. Its explanatory text states
the denominators, distinguishes price observations from sales and attributes
Open Prices / Open Food Facts. XML readback confirmed three worksheets, the
named dashboard, the live Greenplum reader connection, display aliases and no
extract. The workbook and its aggregate thumbnails remain ignored local files.
Store/product Desktop sources, freshness cards and Airflow orchestration remain
open work. No Tableau publication or workbook distribution was performed.
