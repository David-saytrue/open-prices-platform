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
- Greenplum and the Airflow DAG have not yet been implemented or tested.
- Spark curation is validated below; the full warehouse pipeline is not complete.

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
