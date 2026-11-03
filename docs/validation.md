# Validation record

## Source ingestion milestone: 2026-11-03

Environment: Windows, CPython 3.13.9. No external Python dependencies.
All 18 offline regression tests passed locally. A real second capture returned
`created: false` and reused the same verified snapshot without changing it.
Offline tests are configured in GitHub Actions on Windows/Linux with Python 3.11/3.13.
The CI matrix is a declared check, not a claim of success before its runs finish.

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
- Spark, Greenplum and the Airflow DAG have not yet been implemented or tested.
- This validates the capture stage, not the complete target pipeline.

## Local Docker recovery

Docker Desktop 4.48.0 initially failed to create `dockerInference` and then the
Secrets Engine `engine.sock` because stale filesystem reparse points could not
be removed. With Docker processes stopped, the two runtime directories were
renamed to timestamped recovery directories. No volumes, containers, images or
secrets database were deleted. The Secrets Engine directory contained only the
socket. After restart, Docker Desktop reported running and Docker Engine 28.5.1
responded. The inference-disable command had failed before recovery; do not
assume Model Runner was disabled.
