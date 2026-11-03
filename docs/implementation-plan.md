# Open Prices Analytics Platform implementation plan

**Goal:** Turn current Open Prices exports into reproducible, validated retail
price marts with Python, a Data Lake, PySpark, Airflow and Greenplum.

**Spec:** [design.md](design.md)

The user requested starting implementation and committing/pushing meaningful
completed milestones. Deliver each stage with evidence and retain unmet stages
as open work. Never substitute a PostgreSQL run for a claimed Greenplum test.

## Milestone 1: Source capture and audit

- [ ] Implement `open_prices_platform/ingest.py`: `capture(lake_root, ...)`
  streams two bounded official exports, validates them and publishes a manifest.
- [ ] Implement `open_prices_platform/profile.py`: `profile_snapshot(path)`
  verifies stored bytes and reports aggregate quality without printing raw rows.
- [ ] Add a module CLI for `fetch`, `verify` and `profile`.
- [ ] Test interrupted capture, malformed/oversized gzip JSONL, missing keys,
  checksum tampering, source duplicates and an identical-file rerun.
- [ ] Capture real exports, verify them, run profiling, and record aggregate results.
- [ ] Run tests and the English/secret/diff review, then commit and push.

## Milestone 2: Spark curated layer

- [ ] Select and pin a supported Spark/Python/Java runtime.
- [ ] Implement a PySpark job with explicit source schema and decimal/date checks.
- [ ] Write accepted Parquet, quarantine Parquet and reconciliation metrics to
  an isolated candidate directory; publish its manifest only after validation.
- [ ] Test duplicate IDs, corrections, invalid price/date/unit, NULL references,
  equal source versions with conflicting payloads, and interrupted publication.
- [ ] Run small synthetic fixtures and the real snapshot; commit and push.

## Milestone 3: Greenplum warehouse and marts

- [ ] Verify an obtainable Greenplum distribution and supported deployment.
- [ ] Create staging, facts, dimensions and explicitly grained mart SQL.
- [ ] Implement staged loading and transactional promotion, serialized per target.
- [ ] Test join fanout, currency/unit separation, NULLs, replay and rollback
  against actual Greenplum. Record untested integrations honestly.
- [ ] Commit and push the independently working warehouse milestone.

## Milestone 4: Airflow orchestration

- [ ] Pin compatible Airflow and Spark-provider versions.
- [ ] Implement one DAG calling capture, Spark, staging, SQL checks and publication.
- [ ] Pass manifest paths rather than rows through XCom.
- [ ] Verify DAG import, retry behavior, stored-snapshot replay and a full run.
- [ ] Add the operations runbook and CI checks; commit and push.

## Review focus

Mixed source export times, missing product metadata, legitimate repeated prices,
unavailable prior-day exports, and rollback after warehouse validation failure
must remain explicit through all milestones.
