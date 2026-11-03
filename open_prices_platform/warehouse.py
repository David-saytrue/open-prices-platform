"""Load verified local Parquet into actual Greenplum and publish SQL marts."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql
import pyarrow.parquet as pq

from .curated import verify_curated
from .ingest import CaptureError, verify_snapshot
from .warehouse_io import copy_csv, validate_database

ROOT = Path(__file__).resolve().parent.parent
FACT_COLUMNS = (
    "observation_id", "source_snapshot_id", "product_code", "product_name", "category_tag",
    "location_id", "price_amount", "price_before_discount", "currency_code", "price_unit",
    "observed_date", "source_updated_at", "is_discounted", "discount_type", "location_match_status",
)
LOCATION_COLUMNS = (
    "location_id", "location_type", "store_name", "store_brand", "store_city",
    "store_country_code", "location_source_updated",
)


def connect(database: str):
    connection = psycopg2.connect(dbname=database, connect_timeout=15,
                                 application_name="open-prices-warehouse")
    with connection.cursor() as cursor:
        cursor.execute("SELECT version()")
        if "Greenplum Database" not in cursor.fetchone()[0]:
            connection.close()
            raise CaptureError("Actual Greenplum is required; PostgreSQL is not a substitute")
    connection.commit()
    return connection


def initialize(database: str) -> None:
    validate_database(database)
    admin = connect("postgres")
    try:
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if not cursor.fetchone():
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    finally:
        admin.close()
    connection = connect(database)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute((ROOT / "sql/001_schema.sql").read_text(encoding="utf-8"))
            cursor.execute("SELECT version FROM ops.schema_version")
            if cursor.fetchall() != [(1,)]:
                raise CaptureError("Unsupported warehouse schema version")
    finally:
        connection.close()


def _copy_parquet(cursor, directory: Path, table: str, columns: tuple[str, ...]) -> None:
    paths = sorted(directory.glob("*.parquet"))
    if not paths:
        raise CaptureError(f"Missing Parquet files in {directory.name}")
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns)))
    for path in paths:
        with pq.ParquetFile(path) as parquet:
            for batch in parquet.iter_batches(batch_size=10_000, columns=list(columns)):
                cursor.copy_expert(statement.as_string(cursor), io.StringIO(copy_csv(batch.to_pylist(), columns)))


def _scalar(cursor, query: str, parameters=None):
    cursor.execute(query, parameters)
    return cursor.fetchone()[0]


def _validate_stage(cursor, metrics: dict, source_id: str):
    if _scalar(cursor, "SELECT COUNT(*) FROM stage_prices") != metrics["accepted_prices"]:
        raise CaptureError("Staging price count mismatch")
    if _scalar(cursor, "SELECT COUNT(*) FROM stage_locations") != metrics["locations"]:
        raise CaptureError("Staging location count mismatch")
    if _scalar(cursor, "SELECT COUNT(*) FROM (SELECT observation_id FROM stage_prices GROUP BY 1 HAVING COUNT(*) > 1) q"):
        raise CaptureError("Duplicate staging fact IDs")
    if _scalar(cursor, "SELECT COUNT(*) FROM (SELECT location_id FROM stage_locations GROUP BY 1 HAVING COUNT(*) > 1) q"):
        raise CaptureError("Duplicate staging dimension IDs")
    if _scalar(cursor, "SELECT COUNT(*) FROM stage_prices WHERE source_snapshot_id IS DISTINCT FROM %s", (source_id,)):
        raise CaptureError("Price rows do not match source snapshot lineage")
    if _scalar(cursor, """
        SELECT COUNT(*) FROM stage_prices f LEFT JOIN stage_locations d USING (location_id)
        WHERE f.location_match_status IS DISTINCT FROM
          CASE WHEN f.location_id IS NULL THEN 'MISSING'
               WHEN d.location_id IS NULL THEN 'UNMATCHED' ELSE 'MATCHED' END
        """):
        raise CaptureError("Staging location reference status mismatch")


def _validate_publication(cursor, metrics: dict) -> dict:
    counts = {name: _scalar(cursor, "SELECT COUNT(*) FROM " + table) for name, table in (
        ("accepted_prices", "dwh.price_observation"), ("locations", "dwh.location"),
        ("daily_store_rows", "mart.daily_store_prices"), ("daily_product_rows", "mart.daily_product_prices"))}
    if any(counts[key] != metrics[key] for key in ("accepted_prices", "locations")):
        raise CaptureError("Published warehouse row count mismatch")
    if _scalar(cursor, "SELECT COALESCE(SUM(observation_count),0) FROM mart.daily_store_prices") != metrics["accepted_prices"]:
        raise CaptureError("Store mart does not reconcile to fact grain")
    expected_products = _scalar(cursor, "SELECT COUNT(*) FROM dwh.price_observation WHERE product_code IS NOT NULL AND BTRIM(product_code) <> ''")
    if _scalar(cursor, "SELECT COALESCE(SUM(observation_count),0) FROM mart.daily_product_prices") != expected_products:
        raise CaptureError("Product mart does not reconcile to fact grain")
    if _scalar(cursor, "SELECT COUNT(*) FROM mart.source_quality") != 1:
        raise CaptureError("Quality mart must contain exactly one publication")
    for name, condition in (
        ("unknown_price_units", "price_unit='UNKNOWN'"),
        ("missing_locations", "location_match_status='MISSING'"),
        ("unmatched_locations", "location_match_status='UNMATCHED'"),
    ):
        if _scalar(cursor, "SELECT COUNT(*) FROM dwh.price_observation WHERE " + condition) != metrics[name]:
            raise CaptureError(f"Published quality count mismatch: {name}")
    return counts


def load_curated(curated: Path, source_snapshot: Path, database: str,
                 *, allow_older: bool = False) -> dict:
    validate_database(database)
    manifest, source = verify_curated(curated), verify_snapshot(source_snapshot)
    if manifest["recipe"]["source_snapshot_id"] != source["snapshot_id"]:
        raise CaptureError("Raw and curated snapshots do not match")
    metrics = manifest["metrics"]
    if metrics["accepted_prices"] <= 0:
        raise CaptureError("Refusing to replace the warehouse with zero accepted facts")
    expected_raw = next(entry["rows"] for entry in source["files"] if entry["entity"] == "prices")
    if metrics["source_prices"] != expected_raw:
        raise CaptureError("Raw and curated price counts do not match")
    dependency_paths = (ROOT / "sql/001_schema.sql", ROOT / "sql/002_marts.sql",
                        Path(__file__), Path(__file__).with_name("warehouse_io.py"))
    sql_hash = hashlib.sha256(b"\n".join(path.read_bytes() for path in dependency_paths)).hexdigest()
    load_id = hashlib.sha256((manifest["curated_id"] + sql_hash).encode()).hexdigest()
    connection = connect(database)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'; SET LOCAL statement_timeout = '180s'; SET LOCAL lock_timeout = '30s'")
            cursor.execute("LOCK TABLE ops.publication IN EXCLUSIVE MODE")
            cursor.execute("SELECT load_id, source_captured_at FROM ops.publication WHERE slot = 1")
            publication = cursor.fetchone()
            if publication is None:
                raise CaptureError("Warehouse is not initialized")
            if publication[0] == load_id:
                return {"load_id": load_id, "created": False, "counts": _validate_publication(cursor, metrics)}
            if publication[1] and not allow_older and _scalar(cursor,
                "SELECT %s::timestamptz < %s::timestamptz", (source["capture_completed_at"], publication[1])):
                raise CaptureError("Older source capture would replace newer data; use --allow-older explicitly")
            cursor.execute("CREATE TEMP TABLE stage_prices (LIKE dwh.price_observation) ON COMMIT DROP DISTRIBUTED BY (observation_id)")
            cursor.execute("CREATE TEMP TABLE stage_locations (LIKE dwh.location) ON COMMIT DROP DISTRIBUTED BY (location_id)")
            _copy_parquet(cursor, curated / "accepted_prices", "stage_prices", FACT_COLUMNS)
            _copy_parquet(cursor, curated / "locations", "stage_locations", LOCATION_COLUMNS)
            _validate_stage(cursor, metrics, source["snapshot_id"])
            # DELETE is transactional and MVCC-safe; no TRUNCATE or schema replacement.
            cursor.execute("DELETE FROM mart.daily_product_prices; DELETE FROM mart.daily_store_prices; DELETE FROM mart.source_quality; DELETE FROM dwh.price_observation; DELETE FROM dwh.location")
            cursor.execute("INSERT INTO dwh.location SELECT * FROM stage_locations")
            cursor.execute("INSERT INTO dwh.price_observation SELECT * FROM stage_prices")
            cursor.execute((ROOT / "sql/002_marts.sql").read_text(encoding="utf-8"))
            cursor.execute("INSERT INTO mart.source_quality VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                load_id, source["snapshot_id"], metrics["source_prices"], metrics["accepted_prices"],
                metrics["quarantined_prices"], metrics["locations"], metrics["unknown_price_units"],
                metrics["missing_locations"], metrics["unmatched_locations"]))
            counts = _validate_publication(cursor, metrics)
            cursor.execute("""INSERT INTO ops.loads
                (load_id,curated_id,source_snapshot_id,source_captured_at,sql_sha256,accepted_rows,quarantined_rows,location_rows)
                SELECT %s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT 1 FROM ops.loads WHERE load_id=%s)""", (
                    load_id, manifest["curated_id"], source["snapshot_id"], source["capture_completed_at"], sql_hash,
                    metrics["accepted_prices"], metrics["quarantined_prices"], metrics["locations"], load_id))
            cursor.execute("UPDATE ops.publication SET load_id=%s, source_captured_at=%s WHERE slot=1",
                           (load_id, source["capture_completed_at"]))
            return {"load_id": load_id, "created": True, "counts": counts}
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "load"))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "open_prices"))
    parser.add_argument("--curated", type=Path)
    parser.add_argument("--source-snapshot", type=Path)
    parser.add_argument("--allow-older", action="store_true")
    args = parser.parse_args()
    if args.command == "init":
        initialize(args.database)
        print(json.dumps({"database": args.database, "initialized": True}))
    else:
        if not args.curated or not args.source_snapshot:
            parser.error("load requires --curated and --source-snapshot")
        print(json.dumps(load_curated(args.curated, args.source_snapshot, args.database,
                                      allow_older=args.allow_older), indent=2))


if __name__ == "__main__":
    main()
