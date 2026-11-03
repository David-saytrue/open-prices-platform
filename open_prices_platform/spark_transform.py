"""Spark 4.0.1 transformations of a complete, immutable source snapshot."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

from .curated import artifact_inventory, recipe_id, verify_curated
from .ingest import CaptureError, utc_now, verify_snapshot

PRICE_FIELDS = (
    "id", "product_code", "product_name", "category_tag", "location_id", "type",
    "price", "currency", "date", "updated", "price_per", "price_is_discounted",
    "price_without_discount", "discount_type", "duplicate_of",
)
LOCATION_FIELDS = (
    "id", "type", "osm_name", "osm_brand", "osm_address_city",
    "osm_address_country_code", "updated",
)


def read_export(spark: SparkSession, path: Path, fields: tuple[str, ...]) -> DataFrame:
    schema = T.StructType([T.StructField(name, T.StringType(), True) for name in fields])
    return (spark.read.text(str(path)).withColumnRenamed("value", "_raw_json")
            .withColumn("_record", F.from_json("_raw_json", schema))
            .select("_raw_json", "_record.*"))


def normalize_locations(raw: DataFrame) -> DataFrame:
    result = raw.select(
        F.expr("try_cast(id as bigint)").alias("location_id"),
        F.col("type").alias("location_type"), F.col("osm_name").alias("store_name"),
        F.col("osm_brand").alias("store_brand"),
        F.col("osm_address_city").alias("store_city"),
        F.upper(F.trim("osm_address_country_code")).alias("store_country_code"),
        F.col("updated").alias("location_source_updated"),
    )
    if result.filter(F.col("location_id").isNull() | (F.col("location_id") <= 0)).limit(1).count():
        raise CaptureError("Invalid location ID; dimension publication stopped")
    if result.groupBy("location_id").count().filter("count > 1").limit(1).count():
        raise CaptureError("Duplicate location IDs would multiply fact rows")
    return result


def classify_prices(raw: DataFrame, locations: DataFrame, as_of: date,
                    source_snapshot_id: str) -> tuple[DataFrame, DataFrame]:
    """Retain all input rows in either accepted facts or reason-coded quarantine."""
    normalized = (raw
        .withColumn("_row_hash", F.sha2("_raw_json", 256))
        .withColumn("observation_id", F.expr("try_cast(id as bigint)"))
        .withColumn("source_updated_at", F.try_to_timestamp("updated"))
        .withColumn("observed_date", F.expr("try_cast(date as date)"))
        .withColumn("price_amount", F.expr("try_cast(price as decimal(20,6))"))
        .withColumn("price_before_discount", F.expr("try_cast(price_without_discount as decimal(20,6))"))
        .withColumn("currency_code", F.upper(F.trim("currency")))
        .withColumn("price_unit", F.coalesce(F.upper(F.trim("price_per")), F.lit("UNKNOWN")))
        .withColumn("_location_id", F.expr("try_cast(location_id as bigint)"))
        .withColumn("is_discounted", F.expr("try_cast(price_is_discounted as boolean)")))

    # Reject ambiguous source versions rather than arbitrarily choosing a payload.
    conflicts = (normalized.filter("observation_id is not null and source_updated_at is not null")
                 .groupBy("observation_id", "source_updated_at")
                 .agg(F.countDistinct("_row_hash").alias("payloads"))
                 .filter("payloads > 1"))
    if conflicts.limit(1).count():
        raise CaptureError("Conflicting payloads for the same observation ID and update time")
    versions = Window.partitionBy("observation_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("_row_hash"))
    normalized = normalized.withColumn("_version_rank", F.row_number().over(versions))
    reasons = [
        (F.col("observation_id").isNull() | (F.col("observation_id") <= 0), "INVALID_ID"),
        (F.col("source_updated_at").isNull(), "INVALID_SOURCE_UPDATED"),
        (F.col("observed_date").isNull() | ~F.col("date").rlike(r"^\d{4}-\d{2}-\d{2}$"), "INVALID_DATE"),
        (F.col("observed_date") > F.lit(as_of), "FUTURE_DATE"),
        (F.col("price_amount").isNull() | ~F.col("price").rlike(r"^\d{1,14}(\.\d{1,6})?$"), "INVALID_PRICE"),
        (F.col("currency_code").isNull() | ~F.col("currency_code").rlike("^[A-Z]{3}$"), "INVALID_CURRENCY"),
        (~F.col("price_unit").isin("UNIT", "KILOGRAM", "LITER", "UNKNOWN"), "INVALID_UNIT"),
        (F.col("price_is_discounted").isNotNull() &
         ~F.lower("price_is_discounted").isin("true", "false"), "INVALID_DISCOUNT_FLAG"),
        (F.col("price_without_discount").isNotNull() &
         (F.col("price_before_discount").isNull() |
          ~F.col("price_without_discount").rlike(r"^\d{1,14}(\.\d{1,6})?$")), "INVALID_ORIGINAL_PRICE"),
        ((F.col("is_discounted") == True) &
         (F.col("price_before_discount") < F.col("price_amount")), "INVALID_DISCOUNT_RANGE"),
        (F.col("location_id").isNotNull() &
         (F.col("_location_id").isNull() | (F.col("_location_id") <= 0)), "INVALID_LOCATION_ID"),
        (F.col("duplicate_of").isNotNull(), "SOURCE_MARKED_DUPLICATE"),
        (F.col("_version_rank") > 1, "SUPERSEDED_OR_DUPLICATE_ID"),
    ]
    normalized = normalized.withColumn("rejection_reasons", F.filter(
        F.array(*[F.when(condition, F.lit(reason)) for condition, reason in reasons]),
        lambda value: value.isNotNull()))
    # Every row carries snapshot lineage, but contributor/receipt fields are excluded.
    normalized = normalized.withColumn("source_snapshot_id", F.lit(source_snapshot_id))
    quarantine = normalized.filter(F.size("rejection_reasons") > 0).select(
        "source_snapshot_id", *PRICE_FIELDS, "_row_hash", "rejection_reasons")
    facts = normalized.filter(F.size("rejection_reasons") == 0).select(
        "source_snapshot_id", "observation_id", "product_code", "product_name", "category_tag",
        "price_amount", "price_before_discount", "currency_code", "price_unit", "observed_date",
        "source_updated_at", "is_discounted", "discount_type", F.col("_location_id").alias("location_id"))
    dimension = locations.withColumn("_location_matched", F.lit(True))
    facts = (facts.join(dimension, "location_id", "left")
             .withColumn("location_match_status", F.when(F.col("location_id").isNull(), "MISSING")
                         .when(F.col("_location_matched").isNull(), "UNMATCHED").otherwise("MATCHED"))
             .drop("_location_matched"))
    return facts, quarantine


def _write_tables(tables: dict[str, DataFrame], stage: Path) -> dict:
    counts = {}
    for name, frame in tables.items():
        frame.write.mode("errorifexists").parquet(str(stage / name))
        # Read what was actually written, rather than trusting pre-write counts alone.
        stored = frame.sparkSession.read.parquet(str(stage / name))
        counts[name] = stored.count()
        if stored.schema != frame.schema:
            # Parquet readers make nullable flags permissive; compare physical types.
            if [(f.name, f.dataType) for f in stored.schema] != [(f.name, f.dataType) for f in frame.schema]:
                raise CaptureError(f"Parquet schema mismatch: {name}")
    return counts


def transform_snapshot(spark: SparkSession, snapshot: Path, lake_root: Path,
                       *, as_of: date | None = None) -> tuple[Path, bool]:
    source = verify_snapshot(snapshot)
    as_of = as_of or datetime.fromisoformat(source["capture_completed_at"]).date()
    code_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    recipe = {"source_snapshot_id": source["snapshot_id"], "as_of_date": as_of.isoformat(),
              "contract_version": 1, "transform_sha256": code_hash, "spark_version": spark.version}
    identity = recipe_id(recipe)
    parent = Path(lake_root).resolve() / "curated" / "open_prices"
    parent.mkdir(parents=True, exist_ok=True)
    destination = parent / identity
    if destination.exists():
        verify_curated(destination)
        return destination, False
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    prices = read_export(spark, snapshot / "prices.jsonl.gz", PRICE_FIELDS)
    raw_locations = read_export(spark, snapshot / "locations.jsonl.gz", LOCATION_FIELDS)
    locations = normalize_locations(raw_locations).cache()
    accepted = rejected = None
    try:
        accepted, rejected = classify_prices(prices, locations, as_of, source["snapshot_id"])
        accepted, rejected = accepted.cache(), rejected.cache()
        source_count = next(entry["rows"] for entry in source["files"] if entry["entity"] == "prices")
        accepted_count, rejected_count = accepted.count(), rejected.count()
        if accepted_count + rejected_count != source_count:
            raise CaptureError("Source rows do not reconcile with accepted and quarantine")
        if accepted.select("observation_id").distinct().count() != accepted_count:
            raise CaptureError("Accepted fact IDs are not unique")
        rejection_counts = {
            row["reason"]: row["count"] for row in
            rejected.select(F.explode("rejection_reasons").alias("reason")).groupBy("reason").count().collect()
        }
        metrics = {"source_prices": source_count, "accepted_prices": accepted_count,
                   "quarantined_prices": rejected_count, "locations": locations.count(),
                   "rejection_reasons": dict(sorted(rejection_counts.items())),
                   "unknown_price_units": accepted.filter("price_unit = 'UNKNOWN'").count(),
                   "unmatched_locations": accepted.filter("location_match_status = 'UNMATCHED'").count(),
                   "missing_locations": accepted.filter("location_match_status = 'MISSING'").count()}
        with tempfile.TemporaryDirectory(prefix=".curate-", dir=parent) as directory:
            stage = Path(directory)
            output_counts = _write_tables({"accepted_prices": accepted, "quarantine_prices": rejected,
                                          "locations": locations}, stage)
            if output_counts != {"accepted_prices": accepted_count, "quarantine_prices": rejected_count,
                                 "locations": metrics["locations"]}:
                raise CaptureError("Written Parquet row counts do not reconcile")
            manifest = {"schema_version": 1, "status": "complete", "curated_id": identity,
                        "recipe": recipe, "completed_at": utc_now(), "data_license": "ODbL-1.0",
                        "source": "Open Prices / Open Food Facts", "metrics": metrics,
                        "files": artifact_inventory(stage)}
            with (stage / "manifest.json").open("w", encoding="utf-8", newline="\n") as output:
                json.dump(manifest, output, indent=2)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            verify_curated(stage)
            try:
                stage.rename(destination)
            except OSError:
                if not destination.exists():
                    raise
                verify_curated(destination)
                return destination, False
            stage.mkdir()
        return destination, True
    finally:
        for frame in (accepted, rejected, locations):
            if frame is not None:
                frame.unpersist()
