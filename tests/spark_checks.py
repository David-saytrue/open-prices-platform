"""Spark integration tests; run with spark-submit, independently of offline tests."""

import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

from open_prices_platform.curated import verify_curated
from open_prices_platform.ingest import CaptureError, capture
from open_prices_platform.spark_transform import (
    LOCATION_FIELDS, PRICE_FIELDS, classify_prices, normalize_locations, read_export,
    transform_snapshot,
)
from test_ingest import pack, price, source


class SparkChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.appName("open-prices-regression")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def frames(self, records, location_records=None):
        location_records = location_records or [{"id": 10, "type": "OSM", "updated": "2026-10-01"}]
        prices_path, stores_path = self.root / "prices.jsonl.gz", self.root / "locations.jsonl.gz"
        prices_path.write_bytes(pack(records))
        stores_path.write_bytes(pack(location_records))
        return (read_export(self.spark, prices_path, PRICE_FIELDS),
                normalize_locations(read_export(self.spark, stores_path, LOCATION_FIELDS)))

    def test_business_rules_corrections_and_null_references(self):
        records = [
            price(1, price="2.50"),
            price(1, price="3.00", updated="2026-10-03T00:00:00Z"),
            price(2, location_id=999, price_per=None, currency="usd"),
            price(3, location_id=None),
            price(4, price="-1"),
            price(5, date="not-a-date"),
            price(6, date="2027-01-01"),
            price(7, price_per="GALLON"),
            price(8, duplicate_of=1),
            price(9, price="NaN"),
            price(10, price="1.1234567"),
            price(11, price="2.50"),  # Equal value is not a duplicate observation.
            price(12, currency=None),
            price(13, updated=None),
            price(14, price_is_discounted=True, price_without_discount="1.00"),
            price(15, price="2.00"),
            price(15, price="bad", updated="2026-10-04T00:00:00Z"),
            price(16, price="0", price_per="LITER"),
        ]
        raw, locations = self.frames(records)
        accepted, quarantine = classify_prices(raw, locations, date(2026, 11, 3), "synthetic")
        actual = {row.observation_id: row for row in accepted.collect()}
        self.assertEqual(set(actual), {1, 2, 3, 11, 16})
        self.assertEqual(str(actual[1].price_amount), "3.000000")
        self.assertEqual(actual[2].price_unit, "UNKNOWN")
        self.assertEqual(actual[2].currency_code, "USD")
        self.assertEqual(actual[2].location_match_status, "UNMATCHED")
        self.assertEqual(actual[3].location_match_status, "MISSING")
        self.assertEqual(actual[16].price_unit, "LITER")
        rejected = quarantine.collect()
        self.assertEqual(len(actual) + len(rejected), len(records))
        self.assertIn("INVALID_DISCOUNT_RANGE", next(row.rejection_reasons for row in rejected if row.id == "14"))
        self.assertEqual(sum(row.id == "15" for row in rejected), 2)
        self.assertNotIn("owner", accepted.columns)
        self.assertNotIn("_raw_json", quarantine.columns)

    def test_conflicting_versions_and_dimension_fanout_stop_processing(self):
        raw, locations = self.frames([price(1), price(1, price="99.00")])
        with self.assertRaisesRegex(CaptureError, "Conflicting payloads"):
            classify_prices(raw, locations, date(2026, 11, 3), "synthetic")
        with self.assertRaisesRegex(CaptureError, "Duplicate location"):
            self.frames([price()], [
                {"id": 10, "type": "OSM", "updated": "2026-10-01"},
                {"id": 10, "type": "OSM", "updated": "2026-10-02"},
            ])

    def test_exact_duplicate_reconciles_without_double_counting(self):
        raw, locations = self.frames([price(), price()])
        accepted, quarantine = classify_prices(raw, locations, date(2026, 11, 3), "synthetic")
        self.assertEqual(accepted.count(), 1)
        self.assertEqual(quarantine.count(), 1)
        self.assertIn("SUPERSEDED_OR_DUPLICATE_ID", quarantine.first().rejection_reasons)

    def test_publish_replay_failure_and_artifact_tampering(self):
        snapshot, _ = capture(self.root / "lake", opener=source([price(), price(2, price="bad")]))
        output, created = transform_snapshot(self.spark, snapshot, self.root / "lake")
        self.assertTrue(created)
        manifest_bytes = (output / "manifest.json").read_bytes()
        manifest = verify_curated(output)
        self.assertEqual(manifest["metrics"]["accepted_prices"], 1)
        self.assertEqual(manifest["metrics"]["quarantined_prices"], 1)
        self.assertEqual(manifest["recipe"]["spark_version"], "4.0.1")
        self.assertEqual(str(self.spark.read.parquet(str(output / "accepted_prices")).first().price_amount), "2.500000")
        replay, created = transform_snapshot(self.spark, snapshot, self.root / "lake")
        self.assertFalse(created)
        self.assertEqual(replay, output)
        self.assertEqual((output / "manifest.json").read_bytes(), manifest_bytes)
        # A new source candidate failing during its write must preserve the prior output.
        corrected, _ = capture(self.root / "lake", opener=source([price(price="4.00")]))

        def interrupted(tables, stage):
            (stage / "partial-output").write_text("incomplete")
            raise RuntimeError("simulated write failure")

        with patch("open_prices_platform.spark_transform._write_tables", side_effect=interrupted):
            with self.assertRaisesRegex(RuntimeError, "simulated write failure"):
                transform_snapshot(self.spark, corrected, self.root / "lake")
        self.assertEqual(list(output.parent.iterdir()), [output])
        verify_curated(output)
        artifact = next(output.rglob("*.parquet"))
        artifact.write_bytes(b"corrupt")
        with self.assertRaises(CaptureError):
            verify_curated(output)
        with self.assertRaises(CaptureError):
            transform_snapshot(self.spark, snapshot, self.root / "lake")

    def test_empty_outputs_keep_readable_schema(self):
        snapshot, _ = capture(self.root / "lake", opener=source([price()]))
        output, _ = transform_snapshot(self.spark, snapshot, self.root / "lake")
        empty = self.spark.read.parquet(str(output / "quarantine_prices"))
        self.assertEqual(empty.count(), 0)
        self.assertIn("rejection_reasons", empty.columns)
        # Conversely, a complete all-rejected batch still has readable fact schema.
        snapshot, _ = capture(self.root / "lake", opener=source([price(price="bad")]))
        output, _ = transform_snapshot(self.spark, snapshot, self.root / "lake")
        empty = self.spark.read.parquet(str(output / "accepted_prices"))
        self.assertEqual(empty.count(), 0)
        self.assertIn("price_amount", empty.columns)


if __name__ == "__main__":
    unittest.main(verbosity=2)
