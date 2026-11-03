"""Integration checks against real Greenplum using isolated synthetic databases."""

import json
import os
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2 import sql

from open_prices_platform.curated import artifact_inventory, recipe_id
from open_prices_platform.ingest import CaptureError, capture
from open_prices_platform.warehouse import connect, initialize, load_curated
from open_prices_platform.tableau import setup_views
from test_ingest import price, source

FACT_SCHEMA = pa.schema([
    ("observation_id", pa.int64()), ("source_snapshot_id", pa.string()),
    ("product_code", pa.string()), ("product_name", pa.string()), ("category_tag", pa.string()),
    ("location_id", pa.int64()), ("price_amount", pa.decimal128(20, 6)),
    ("price_before_discount", pa.decimal128(20, 6)), ("currency_code", pa.string()),
    ("price_unit", pa.string()), ("observed_date", pa.date32()),
    ("source_updated_at", pa.timestamp("us", tz="UTC")), ("is_discounted", pa.bool_()),
    ("discount_type", pa.string()), ("location_match_status", pa.string()),
])
LOCATION_SCHEMA = pa.schema([
    ("location_id", pa.int64()), ("location_type", pa.string()), ("store_name", pa.string()),
    ("store_brand", pa.string()), ("store_city", pa.string()), ("store_country_code", pa.string()),
    ("location_source_updated", pa.string()),
])


def fact(identifier=1, amount="2.00", **changes):
    return {"observation_id": identifier, "source_snapshot_id": "fixture",
            "product_code": "0001", "product_name": "Synthetic product", "category_tag": None,
            "location_id": 10, "price_amount": Decimal(amount), "price_before_discount": None,
            "currency_code": "EUR", "price_unit": "UNIT", "observed_date": date(2026, 10, 1),
            "source_updated_at": datetime(2026, 10, 2, tzinfo=timezone.utc),
            "is_discounted": None, "discount_type": None, "location_match_status": "MATCHED", **changes}


class WarehouseChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("OPEN_PRICES_TEST_DB") != "1":
            raise RuntimeError("Set OPEN_PRICES_TEST_DB=1 to create isolated synthetic test databases")

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.database = "open_prices_test_" + uuid.uuid4().hex[:10]
        initialize(self.database)
        self.addCleanup(self.drop_created_database)

    def drop_created_database(self):
        # Only the exact random database created by this test is eligible for cleanup.
        if not self.database.startswith("open_prices_test_"):
            raise RuntimeError("Refusing to clean an unrelated database")
        connection = connect("postgres")
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(self.database)))
        finally:
            connection.close()

    def query(self, statement, parameters=None):
        connection = connect(self.database)
        try:
            with connection, connection.cursor() as cursor:
                cursor.execute(statement, parameters)
                return cursor.fetchall()
        finally:
            connection.close()

    def fixture(self, facts, locations=None, *, captured_at=None):
        locations = locations or [
            {"location_id": 10, "location_type": "OSM", "store_name": 'Synthetic, "shop"\nA',
             "store_brand": None, "store_city": "Test City", "store_country_code": "GB",
             "location_source_updated": "2026-10-01"},
            {"location_id": 20, "location_type": "OSM", "store_name": "Synthetic B",
             "store_brand": "", "store_city": None, "store_country_code": "GB",
             "location_source_updated": "2026-10-01"},
        ]
        # Build genuinely verified raw and curated fixtures without live source calls.
        raw, _ = capture(self.root / ("raw-" + uuid.uuid4().hex[:6]), opener=source(
            [price(row["observation_id"], price=str(row["price_amount"])) for row in facts],
            [{"id": row["location_id"], "type": "OSM", "updated": "2026-10-01"} for row in locations]))
        raw_manifest = json.loads((raw / "manifest.json").read_text())
        if captured_at:
            raw_manifest["capture_completed_at"] = captured_at
            (raw / "manifest.json").write_text(json.dumps(raw_manifest))
        for row in facts:
            row["source_snapshot_id"] = raw_manifest["snapshot_id"]
        recipe = {"source_snapshot_id": raw_manifest["snapshot_id"], "fixture": uuid.uuid4().hex}
        output = self.root / recipe_id(recipe)
        output.mkdir()
        for name, records, schema in (("accepted_prices", facts, FACT_SCHEMA),
                                      ("locations", locations, LOCATION_SCHEMA)):
            (output / name).mkdir()
            pq.write_table(pa.Table.from_pylist(records, schema=schema), output / name / "part.parquet")
        metrics = {"source_prices": len(facts), "accepted_prices": len(facts), "quarantined_prices": 0,
                   "locations": len(locations), "unknown_price_units": sum(row["price_unit"] == "UNKNOWN" for row in facts),
                   "missing_locations": sum(row["location_id"] is None for row in facts),
                   "unmatched_locations": sum(row["location_match_status"] == "UNMATCHED" for row in facts)}
        (output / "manifest.json").write_text(json.dumps({
            "schema_version": 1, "status": "complete", "recipe": recipe,
            "curated_id": recipe_id(recipe), "metrics": metrics, "files": artifact_inventory(output)}))
        return output, raw

    def test_grain_currencies_units_nulls_and_replay(self):
        output, raw = self.fixture([
            fact(1, "2", product_name="\\N", is_discounted=True, price_before_discount=Decimal("3")),
            fact(2, "4", location_id=20, product_name=""), fact(3, "6", location_id=20),
            fact(4, "9", currency_code="USD"), fact(5, "8", price_unit="KILOGRAM"),
            fact(6, "3", price_unit="UNKNOWN"), fact(7, "7", price_unit="UNKNOWN", location_id=20),
            fact(8, "11", product_code=None, location_id=None, location_match_status="MISSING"),
            fact(9, "5", product_code="0002", location_id=999, location_match_status="UNMATCHED"),
        ])
        first = load_curated(output, raw, self.database)
        self.assertTrue(first["created"])
        row = self.query("""SELECT observation_count, matched_store_count, median_observed_price,
                         comparable_store_price_gap FROM mart.daily_product_prices
                         WHERE product_code='0001' AND currency_code='EUR' AND price_unit='UNIT'""")[0]
        self.assertEqual(row, (3, 2, Decimal("4"), Decimal("3")))
        self.assertEqual(self.query("SELECT comparable_store_price_gap FROM mart.daily_product_prices WHERE price_unit='UNKNOWN'"), [(None,)])
        self.assertEqual(self.query("SELECT product_name FROM dwh.price_observation WHERE observation_id=1"), [("\\N",)])
        self.assertEqual(self.query("SELECT store_name FROM dwh.location WHERE location_id=10"), [('Synthetic, "shop"\nA',)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM mart.daily_store_prices WHERE location_id IS NULL"), [(1,)])
        self.assertEqual(load_curated(output, raw, self.database)["created"], False)
        self.assertEqual(self.query("SELECT COUNT(*) FROM ops.loads"), [(1,)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM dwh.price_observation"), [(9,)])

    def test_failure_after_mart_build_rolls_back_previous_publication(self):
        output, raw = self.fixture([fact()])
        first = load_curated(output, raw, self.database)
        corrected, corrected_raw = self.fixture([fact(amount="99")])
        with patch("open_prices_platform.warehouse._validate_publication", side_effect=RuntimeError("simulated quality failure")):
            with self.assertRaisesRegex(RuntimeError, "simulated quality failure"):
                load_curated(corrected, corrected_raw, self.database)
        self.assertEqual(self.query("SELECT price_amount FROM dwh.price_observation"), [(Decimal("2"),)])
        self.assertEqual(self.query("SELECT load_id FROM ops.publication"), [(first["load_id"],)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM ops.loads"), [(1,)])
        # A valid correction is accepted after the failed transaction released locks.
        self.assertTrue(load_curated(corrected, corrected_raw, self.database)["created"])
        self.assertEqual(self.query("SELECT price_amount FROM dwh.price_observation"), [(Decimal("99"),)])

    def test_duplicate_dimensions_and_stale_source_do_not_replace_data(self):
        output, raw = self.fixture([fact()])
        load_curated(output, raw, self.database)
        duplicate_locations = [{"location_id": 10, "location_type": "OSM", "store_name": "Synthetic"}] * 2
        bad, bad_raw = self.fixture([fact(amount="5")], duplicate_locations)
        with self.assertRaisesRegex(CaptureError, "Duplicate staging dimension"):
            load_curated(bad, bad_raw, self.database)
        older, older_raw = self.fixture([fact(amount="6")], captured_at="2026-10-02T00:00:00+00:00")
        with self.assertRaisesRegex(CaptureError, "Older source"):
            load_curated(older, older_raw, self.database)
        self.assertEqual(self.query("SELECT price_amount FROM dwh.price_observation"), [(Decimal("2"),)])

    def test_concurrent_same_snapshot_publishes_only_once(self):
        output, raw = self.fixture([fact()])
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: load_curated(output, raw, self.database), range(2)))
        self.assertEqual(sorted(result["created"] for result in results), [False, True])
        self.assertEqual(self.query("SELECT COUNT(*) FROM ops.loads"), [(1,)])

    def test_medians_preserve_decimal_precision_beyond_float_range(self):
        output, raw = self.fixture([
            fact(1, "99999999999999.000001"),
            fact(2, "99999999999999.000003", location_id=20),
        ])
        load_curated(output, raw, self.database)
        self.assertEqual(self.query("SELECT median_observed_price, comparable_store_price_gap FROM mart.daily_product_prices"),
                         [(Decimal("99999999999999.000002"), Decimal("0.000002"))])

    def test_tableau_views_preserve_mart_grain(self):
        output, raw = self.fixture([
            fact(1, product_name="Older name"),
            fact(2, "3", product_name="Latest name", source_updated_at=datetime(2026, 10, 3, tzinfo=timezone.utc)),
            fact(3, "4", location_id=None, location_match_status="MISSING"),
        ])
        load_curated(output, raw, self.database)
        setup_views(self.database)
        self.assertEqual(self.query("SELECT COUNT(*) FROM bi.store_price_trends"),
                         self.query("SELECT COUNT(*) FROM mart.daily_store_prices"))
        self.assertEqual(self.query("SELECT COUNT(*) FROM bi.product_price_comparison"),
                         self.query("SELECT COUNT(*) FROM mart.daily_product_prices"))
        self.assertEqual(self.query("SELECT SUM(observation_count) FROM bi.store_price_trends"), [(Decimal(3),)])
        self.assertEqual(self.query("SELECT product_label FROM bi.product_price_comparison"), [("Latest name",)])
        self.assertEqual(self.query("SELECT accepted_rows, quarantine_rate FROM bi.pipeline_quality"), [(3, Decimal(0))])


if __name__ == "__main__":
    unittest.main(verbosity=2)
