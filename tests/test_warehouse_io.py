import unittest
from datetime import datetime
from decimal import Decimal

from open_prices_platform.ingest import CaptureError
from open_prices_platform.warehouse_io import copy_csv, validate_database


class WarehouseIOTests(unittest.TestCase):
    def test_null_empty_and_literal_null_marker_are_distinct(self):
        self.assertEqual(copy_csv([{"a": None, "b": "", "c": "\\N"}], ("a", "b", "c")), '\\N,"","\\N"\n')

    def test_csv_quotes_newlines_and_commas(self):
        self.assertEqual(copy_csv([{"a": 'store,"name"\nsecond line'}], ("a",)), '"store,""name""\nsecond line"\n')

    def test_decimal_boolean_and_naive_utc_timestamp(self):
        result = copy_csv([{"a": Decimal("1.123456"), "b": False, "c": datetime(2026, 11, 3)}], ("a", "b", "c"))
        self.assertEqual(result, '"1.123456","false","2026-11-03T00:00:00+00:00"\n')

    def test_unrelated_database_is_rejected(self):
        for name in ("postgres", "retail", "open_prices_test_", "open_prices_test_x;DROP"):
            with self.assertRaises(CaptureError):
                validate_database(name)
        self.assertEqual(validate_database("open_prices"), "open_prices")
        self.assertEqual(validate_database("open_prices_test_a123"), "open_prices_test_a123")
