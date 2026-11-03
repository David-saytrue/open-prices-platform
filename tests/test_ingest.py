import gzip
import io
import json
import tempfile
import unittest
import urllib.error
from datetime import date
from pathlib import Path
from unittest.mock import patch

from open_prices_platform.ingest import CaptureError, capture, rows, verify_snapshot
from open_prices_platform.profile import profile_snapshot


def pack(records):
    return gzip.compress(("\n".join(json.dumps(record) for record in records) + "\n").encode(), mtime=0)


def price(identifier=1, **changes):
    return {"id": identifier, "price": "2.50", "currency": "GBP", "date": "2026-10-01",
            "updated": "2026-10-02T00:00:00Z", "location_id": 10,
            "product_code": "0000000000001", "price_per": "UNIT", **changes}


class Response(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.headers = {"Content-Length": str(len(data)), "Last-Modified": "test"}


def source(prices=None, locations=None):
    payloads = {
        "prices": pack(prices if prices is not None else [price()]),
        "locations": pack(locations if locations is not None else [{"id": 10, "type": "OSM", "updated": "2026-10-01"}]),
    }

    def open_export(request, timeout):
        entity = request.full_url.rsplit("/", 1)[1].split(".")[0]
        return Response(payloads[entity])

    return open_export


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_rerun_reuses_verified_snapshot(self):
        first, created = capture(self.root, opener=source())
        self.assertTrue(created)
        original = (first / "manifest.json").read_bytes()
        second, created = capture(self.root, opener=source())
        self.assertFalse(created)
        self.assertEqual(first, second)
        self.assertEqual(original, (second / "manifest.json").read_bytes())
        self.assertEqual(len(list(first.parent.iterdir())), 1)

    def test_changed_source_creates_separate_snapshot(self):
        first, _ = capture(self.root, opener=source())
        second, _ = capture(self.root, opener=source([price(price="3.00")]))
        self.assertNotEqual(first, second)
        self.assertEqual(verify_snapshot(first)["files"][0]["rows"], 1)

    def test_second_download_failure_publishes_nothing(self):
        working = source()

        def unavailable(request, timeout):
            if "locations" in request.full_url:
                raise urllib.error.URLError("offline")
            return working(request, timeout)

        with self.assertRaises(urllib.error.URLError):
            capture(self.root, opener=unavailable, attempts=1)
        self.assertEqual(list((self.root / "raw" / "open_prices").iterdir()), [])

    def test_transient_error_retries(self):
        working = source()
        attempts = []

        def flaky(request, timeout):
            attempts.append(request.full_url)
            if len(attempts) == 1:
                raise urllib.error.URLError("temporary")
            return working(request, timeout)

        waits = []
        snapshot, _ = capture(self.root, opener=flaky, sleeper=waits.append)
        self.assertEqual(waits, [1])
        self.assertEqual(verify_snapshot(snapshot)["files"][0]["rows"], 1)

    def test_forbidden_is_not_retried(self):
        calls = []

        def forbidden(request, timeout):
            calls.append(1)
            raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

        with self.assertRaises(urllib.error.HTTPError):
            capture(self.root, opener=forbidden)
        self.assertEqual(calls, [1])

    def test_rate_limit_respects_retry_after(self):
        working = source()
        calls = []

        def limited(request, timeout):
            calls.append(1)
            if len(calls) == 1:
                raise urllib.error.HTTPError(request.full_url, 429, "Limited", {"Retry-After": "7"}, None)
            return working(request, timeout)

        waits = []
        capture(self.root, opener=limited, sleeper=waits.append)
        self.assertEqual(waits, [7])

    def test_long_http_date_retry_after_stops_without_early_retry(self):
        def limited(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 429, "Limited",
                                         {"Retry-After": "Fri, 01 Jan 2100 00:00:00 GMT"}, None)

        with self.assertRaises(CaptureError):
            capture(self.root, opener=limited, sleeper=lambda _: self.fail("Must not retry early"))

    def test_missing_required_field_rejects_capture(self):
        record = price()
        del record["currency"]
        with self.assertRaises(CaptureError):
            capture(self.root, opener=source([record]))

    def test_empty_export_rejected(self):
        with self.assertRaises(CaptureError):
            capture(self.root, opener=source([]))

    def test_checksum_tampering_is_detected(self):
        snapshot, _ = capture(self.root, opener=source())
        (snapshot / "prices.jsonl.gz").write_bytes(pack([price(price="9.00")]))
        with self.assertRaises(CaptureError):
            verify_snapshot(snapshot)

    def test_corrupt_existing_snapshot_is_not_silently_reused(self):
        snapshot, _ = capture(self.root, opener=source())
        (snapshot / "locations.jsonl.gz").write_bytes(b"broken")
        with self.assertRaises(CaptureError):
            capture(self.root, opener=source())

    def test_path_in_manifest_is_rejected(self):
        snapshot, _ = capture(self.root, opener=source())
        manifest = verify_snapshot(snapshot)
        manifest["files"][0]["file"] = "../prices.jsonl.gz"
        (snapshot / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaises(CaptureError):
            verify_snapshot(snapshot)

    def test_truncated_gzip_is_rejected(self):
        working = source()

        def truncated(request, timeout):
            result = working(request, timeout)
            return Response(result.getvalue()[:-5])

        with self.assertRaises((EOFError, OSError)):
            capture(self.root, opener=truncated, attempts=1)

    def test_oversized_line_is_rejected(self):
        path = self.root / "oversized.gz"
        path.write_bytes(pack([{"message": "x" * 100}]))
        with patch("open_prices_platform.ingest.MAX_LINE_BYTES", 30):
            with self.assertRaises(CaptureError):
                list(rows(path))

    def test_invalid_json_and_nonobject_are_rejected(self):
        for data in (b"not-json\n", b"[]\n"):
            path = self.root / "bad.gz"
            path.write_bytes(gzip.compress(data))
            with self.assertRaises(CaptureError):
                list(rows(path))

    def test_profile_reports_missing_references_and_nonfinite_price(self):
        snapshot, _ = capture(self.root, opener=source([
            price(), price(2, location_id=99, price="NaN"),
            price(3, location_id=None, price="-1", product_code=None),
            price(4, date="not-a-date", price="Infinity"),
        ]))
        report = profile_snapshot(snapshot)
        self.assertEqual(report["counts"]["observations"], 4)
        self.assertEqual(report["counts"]["unmatched_location_reference"], 1)
        self.assertEqual(report["counts"]["missing_location_reference"], 1)
        self.assertEqual(report["counts"]["invalid_or_negative_price"], 3)
        self.assertEqual(report["counts"]["invalid_observation_date"], 1)

    def test_duplicate_source_ids_reported_not_silently_dropped(self):
        snapshot, _ = capture(self.root, opener=source([price(), price()]))
        manifest = verify_snapshot(snapshot)
        self.assertEqual(manifest["files"][0]["duplicate_ids"], 1)
        self.assertEqual(manifest["files"][0]["rows"], 2)

    def test_future_date_check_has_explicit_reference_date(self):
        snapshot, _ = capture(self.root, opener=source([price(date="2026-10-02")]))
        report = profile_snapshot(snapshot, as_of=date(2026, 10, 1))
        self.assertEqual(report["counts"]["future_observation_dates"], 1)
        self.assertEqual(report["as_of_date"], "2026-10-01")


if __name__ == "__main__":
    unittest.main()
