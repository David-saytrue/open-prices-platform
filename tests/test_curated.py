import json
import tempfile
import unittest
from pathlib import Path

from open_prices_platform.curated import artifact_inventory, recipe_id, verify_curated
from open_prices_platform.ingest import CaptureError


class CuratedIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "facts.parquet").write_bytes(b"synthetic artifact for integrity checks")
        self.manifest = {
            "schema_version": 1, "status": "complete", "recipe": {"source": "synthetic"},
            "curated_id": recipe_id({"source": "synthetic"}),
            "files": artifact_inventory(self.root),
            "metrics": {"source_prices": 3, "accepted_prices": 2, "quarantined_prices": 1},
        }
        self.save()

    def save(self):
        (self.root / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")

    def test_complete_artifact_verifies(self):
        self.assertEqual(verify_curated(self.root)["metrics"]["source_prices"], 3)

    def test_changed_or_extra_artifact_fails(self):
        (self.root / "extra.parquet").write_bytes(b"unexpected")
        with self.assertRaises(CaptureError):
            verify_curated(self.root)

    def test_manifest_without_complete_state_fails(self):
        self.manifest["status"] = "writing"
        self.save()
        with self.assertRaises(CaptureError):
            verify_curated(self.root)

    def test_row_count_mismatch_fails(self):
        self.manifest["metrics"]["accepted_prices"] = 1
        self.save()
        with self.assertRaises(CaptureError):
            verify_curated(self.root)

    def test_unsafe_artifact_path_fails(self):
        self.manifest["files"][0]["file"] = "../outside.parquet"
        self.save()
        with self.assertRaises(CaptureError):
            verify_curated(self.root)


if __name__ == "__main__":
    unittest.main()
