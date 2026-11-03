"""Integrity contract for immutable local curated datasets."""

import hashlib
import json
from pathlib import Path, PurePosixPath

from .ingest import CaptureError, checksum


def recipe_id(recipe: dict) -> str:
    return hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest()


def artifact_inventory(directory: Path) -> list[dict]:
    return [
        {"file": path.relative_to(directory).as_posix(), "bytes": path.stat().st_size,
         "sha256": checksum(path)}
        for path in sorted(directory.rglob("*"))
        if path.is_file() and path != directory / "manifest.json"
    ]


def verify_curated(directory: Path) -> dict:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "complete" or manifest.get("schema_version") != 1:
        raise CaptureError("Unsupported or incomplete curated manifest")
    if manifest.get("curated_id") != recipe_id(manifest["recipe"]):
        raise CaptureError("Curated recipe identity mismatch")
    files = manifest.get("files", [])
    if not files or not any(entry.get("file", "").endswith(".parquet") for entry in files):
        raise CaptureError("Missing curated Parquet artifacts")
    for entry in files:
        name = entry["file"]
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or "\\" in name or ":" in name:
            raise CaptureError("Unsafe curated artifact path")
    if files != artifact_inventory(directory):
        raise CaptureError("Curated artifact inventory or checksum mismatch")
    metrics = manifest["metrics"]
    if metrics["source_prices"] != metrics["accepted_prices"] + metrics["quarantined_prices"]:
        raise CaptureError("Curated row reconciliation failed")
    return manifest
