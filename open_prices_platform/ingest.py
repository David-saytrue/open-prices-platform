"""Capture bounded, immutable snapshots of official bulk exports."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import socket
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

SOURCE_BASE = "https://prices.openfoodfacts.org/data/"
ENTITIES = ("prices", "locations")
REQUIRED = {
    "prices": {"id", "price", "currency", "date", "updated", "location_id"},
    "locations": {"id", "type", "updated"},
}
CHUNK_BYTES = 1024 * 1024
MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024
MAX_EXPANDED_BYTES = 2 * 1024 * 1024 * 1024
MAX_LINE_BYTES = 4 * 1024 * 1024
USER_AGENT = "open-prices-platform/0.1 (educational bulk-export reader)"


class CaptureError(ValueError):
    """A source or snapshot does not satisfy the capture contract."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rows(path: Path):
    """Read JSONL with bounded line and expanded sizes; verify gzip through EOF."""
    expanded = 0
    with gzip.open(path, "rb") as stream:
        line_number = 0
        while line := stream.readline(MAX_LINE_BYTES + 1):
            line_number += 1
            expanded += len(line)
            if len(line) > MAX_LINE_BYTES or expanded > MAX_EXPANDED_BYTES:
                raise CaptureError(f"Expanded data exceeds limits in {path.name}")
            try:
                record = json.loads(line)
            except (ValueError, UnicodeError) as exc:
                raise CaptureError(f"Invalid JSON in {path.name}, line {line_number}") from exc
            if not isinstance(record, dict):
                raise CaptureError(f"Expected JSON object in {path.name}, line {line_number}")
            yield record


def validate_export(path: Path, entity: str) -> dict:
    """Validate source structure, retaining business anomalies for later processing."""
    count = 0
    identifiers = set()
    duplicate_ids = 0
    for record in rows(path):
        if not REQUIRED[entity].issubset(record):
            raise CaptureError(f"Missing required keys in {entity}, line {count + 1}")
        if type(record["id"]) is not int or record["id"] <= 0:
            raise CaptureError(f"Invalid source ID in {entity}, line {count + 1}")
        duplicate_ids += record["id"] in identifiers
        identifiers.add(record["id"])
        count += 1
    if not count:
        raise CaptureError(f"Empty export: {entity}")
    return {"rows": count, "duplicate_ids": duplicate_ids}


def download(entity: str, target: Path, *, opener=None, attempts: int = 3,
             sleeper=time.sleep) -> dict:
    """Retry transient failures, never retry permanent HTTP errors."""
    if entity not in ENTITIES:
        raise CaptureError("Unsupported source entity")
    if attempts < 1:
        raise CaptureError("Attempts must be positive")
    opener = opener or urllib.request.urlopen
    url = SOURCE_BASE + entity + ".jsonl.gz"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(attempts):
        try:
            with opener(request, timeout=60) as response, target.open("wb") as output:
                advertised = response.headers.get("Content-Length")
                if advertised and int(advertised) > MAX_DOWNLOAD_BYTES:
                    raise CaptureError(f"Compressed export too large: {entity}")
                size = 0
                while chunk := response.read(CHUNK_BYTES):
                    size += len(chunk)
                    if size > MAX_DOWNLOAD_BYTES:
                        raise CaptureError(f"Compressed export too large: {entity}")
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
                if advertised and size != int(advertised):
                    raise OSError("Incomplete HTTP response")
                metadata = {
                    "entity": entity, "file": target.name, "source_url": url,
                    "bytes": size, "sha256": checksum(target),
                    "last_modified": response.headers.get("Last-Modified"),
                    "etag": response.headers.get("ETag"), "downloaded_at": utc_now(),
                }
            metadata.update(validate_export(target, entity))
            return metadata
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise
            retry_after = exc.headers.get("Retry-After", "")
            delay = 2 ** attempt
            if retry_after.isdigit():
                delay = max(delay, int(retry_after))
            elif retry_after:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    delay = max(delay, (retry_at - datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError):
                    raise CaptureError("Unrecognized Retry-After; run capture later") from exc
            if delay > 60:
                raise CaptureError("Source requests a later retry; run capture later") from exc
            sleeper(delay)
        except (urllib.error.URLError, OSError, EOFError, socket.timeout):
            if attempt == attempts - 1:
                raise
            sleeper(2 ** attempt)
    raise CaptureError("Capture retry loop exhausted")


def snapshot_id(files: list[dict]) -> str:
    identity = [{"entity": entry["entity"], "sha256": entry["sha256"]} for entry in files]
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def verify_snapshot(snapshot: Path) -> dict:
    """Verify manifest identity and actual file bytes before use."""
    manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("status") != "complete":
        raise CaptureError("Unsupported or incomplete manifest")
    files = manifest.get("files", [])
    if [entry.get("entity") for entry in files] != list(ENTITIES):
        raise CaptureError("Manifest must contain prices and locations in canonical order")
    if manifest.get("snapshot_id") != snapshot_id(files):
        raise CaptureError("Manifest content identity mismatch")
    for entry in files:
        if entry.get("file") != entry["entity"] + ".jsonl.gz":
            raise CaptureError("Unexpected export filename in manifest")
        path = snapshot / entry["file"]
        if path.stat().st_size != entry["bytes"] or checksum(path) != entry["sha256"]:
            raise CaptureError(f"Stored export checksum mismatch: {entry['entity']}")
        observed = validate_export(path, entry["entity"])
        if any(entry.get(key) != value for key, value in observed.items()):
            raise CaptureError(f"Stored export counts mismatch: {entry['entity']}")
    return manifest


def capture(lake_root: Path, *, opener=None, attempts: int = 3, sleeper=time.sleep) -> tuple[Path, bool]:
    """Publish a complete content-addressed bundle; identical reruns reuse it."""
    root = Path(lake_root).resolve()
    snapshots = root / "raw" / "open_prices"
    snapshots.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".capture-", dir=snapshots) as directory:
        stage = Path(directory)
        started = utc_now()
        files = [download(entity, stage / (entity + ".jsonl.gz"), opener=opener,
                          attempts=attempts, sleeper=sleeper) for entity in ENTITIES]
        identity = snapshot_id(files)
        destination = snapshots / identity
        if destination.exists():
            verify_snapshot(destination)
            return destination, False
        manifest = {
            "schema_version": 1, "status": "complete", "snapshot_id": identity,
            "capture_started_at": started, "capture_completed_at": utc_now(),
            "source": "Open Prices / Open Food Facts", "data_license": "ODbL-1.0",
            "cross_entity_consistency": "independently generated source exports",
            "files": files,
        }
        with (stage / "manifest.json").open("w", encoding="utf-8", newline="\n") as output:
            json.dump(manifest, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        # Same-filesystem rename makes the complete directory visible together.
        try:
            stage.rename(destination)
        except OSError:
            if not destination.exists():
                raise
            verify_snapshot(destination)
            return destination, False
        # TemporaryDirectory must retain its own empty cleanup path after rename.
        stage.mkdir()
        return destination, True
