"""Aggregate-only source audit; no raw rows or contributor identifiers in reports."""

from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .ingest import rows, verify_snapshot


def profile_snapshot(snapshot: Path, *, as_of: date | None = None) -> dict:
    manifest = verify_snapshot(snapshot)
    location_ids = {record["id"] for record in rows(snapshot / "locations.jsonl.gz")}
    counts = Counter()
    currencies = Counter()
    units = Counter()
    observed_dates = []
    for record in rows(snapshot / "prices.jsonl.gz"):
        counts["observations"] += 1
        counts["missing_location_reference"] += record.get("location_id") is None
        counts["unmatched_location_reference"] += (
            record.get("location_id") is not None and record["location_id"] not in location_ids
        )
        counts["missing_product_code"] += not record.get("product_code")
        counts["missing_product_name"] += not record.get("product_name")
        counts["discounted_observations"] += record.get("price_is_discounted") is True
        counts["source_marked_duplicates"] += record.get("duplicate_of") is not None
        currencies[str(record.get("currency") or "UNKNOWN")] += 1
        units[str(record.get("price_per") or "UNKNOWN")] += 1
        try:
            value = Decimal(str(record.get("price")))
            valid = value.is_finite() and value >= 0
        except InvalidOperation:
            valid = False
        counts["invalid_or_negative_price"] += not valid
        try:
            observed_dates.append(date.fromisoformat(record["date"]))
        except (ValueError, TypeError):
            counts["invalid_observation_date"] += 1
    today = as_of or datetime.now(timezone.utc).date()
    counts["future_observation_dates"] = sum(value > today for value in observed_dates)
    return {
        "snapshot_id": manifest["snapshot_id"],
        "source": manifest["source"], "data_license": manifest["data_license"],
        "capture_completed_at": manifest["capture_completed_at"],
        "as_of_date": today.isoformat(),
        "counts": dict(sorted(counts.items())),
        "locations": len(location_ids),
        "observation_date_min": str(min(observed_dates)) if observed_dates else None,
        "observation_date_max": str(max(observed_dates)) if observed_dates else None,
        "currencies": dict(sorted(currencies.items())),
        "price_units": dict(sorted(units.items())),
        "duplicate_ids_by_entity": {entry["entity"]: entry["duplicate_ids"] for entry in manifest["files"]},
        "limitations": [
            "Crowdsourced observations are not sales or inventory counts.",
            "Source exports have independent generation times.",
            "Product names are not guaranteed present or English.",
            "Category and product metadata enrichment is not yet implemented.",
        ],
    }
