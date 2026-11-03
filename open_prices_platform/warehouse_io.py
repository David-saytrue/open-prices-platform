"""Dependency-free COPY serialization and warehouse target guards."""

from datetime import datetime, timezone
from decimal import Decimal

from .ingest import CaptureError


def validate_database(name: str) -> str:
    if name != "open_prices" and not (name.startswith("open_prices_test_") and
                                      name[17:].isalnum()):
        raise CaptureError("Warehouse target must be open_prices or an isolated open_prices_test_<id> database")
    return name


def copy_csv(records: list[dict], columns: tuple[str, ...]) -> str:
    """Quote every non-NULL value, so literal backslash-N never becomes SQL NULL."""
    lines = []
    for record in records:
        fields = []
        for column in columns:
            value = record[column]
            if value is None:
                fields.append("\\N")
                continue
            if isinstance(value, bool):
                value = "true" if value else "false"
            elif isinstance(value, datetime):
                value = (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).isoformat()
            elif isinstance(value, Decimal):
                value = str(value)
            fields.append('"' + str(value).replace('"', '""') + '"')
        lines.append(",".join(fields) + "\n")
    return "".join(lines)
