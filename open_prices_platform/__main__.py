"""Command-line entry point for source capture and audit."""

import argparse
import json
import sys
import urllib.error
from datetime import date
from pathlib import Path

from .ingest import CaptureError, capture, verify_snapshot
from .profile import profile_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    fetch = commands.add_parser("fetch", help="Capture both official bulk exports")
    fetch.add_argument("--lake-root", type=Path, default=Path("data/lake"))
    for name in ("verify", "profile"):
        subcommand = commands.add_parser(name)
        subcommand.add_argument("snapshot", type=Path)
        if name == "profile":
            subcommand.add_argument("--as-of", type=date.fromisoformat,
                                    help="Reference date for reproducible future-date checks")
    args = parser.parse_args()
    try:
        if args.command == "fetch":
            path, created = capture(args.lake_root)
            result = {"snapshot": str(path), "created": created}
        elif args.command == "verify":
            manifest = verify_snapshot(args.snapshot)
            result = {"snapshot_id": manifest["snapshot_id"], "verified": True,
                      "rows": {entry["entity"]: entry["rows"] for entry in manifest["files"]}}
        else:
            result = profile_snapshot(args.snapshot, as_of=args.as_of)
    except (CaptureError, OSError, EOFError, urllib.error.URLError, ValueError, KeyError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
