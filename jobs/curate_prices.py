"""Submit the immutable Open Prices transformation with spark-submit."""

import argparse
import json
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from open_prices_platform.curated import verify_curated
from open_prices_platform.spark_transform import transform_snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--lake-root", type=Path, default=Path("data/lake"))
    parser.add_argument("--as-of", type=date.fromisoformat)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("open-prices-curation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        path, created = transform_snapshot(spark, args.snapshot, args.lake_root, as_of=args.as_of)
        print(json.dumps({"curated_path": str(path), "created": created,
                          "metrics": verify_curated(path)["metrics"]}, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
