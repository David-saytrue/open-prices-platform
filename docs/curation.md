# Spark curation contract

## Runtime and source

Spark 4.0.1, Python 3.10.12 and Java 17.0.16 run in an official digest-pinned
Apache Spark container. The source capture is verified before processing.
An explicit JSON schema reads selected source fields as strings; casting uses
Spark's tolerant `try_cast` / `try_to_timestamp` operations with ANSI mode on.
Malformed fields become reason-coded rejections rather than job-wide cast errors.

## Fact grain and corrections

One accepted fact represents a source price observation ID. Equal prices with
different IDs remain distinct. For repeated IDs within a source file, parseable
source update timestamps determine the latest candidate. Superseded or identical
repeated rows go to quarantine. Distinct raw JSON payloads at the same ID and
parsed update time are ambiguous and fail the entire candidate.

An invalid business value in the latest version excludes that ID from accepted
facts; no older version is resurrected. Rows without a parseable update timestamp
are rejected. Every source row is accounted for in facts or quarantine. An output
is a full snapshot; this is not CDC or a cross-snapshot history merge.

## Business validation

- Price and optional original price use DECIMAL(20,6): up to 14 integral and six
  fractional digits, fixed-point nonnegative notation. Values requiring rounding,
  negative values, NaN, infinity and overflows are rejected. Zero is valid.
- Observation dates must be valid ISO YYYY-MM-DD dates, no later than `as_of`.
- Currency is trimmed and uppercased; a three-letter format is required. This
  checks shape, not ISO-4217 membership or historical validity. No conversion runs.
- Supported explicit price units are UNIT, KILOGRAM and LITER. Missing units stay
  UNKNOWN. An unsupported explicit unit is rejected. UNKNOWN does not establish
  comparability between packaged goods or products.
- Discount flags, when present, must be true/false. An original price below a
  discounted price is rejected. Missing original prices stay NULL; discount
  percentages must not be calculated from them.
- Source-marked duplicate observations (`duplicate_of` is not NULL) go to quarantine.
- Product codes, names and categories can be missing; no descriptions are invented
  or translated. Names are not guaranteed to be English.

## Location joins

Location IDs must be positive and unique; duplicates stop publication before a
join can multiply facts. Facts use a left join. Missing or absent location
references remain accepted with MISSING or UNMATCHED status and explicit metrics.
Invalid non-null reference IDs are rejected. Store names remain source literals.

## Publication and recovery

Write Parquet into an isolated candidate directory. Read back all three tables,
verify counts/types and unique fact IDs, reconcile accepted plus quarantine to
source rows, calculate artifact checksums, and publish a completion manifest.
Same-filesystem directory rename publishes the complete local output together.
No partial directory is an input to downstream work. A failed candidate leaves
previous outputs unchanged. Identical recipes reuse verified output files;
checksum corruption fails instead of silently overwriting an existing result.
Filesystem publication is local; S3 atomicity is not claimed.

Quarantine contains selected source values, reason arrays and row hashes, without
contributor identifiers, proof payloads or free-text owner comments. Reason counts
may overlap because one rejected row can have several reasons.

## References

- [Spark 4.0.1 runtime requirements](https://spark.apache.org/docs/4.0.1/)
- [Spark ANSI mode and tolerant casts](https://spark.apache.org/docs/4.0.1/sql-ref-ansi-compliance.html)
- [Spark JSON data source](https://spark.apache.org/docs/4.0.1/sql-data-sources-json.html)
- [Open Prices unit definitions](https://github.com/openfoodfacts/open-prices/blob/main/open_prices/prices/constants.py)
