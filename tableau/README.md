# Tableau Desktop setup

The user selected Tableau Desktop for the local BI layer. Desktop Free Edition
supports live database connections and local analysis. Its publishing and packaged
workbook sharing capabilities differ from Professional Edition. No paid license,
trial or account has been created by this project.

## Installation status

The user installed Tableau Desktop 2026.3. The running application was verified
locally on 2026-11-03. No paid license or trial was provisioned by the project.

The three `.tds` files are hand-authored connection templates containing no
passwords or embedded records. Their required datasource attribute `inline` is
set explicitly. The corrected Pipeline Quality template opened in Desktop 2026.3
and reached the Greenplum Sign In dialog without the previous XML loading error.
The Pipeline Quality source has now authenticated and returned data in Desktop.
The other two templates still need Desktop validation.

## Prepare Greenplum sources

After the warehouse milestone has completed:

```powershell
docker compose up -d --wait greenplum
docker compose run --rm --entrypoint python warehouse -m open_prices_platform.tableau --grant-reader
```

This creates three views in `bi` and the project-specific login
`open_prices_tableau`. The role has SELECT on the BI views and a read-only default
transaction setting for this database. No password is embedded in project files;
the isolated local demo retains the image's existing trust authentication.

## Connect from Desktop

Use the Pivotal Greenplum Database connector:

- Server: `127.0.0.1`
- Port: `55433`
- Database: `open_prices`
- Username: `open_prices_tableau`
- Password: leave blank for this local demo's trust authentication.

Opening a template can display a Sign In dialog even with trust authentication.
Leave Password empty and click **OK**. Reaching this dialog alone does not verify
driver compatibility or successful data retrieval.

The Windows installation required an additional 64-bit PostgreSQL ODBC driver.
Without it, Desktop returned `A73B8869` (ODBC Driver Manager: data source name
not found). The official `psqlODBC 17.00.0010` x64 MSI was installed locally;
Windows administrator approval was required. Restart Desktop after installation.
The installed `PostgreSQL Unicode(x64)` driver successfully read
`bi.pipeline_quality` as `open_prices_tableau` with `transaction_read_only=on`.
Desktop also successfully queried the same view and rendered a local dashboard.
Use [official PostgreSQL downloads](https://www.postgresql.org/ftp/odbc/releases/REL-17_00_0010/)
or the driver link displayed by Tableau. Do not expose the demo database to the public network.
Desktop 2026.3, this ODBC driver and Greenplum 7.0.0 build dev were verified together
for the pipeline-quality source. Store and product sources remain unverified in Desktop.

## Verified local workbook

The local file `artifacts/tableau/Pipeline Health.twb` was authored and saved in
Desktop on 2026-11-03. It uses a live Greenplum connection, with three worksheets
(`Accepted Rows`, `Quarantined`, `Quality Details`) and one `Pipeline Health`
dashboard. It contains no extract and remains excluded from Git. The dashboard
was visually checked in Desktop presentation mode.

- Accepted rows: 330,358; quarantined rows: 9,680; source rows: 340,038.
- Quarantine rate: 2.85%, with source rows as denominator.
- Unknown-unit rows: 319,938; unknown-unit rate: 96.85%, with accepted rows as denominator.
- Location rows: 7,781; missing location references: 2; unmatched references: 0.

Both rates use Percentage with two decimal places. The quality worksheet selects
the nine explicit BI measures and omits Tableau's generated record-count measure.
Display aliases shorten long field names without changing their formulas.
An English text block explains denominators, the unit-comparability limitation
and the Open Prices / Open Food Facts source. To view it locally, start the
project Greenplum service, open the workbook and use the reader login above.
The checked workbook is local work; it has not been published to Tableau Cloud,
Server or Public, or distributed as a packaged workbook.

## Sources and dashboard outline

Keep the sources separate; they have different grains. Do not physically join
product and store marts to quality totals.

1. `bi.store_price_trends`: date, store, currency and price unit. Sum observation
   counts across groups; do not sum or average group medians to invent an overall
   median. Distinct product counts also cannot be summed into global uniqueness.
2. `bi.product_price_comparison`: date, product code, currency and unit. The latest
   available source name is a display label, not a translation. UNKNOWN units
   never produce an asserted comparable store gap.
3. `bi.pipeline_quality`: current publication only. Use its source/accepted/
   quarantine counts for whole-pipeline KPI cards, unaffected by retail filters.

Dashboard page status:

- **Market Overview (planned):** observed price trends at the stated grain, filtered by one
  currency and one unit. Counts represent submissions, not purchases or demand.
- **Product Comparison (planned):** one selected product, currency and unit; known-unit
  cross-store gaps and descriptive observed prices.
- **Pipeline Health (local v1 complete):** accepted/quarantine counts, unknown-unit
  coverage and location-reference checks. Source-capture/publication timestamps
  are available in the BI view; freshness cards remain planned.

All authored titles, controls and documentation are English. Original product and
place names remain source literals and may be multilingual or missing.

## Official references

- [Download Desktop Free Edition](https://www.tableau.com/products/desktop-free/download)
- [Desktop edition comparison](https://help.tableau.com/current/pro/desktop/en-us/desktop_comparison.htm)
- [Greenplum connector](https://help.tableau.com/current/pro/desktop/en-us/examples_greenplum.htm)
