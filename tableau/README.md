# Tableau Desktop setup

The user selected Tableau Desktop for the local BI layer. Desktop Free Edition
supports live database connections and local analysis. Its publishing and packaged
workbook sharing capabilities differ from Professional Edition. No paid license,
trial or account has been created by this project.

## Installation status

Tableau Desktop is **not installed or activated yet**. The official 2026.3 download
page requires a registration form. The user must supply their own details and
download the official installer. Command-line attempts to retrieve the installer
returned HTTP 403; the old 2024 winget package was not installed.

The three `.tds` files are hand-authored connection templates containing no
passwords or embedded records. They are XML-validated, but have **not yet been
opened or validated in Tableau Desktop**. If the installed connector requires
additional metadata, create the connection using the UI settings below instead.

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

Tableau may require a connector driver. Use the driver link displayed by the
installed Tableau version. Do not expose the demo database to the public network.
Compatibility of this exact Desktop version/driver with Greenplum 7.0.0 build dev
remains unverified until an actual Desktop connection succeeds.

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

Proposed local dashboard pages, not yet authored in Desktop:

- **Market Overview:** observed price trends at the stated grain, filtered by one
  currency and one unit. Counts represent submissions, not purchases or demand.
- **Product Comparison:** one selected product, currency and unit; known-unit
  cross-store gaps and descriptive observed prices.
- **Pipeline Health:** accepted/quarantine counts, unknown-unit coverage and
  last source-capture/publication times.

All authored titles, controls and documentation are English. Original product and
place names remain source literals and may be multilingual or missing.

## Official references

- [Download Desktop Free Edition](https://www.tableau.com/products/desktop-free/download)
- [Desktop edition comparison](https://help.tableau.com/current/pro/desktop/en-us/desktop_comparison.htm)
- [Greenplum connector](https://help.tableau.com/current/pro/desktop/en-us/examples_greenplum.htm)
