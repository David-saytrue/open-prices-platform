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
Successful authentication, data retrieval and the other two templates still need
Desktop validation.

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
