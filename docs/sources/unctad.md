# UNCTAD Data Hub (UNCTADstat) (`unctad`)

**Extraction (2 Nov 2026).** Every Data Hub table has a bulk file: one 7z archive with the whole
table as CSV, at a stable URL per report code
(`https://unctadstat-api.unctad.org/bulkdownload/US.PLSCI/US_PLSCI`). The declaration `unctad.yaml`
lands nine tables through the shared `bulk_file` pattern, one entity per table: FDI flows and
stocks, merchandise trade, services trade by category, digitally deliverable services, monthly
commodity prices, liner shipping connectivity, merchant fleet, seaborne trade and port calls (about
14 MB compressed in all). Tables that other sources already cover (GDP, population, CPI, exchange
rates) and the large creative-economy and bilateral-services tables are not declared. The server
sends no Last-Modified, so the version is the download time and a file is written only when its
SHA-256 changed. Restart unit: one table. Licence CC BY 3.0 IGO (register GL-021), attribution
"Source: United Nations Trade and Development (UNCTAD) Data Hub, unctadstat.unctad.org"; tables
with third-party series keep their own terms.

**Implemented (11 Nov 2026).** Every table's CSV has one shape: a time field (Year, Period or Quarter),
code/label pairs (Economy, Flow, Category…) and measures, each with "Footnote" and "Missing value"
columns. Bronze: `bronze.unctad_rows`, generic CSV rows (`field_names` / `field_values`, `|`-separated:
column names contain commas), extracted with the runner's 7-Zip. Silver: one model per table
(`silver.unctad_fdi_flows_stock`, `unctad_merchandise_trade`, `unctad_services_trade`,
`unctad_digital_services_trade`, `unctad_commodity_prices`, `unctad_liner_shipping_connectivity`,
`unctad_merchant_fleet`, `unctad_seaborne_trade`, `unctad_port_calls`) from the macro `unctad_table`:
period, economy, the other dimensions and the measures (value, footnote, missing reason) as JSON maps by
name. Gold: `gold.build_unctad` (FDI flows and stocks, merchandise and services exports and imports) for
the EU27 members (UNCTAD codes, France 250), the group "European Union (2020 …)" (3412, EU27_2020) and
World; KPIs fdi_inflow, exports_goods and exports_services, draft until the per-table third-party terms
(register GL-021 blocker; merchandise trade is synchronised with WTO) are checked. Weekly schedule (Tuesday
05:55 UTC), full rebuild when a table's bytes change (D32: each archive is the whole table); the services
table (807,733 rows) is rebuilt in full under the exception #2777 to #822.
