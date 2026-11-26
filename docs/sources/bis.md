# BIS Data Portal (SDMX) (`bis`)

**Built (11 Nov 2026, register GL-015).** The BIS Data Portal has csv_flat bulk zips per dataflow
(`https://data.bis.org/bulkdownload`), but they decorate every code with its label, put series
attributes on separate rows and carry the daily series (WS_CBPOL 476 MB unzipped). The SDMX REST API
returns the same dataflow as SDMX-CSV 2.0 codes (`https://stats.bis.org/api/v1/data/<dataflow>/<key>/all?format=sdmx-csv`),
one response per selection. `bis.yaml` lands six selections through the shared `bulk_file` pattern, one
request per entity and run: `cbpol` (WS_CBPOL, monthly), `eer` (WS_EER, monthly), `spp` (WS_SPP), `tc`
(WS_TC), `credit_gap` (WS_CREDIT_GAP), `dsr` (WS_DSR); 371,000 observations, 64 MB, Poland in all six
(Dev probe 43087048719). No Last-Modified, so version = download time; a copy is written only when the
SHA-256 changed. Bronze: `bronze.bis_observations` (the generic SDMX format of the OECD onboarding).
Silver: `silver.bis_observations` (dimensions as JSON, area from REF_AREA or BORROWERS_CTY, series
attributes sent on rows without a period filled onto their observations, `NaN` as no value). Terms:
BIS permitted use of statistics (cite the BIS; no additional charge to users; no warranty), licence row
`bis_terms`. Weekly Thursday check. Restart unit: one selection.
Gold (#2861): `gold.build_bis` — the series of macro `poc_bis_measures` (`warehouse/macros/sources/bis.sql`) (policy rate, broad-basket effective exchange rates,
residential property prices, credit as % of GDP, credit-to-GDP gap, debt service ratios), each its own measure,
for the countries the BIS publishes and the euro area; additional source of KPI `reer` (next to the World Bank), draft KPIs
`policy_rate`, `house_price_growth`, `real_house_price_growth`, `credit_to_gdp`, `debt_service_ratio`.
`tests/bis_gold_units.sql` fails when the BIS changes the unit of a built series.
