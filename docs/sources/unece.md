# UNECE Statistical Database (PXWeb) (`unece`)

**Built (11 Nov 2026, register GL-038).** No bulk feature: the database (PXWeb 2015) does not serve
its PX files, and its PXWeb API v1 (`https://w3.unece.org/PXWeb2015/api/v1/en/STAT`) answers data only
to POST queries. `ingestion/to_landing/unece/` therefore lists the tables of the economic statistics
folder `20-ME` (macroeconomic overview, national accounts, labour force and wages, price indices,
industrial production, external economic relations, globalization) and queries each whole as JSON-stat
2.0; a table above the API cell limit (HTTP 403) is queried per value of its first variable and lands
as entities `<table>__p<value code>` (Silver keeps only the newer of the whole and part shapes). Version = the table's `updated` time (the API restamps tables daily, so the
SHA-256 decides whether a copy is written). One request at a time with a 4 s pause: the API answers
bursts with 429. Bronze: `bronze.unece_observations` (the generic SDMX format of the OECD onboarding:
variable codes as series_key (`|`-separated), period label, value and status) and `bronze.unece_codes` (variable and
value labels per file). Silver: `silver.unece_observations` (codes and labels as JSON, area = the
Country variable: M49 codes and aggregates such as E27 = EU-27). Terms: UNECE copyright notice (free
reuse incl. commercial, acknowledge UNECE), licence row `unece_terms`. Weekly Monday check. Restart
unit: one table.
Gold (#2861, #2885): `gold.build_unece` — nine of the 29 tables, the measures of macro `poc_unece_measures`
(`warehouse/macros/sources/unece.sql`): unemployment rate (annual, monthly), consumer and producer price indices
(annual, quarterly), gross average monthly wages, industrial production, population and employment, exchange
rates and PPPs (annual), total employment (quarterly), for the 56 countries, the EU-27 and the euro area; additional
source of KPIs `unemployment_rate`, `unemployment_rate_monthly_sa`, `cpi_annual_rate`, `ppi_annual_rate`,
`industrial_production` and `population`. The other 20 tables (about 2.3 million rows: GDP by expenditure and
activity, the overviews, monthly prices, exchange rates and industrial production) are outside this curated
selection and reach Gold with the per-block full coverage (D35, docs/gold-coverage.md). `tests/unece_measures_match.sql` fails when a built table's codes change.
