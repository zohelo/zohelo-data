# UNESCO UIS (Data API selections) (`unesco_uis`)

**Built (12 Nov 2026, register GL-026).** The UIS bulk files (BDDS) hold whole themes under a release folder
(SDG.zip 34 MB, OPRI.zip 68 MB in 202602); the public UIS Data API returns an indicator's complete data in one
request (`https://api.uis.unesco.org/api/public/data/indicators?indicator=<code>`, at most 100,000 records).
`unesco_uis.yaml` lands two whole-indicator selections through the shared `bulk_file` pattern, version =
download time (unchanged bytes skipped): `gerd_gdp` (EXPGDP.TOT, GERD as % of GDP, 1996–2024) and
`tertiary_attainment` (EA.5T8.AG25T99 with .F and .M, short-cycle tertiary or higher, population 25+,
1970–2025): 10,689 records, every country and region (Dev trial 43159213097). Bronze:
`bronze.unesco_uis_observations` (one row per record, DuckDB JSON reader). Silver:
`silver.unesco_uis_observations` (grain entity × indicator_id × geo_unit × year). Gold: `gold.build_unesco_uis`
for the EU27 members (ISO alpha-3), sex as breakdown for attainment (KPIs gerd_gdp, tertiary_attainment).
Refresh full (D32). Terms: CC BY-SA 4.0: values derived from UIS data are published under CC BY-SA 4.0 with the credit "Source:
(Adapted from) UNESCO Institute for Statistics (UIS), <URL>, <date of extraction>"; licence row `cc_by_sa_4_0_uis`.
The UIS `kpi_v2_map` rows stay `needs-block` until publishing carries that licence per series (#2816). Restart unit: one selection.
