# WHO data.who.int (indicator files) (`who`)

**Built (12 Nov 2026, register GL-025).** Every data.who.int indicator page links its complete data as one CSV,
`https://srhdpeuwpubsa.blob.core.windows.net/whdh/DATADOT/INDICATOR/<uid>_ALL_LATEST.csv`. `who.yaml` lands two
through the shared `bulk_file` pattern, version = the file's Last-Modified: `life_expectancy` (90E2E48,
WHOSIS_000001) and `hale` (C64284D, WHOSIS_000002), every country, WHO region, income group and World, 2000–2021,
by sex: 25,872 rows (Dev trial 43159213097). This is the CC BY 4.0 route to the GHO indicators; the GHO OData
API (GL-024) stays parked for its terms. Bronze: `bronze.who_observations` (CSV columns lower-cased, values as
text). Silver: `silver.who_observations` (grain entity × geo_type × geo_code_m49 × year × sex, value with
bounds). Gold: `gold.build_who` — both indicators for the EU27 members (M49, macro `poc_m49_eu27`) and World, sex
as breakdown (KPI life_expectancy). Refresh full (D32). Terms: CC BY 4.0 with WHO's additional terms, licence
row `cc_by_4_0_who`. Restart unit: one indicator file.
