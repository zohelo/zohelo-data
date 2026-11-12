# GUS NSP 2021 census tables (`gus_nsp2021`)

**Built (12 Nov 2026, register PLS-005, #2897).** One-off census: the final-results pages link one XLSX of tables per
theme at fixed `/download/` URLs, declared as files of the shared `bulk_file` pattern (no extractor code): themes
`ludnosc_stan_struktura` and `rynek_pracy`. Entity = one workbook, version = download time when the SHA-256 changed,
restart unit one file. Bronze `bronze.gus_nsp2021_cells` (`ingestion/common/gus_publications.py`); Silver
`silver.gus_nsp2021_tables` with the curated tables found by title and header (`warehouse/macros/gus_tables.sql`):
population 2011 and 2021 (total, urban, rural, urban share) and census activity, employment and unemployment rates by
voivodeship. Gold block `census_2021` (annual, census year as period), registered in
`warehouse/macros/sources/gus_nsp2021.sql`; census values are benchmarks and are not added to the population or
unemployment KPIs; KPI `urban_population_share` (draft). Housing indicators: Task #2921. Quarterly check. Licence: GUS
terms (source given; commercial reuse not addressed).
