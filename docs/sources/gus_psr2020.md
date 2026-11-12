# GUS PSR 2020 agricultural census tables (`gus_psr2020`)

**Built (12 Nov 2026, register PLS-006, #2897).** One-off census: part 2 of "Charakterystyka gospodarstw rolnych w
2020 r." (tables by voivodeship) is a declared file of the shared `bulk_file` pattern (`charakterystyka_cz2`). Entity
= one workbook, version = download time when the SHA-256 changed, restart unit one file. Bronze
`bronze.gus_psr2020_cells`; Silver `silver.gus_psr2020_tables`: holdings 2010 and 2020, agricultural land, average
agricultural land per holding, cattle, pigs by voivodeship. Gold block `agricultural_census_2020`, registered in
`warehouse/macros/sources/gus_psr2020.sql`; KPIs `agricultural_holdings`, `average_farm_area` (draft). Part 1 and the
.xls report annex: Task #2922. Quarterly check. Licence: GUS terms.
