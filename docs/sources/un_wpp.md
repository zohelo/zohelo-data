# UN World Population Prospects (WPP) (`un_wpp`)

**Extraction (2 Nov 2026).** The UN Population Division publishes each WPP revision as fixed
files; the declaration `un_wpp.yaml` lands the 2024 revision's "Demographic Indicators, medium
variant" CSV (gzip) through the shared `bulk_file` pattern as entity
`demographic_indicators_medium` (version = Last-Modified, download time when absent; written
only when the SHA-256 changed, after the gzip check). Restart unit: the whole file (one
download). Licence CC BY 3.0 IGO (register GL-039). The URL is from the register review and is
proven from Actions by the Dev trial; a new revision (every two years) means a new URL in the
declaration.

**Implemented (11 Nov 2026).** A second file, `population_by_age5_medium` (population by five-year age
group and sex, 1,759,905 rows, 30 MB gzip), gives the dependency ratios. Bronze: `bronze.un_wpp_rows`,
generic CSV rows (one header row per file with `field_names`, data rows with `field_values`, text).
Silver: `silver.un_wpp_demographic_indicators`
(555 locations × 1950-2101, 54 indicators typed) and `silver.un_wpp_population_age_groups` (broad age
groups summed from the five-year groups, dependency ratios; the five-year rows stay in Bronze, above the
~1M full-rebuild policy). Gold: `gold.build_un_wpp` for the EU27 members, the WPP group "European Union
(EU: 27)" (LocID 1551, EU27_2020) and World; every indicator is an `:estimate` (to 2023) and a `:projection`
(2024-2100, status forecast) measure; values, unit and scale as published (D33; population in thousands); latest revision only, no vintages; areas through
`poc_geography__un_wpp()` (`warehouse/macros/sources/un_wpp.sql`, static codes); draft KPIs total_fertility_rate, life_expectancy,
old_age_dependency (computed from the five-year age groups) and population_projection. Licence `unresolved`: no UN
page states the WPP terms (Dev probe 43098349843), so WPP is kept out of the published layer: no row for the
published KPI population, and its `kpi_v2_map` rows are needs-block. Monthly check (3rd, 06:25 UTC), full rebuild (D32).
