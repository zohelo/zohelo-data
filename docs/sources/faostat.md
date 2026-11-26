# FAOSTAT (bulk domain files) (`faostat`)

**Built (12 Nov 2026, register GL-022).** FAOSTAT publishes each domain as one zip of the whole domain ("All
Data (Normalized)"), listed with size, rows and update date in the bulk catalogue
`https://bulks-faostat.fao.org/production/datasets_E.json`. `faostat.yaml` lands two through the shared
`bulk_file` pattern, one request per entity and run, version = the file's Last-Modified: `emissions_totals`
(GT, agrifood systems emissions totals, 20.6 MB zip, 2,498,428 rows) and `land_use` (RL, 3.0 MB, 421,850
rows), 278–284 areas from 1961 (Dev trial 43159213097). Population (OA) is UN WPP re-disseminated (third-party
data, onboarded as `un_wpp`) and not landed. Bronze: `bronze.faostat_observations` (one row per published row,
values as text, DuckDB CSV reader). Silver: `silver.faostat_observations` (grain entity × area_code ×
item_code × element_code × year × source_code). Gold: `gold.build_faostat` — GHG emissions all sectors without
LULUCF (kt CO2eq, AR5, 1990–2024; KPI ghg_emissions) and land area (1000 ha as published, 1961–2024; KPI area,
whose unit is km² — 1000 ha = 10 km², converted only in a consumer, never in Silver or Gold) for the EU27
members (M49 codes, macro `poc_m49_eu27`), European Union (27) and World; flags E and I → estimated; projected
years (after the file's version year, Silver `is_projection`) are not built. Refresh full (D32), ceiling
exception #2815. Terms: CC BY 4.0 with FAO's additional terms (no use for the promotion of a commercial
enterprise), licence row `cc_by_4_0_fao`. Restart unit: one domain file.
