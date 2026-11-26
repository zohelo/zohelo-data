{{ config(severity='warn') }}
-- kgpsp_interventions: a year whose gmina tables lack a TERYT code of a city with districts (the city row and every
-- district: Wrocław 0264 6 codes, Łódź 1061 6, Kraków 1261 5, Warszawa 1465 19, Poznań 3064 6). Gold does not sum that
-- city's voivodeship and Poland for the year (poc_build_kgpsp_interventions.sql, same macro). A warning, not an error:
-- the producer's 2025 tables (13 Mar 2026) have one row per city; each run lists such years so a corrected file is
-- seen.
{{ poc_kgpsp_interventions_incomplete_cities(ref('kgpsp_interventions_counts')) }}
