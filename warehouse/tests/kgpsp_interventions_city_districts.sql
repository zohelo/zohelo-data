{{ config(severity='warn') }}
-- kgpsp_interventions: a year whose gmina tables print a city with districts (TERYT: Wrocław 0264, Łódź 1061, Kraków
-- 1261, Warszawa 1465, Poznań 3064) as a single row. Gold does not sum that city's voivodeship and Poland for the year
-- (poc_build_kgpsp_interventions.sql). A warning, not an error: the producer's 2025 tables (13 Mar 2026) have one row
-- per city; each run lists such years so a corrected file is seen.
select
    report_year,
    table_code,
    left(area_code, 4) as city_code,
    count(distinct area_code) as rows_printed
from {{ ref('kgpsp_interventions_counts') }}
where area_level = 'gmina' and left(area_code, 4) in ('0264', '1061', '1261', '1465', '3064')
group by report_year, table_code, left(area_code, 4)
having count(distinct area_code) < 2
