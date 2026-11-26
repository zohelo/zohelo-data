{# kgpsp_interventions (register EX-003): Gold registration, decision D34 (docs/source-registration.md). Blocks
   'incidents' (table 1) and 'casualties' (table 10) of models/gold/poc_build_kgpsp_interventions.sql. Areas: the
   two-digit TERYT voivodeship code (silver.kgpsp_interventions_counts voivodeship_code), matched to dim_geography's
   terc_code; 'PL' = Poland. A static list from geo, so the shared map reads no Silver table of this source. #}
{% macro poc_source__kgpsp_interventions() %}
    {{ return({'block_model': 'poc_build_kgpsp_interventions', 'blocks': [
        ['incidents', 'KG PSP incidents by type and size'],
        ['casualties', 'KG PSP casualties of fires and local hazards'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__kgpsp_interventions() %}
kgpsp_interventions as (
    select
        'kgpsp_interventions' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'kgpsp_interventions' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}

{# The gmina years (from 2023) whose tables do not print a city with districts with all its TERYT codes: the city
   row WWPP01 and its districts WWPP02 ... (Wrocław 0264: 6 codes, Łódź 1061: 6, Kraków 1261: 5, Warszawa 1465: 19,
   Poznań 3064: 6). One row per year, table and city with a code missing, its voivodeship and the codes missing.
   Read by the Gold build (such a voivodeship and Poland are not summed) and by the warning test
   tests/kgpsp_interventions_city_districts.sql, so both use one rule. #}
{% macro poc_kgpsp_interventions_incomplete_cities(silver) %}
select
    y.report_year,
    y.table_code,
    c.city_code,
    left(c.city_code, 2) as voivodeship_code,
    c.codes as codes_expected,
    count(s.area_code) as codes_printed,
    string_agg(c.area_code, ', ' order by c.area_code) filter (where s.area_code is null) as codes_missing
from (select distinct report_year, table_code from {{ silver }} where area_level = 'gmina') as y
cross join (
    select
        t.city_code,
        t.codes,
        t.city_code || lpad(cast(r.n as varchar), 2, '0') as area_code
    from (values ('0264', 6), ('1061', 6), ('1261', 5), ('1465', 19), ('3064', 6)) as t (city_code, codes)
    inner join range(1, 20) as r (n) on r.n <= t.codes
) as c
left join (select distinct report_year, table_code, area_code from {{ silver }} where area_level = 'gmina') as s
    on y.report_year = s.report_year and y.table_code = s.table_code and c.area_code = s.area_code
group by y.report_year, y.table_code, c.city_code, c.codes
having count(s.area_code) < c.codes
{% endmacro %}
