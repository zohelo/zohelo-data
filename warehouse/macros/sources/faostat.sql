{# FAOSTAT geography codes (D34 registration, docs/source-registration.md). Reads the source's own Silver
   data through poc_optional_ref, so models/gold/poc_map_geography.sql lists those models in its config meta
   `optional_parents` (ops/repo_check.py pairs the two). #}
{% macro poc_geography__faostat() %}
-- FAOSTAT: FAO area codes; countries through their UN M49 code (poc_m49_eu27: the EU27 members), FAO aggregates
-- 5707 European Union (27) = EU27_2020 (current members, fixed composition) and 5000 World. Other areas have no
-- Gold key here (_X); the build stage builds only the mapped ones.
faostat_m49 as ({{ poc_m49_eu27() }}),

faostat as (
    select
        'faostat' as source_system,
        cast(a.area_code as varchar) as source_code,
        coalesce(
            g.geography_key,
            case a.area_code when 5707 then 'EUROSTAT:EU27_2020' when 5000 then 'WB:WLD' end,
            '_X'
        ) as geography_key
    from (
        select distinct
            area_code,
            area_code_m49
        from {{ poc_optional_ref('faostat_observations', 'area_code:integer, area_code_m49:varchar') }}
    ) as a
    left join faostat_m49 as m on a.area_code_m49 = m.m49 and a.area_code < 5000
    left join geo as g on g.level_name = 'country' and m.iso2 = g.geography_key
)
{% endmacro %}

{# FAOSTAT blocks (gold.build_faostat): one per domain item × element; areas: EU27 members (M49, macro
   poc_m49_eu27), FAO aggregates 5707 European Union (27) and 5000 World. #}
{% macro poc_faostat_blocks() %}
    select * from (
        values
        ('emissions_totals', '6829', '723113', 'GHG emissions, all sectors without LULUCF, CO2eq (AR5) (FAOSTAT)'),
        ('land_use', '6601', '5110', 'Land area (FAOSTAT)')
    ) as t (entity, item_code, element_code, block_name)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__faostat() %}
    {{ return({
        'block_model': 'poc_build_faostat',
        'activated': "select 'faostat', entity || ':' || item_code || '.' || element_code, block_name from (" ~ poc_faostat_blocks() ~ ")",
    }) }}
{% endmacro %}
