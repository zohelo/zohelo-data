{# WHO data.who.int geography codes (D34 registration, docs/source-registration.md). Reads the source's own Silver
   data through poc_optional_ref, so models/gold/poc_map_geography.sql lists those models in its config meta
   `optional_parents` (ops/repo_check.py pairs the two). #}
{% macro poc_geography__who() %}
-- WHO data.who.int: areas by M49 code (unpadded) and type; countries through poc_m49_eu27 (the EU27 members),
-- World (GLOBAL 1) = WB:WLD. WHO regions and income groups have no Gold key (_X); the build stage builds only the
-- mapped areas. source_code = <type>:<code>, as region and country codes share one number space.
who_m49 as ({{ poc_m49_eu27() }}),

who as (
    select
        'who' as source_system,
        a.geo_type || ':' || a.geo_code_m49 as source_code,
        coalesce(g.geography_key, case when a.geo_type = 'GLOBAL' and a.geo_code_m49 = '1' then 'WB:WLD' end, '_X')
            as geography_key
    from (
        select distinct
            geo_type,
            geo_code_m49
        from {{ poc_optional_ref('who_observations', 'geo_type:varchar, geo_code_m49:varchar') }}
    ) as a
    left join who_m49 as m on a.geo_type = 'COUNTRY' and lpad(a.geo_code_m49, 3, '0') = m.m49
    left join geo as g on g.level_name = 'country' and m.iso2 = g.geography_key
)
{% endmacro %}

{# WHO data.who.int blocks (gold.build_who): one per indicator file; areas: EU27 members (M49, macro
   poc_m49_eu27) and World (M49 1, GLOBAL). #}
{% macro poc_who_blocks() %}
    select * from (
        values
        ('life_expectancy', 'Life expectancy at birth (WHO)'),
        ('hale', 'Healthy life expectancy at birth (WHO)')
    ) as t (entity, block_name)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__who() %}
    {{ return({
        'block_model': 'poc_build_who',
        'activated': "select 'who', entity, block_name from (" ~ poc_who_blocks() ~ ")",
    }) }}
{% endmacro %}
