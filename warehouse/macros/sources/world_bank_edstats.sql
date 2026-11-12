{# OECD PISA mean scores through the World Bank EdStats republication (gold.build_world_bank_edstats): one block per
   subject; areas: EU27 members (ISO alpha-3 through dim_geography.iso3). #}
{% macro poc_world_bank_edstats_blocks() %}
    select * from (
        values
        ('pisa_reading', 'LO.PISA.REA', 'PISA mean score, reading (OECD via World Bank EdStats)'),
        ('pisa_mathematics', 'LO.PISA.MAT', 'PISA mean score, mathematics (OECD via World Bank EdStats)'),
        ('pisa_science', 'LO.PISA.SCI', 'PISA mean score, science (OECD via World Bank EdStats)')
    ) as t (entity, indicator_id, block_name)
{% endmacro %}

{# D34 registration (docs/source-registration.md): the Gold block and the geography of World Bank EdStats (#2370). #}
{% macro poc_source__world_bank_edstats() %}
    {{ return({
        'block_model': 'poc_build_world_bank_edstats',
        'activated': "select 'world_bank_edstats', entity, block_name from (" ~ poc_world_bank_edstats_blocks() ~ ")",
    }) }}
{% endmacro %}

{% macro poc_geography__world_bank_edstats() %}
-- World Bank EdStats (OECD PISA republication): countries by ISO 3166-1 alpha-3 (dim_geography iso3), built from
-- Gold's own list like oecd, so the dimensions never wait for the source; World Bank aggregates (OED, ...) are not
-- in this list, so the build stage's inner join leaves them out.
world_bank_edstats as (
    select
        'world_bank_edstats' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}
