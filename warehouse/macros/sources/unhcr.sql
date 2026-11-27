{# UNHCR geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__unhcr() %}
-- UNHCR Refugee Statistics (cf_type=ISO): countries of asylum by ISO 3166-1 alpha-3 (dim_geography iso3), built
-- from Gold's own list like oecd, so the dimensions never wait for the UNHCR source; UNHCR's own codes for
-- unknown or stateless origins are breakdowns, not areas.
unhcr as (
    select
        'unhcr' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}

{# UNHCR Refugee Statistics blocks (gold.build_unhcr): one per API selection; areas: EU27 members as countries of
   asylum (ISO alpha-3 through dim_geography.iso3). #}
{% macro poc_unhcr_blocks() %}
    select * from (
        values
        ('population_by_asylum', 'People under UNHCR mandate by country of asylum, end of year (UNHCR)'),
        ('poland_by_origin', 'People under UNHCR mandate in Poland by country of origin, end of year (UNHCR)')
    ) as t (entity, block_name)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__unhcr() %}
    {{ return({
        'block_model': 'poc_build_unhcr',
        'activated': "select 'unhcr', entity, block_name from (" ~ poc_unhcr_blocks() ~ ")",
    }) }}
{% endmacro %}
