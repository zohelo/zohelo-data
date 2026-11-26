{# OECD geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__oecd() %}
-- OECD (#2738): countries by ISO 3166 alpha-3 (dim_geography iso3); the codes of macro poc_oecd_areas that equal a
-- Eurostat aggregate (EU27_2020, EA20, EA19; #2775) map to it, the others are OECD:<code> members. Built from Gold's
-- own lists, not from silver.oecd_cli, so the dimensions never wait for the OECD source; codes outside them are
-- unmatched (_X).
oecd_areas as ({{ poc_oecd_areas() }}),

oecd as (
    select
        'oecd' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'oecd' as source_system,
        ref_area as source_code,
        'EUROSTAT:' || eurostat_code as geography_key
    from oecd_areas
    where eurostat_code <> ''
    union all
    select
        'oecd' as source_system,
        replace(g.geography_key, 'OECD:', '') as source_code,
        g.geography_key
    from geo as g
    where g.geography_key like 'OECD:%'
)
{% endmacro %}
