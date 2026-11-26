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
