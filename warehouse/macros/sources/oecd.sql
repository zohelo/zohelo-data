{# OECD geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__oecd() %}
-- OECD (#2738): countries by ISO 3166 alpha-3 (dim_geography iso3); EU27_2020 is the Eurostat aggregate, the
-- aggregates of macro poc_oecd_areas are OECD:<code> members. Built from Gold's own lists, not from
-- silver.oecd_cli, so the dimensions never wait for the OECD source; codes outside them are unmatched (_X).
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
        'EU27_2020' as source_code,
        'EUROSTAT:EU27_2020' as geography_key
    union all
    select
        'oecd' as source_system,
        replace(g.geography_key, 'OECD:', '') as source_code,
        g.geography_key
    from geo as g
    where g.geography_key like 'OECD:%'
)
{% endmacro %}
