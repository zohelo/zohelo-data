{# ECB geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__ecb() %}
-- ECB: ISO two-letter countries (GR, GB) and the aggregates of poc_ecb_areas (U2, I8, I9, B6, B0, D0); static
-- codes, so Gold does not read ECB Silver here. Countries come from dim_geography's country keys.
ecb_areas as ({{ poc_ecb_areas() }}),

ecb as (
    select
        'ecb' as source_system,
        area_code as source_code,
        candidate_key as geography_key
    from ecb_areas
    union all
    select
        'ecb' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
)
{% endmacro %}
