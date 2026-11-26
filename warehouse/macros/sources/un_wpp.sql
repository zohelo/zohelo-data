{# UN WPP geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__un_wpp() %}
-- UN WPP: locations by ISO two-letter code (countries and areas, matched to dim_geography's country keys) and the
-- groups of poc_un_wpp_areas by LocID (1551 European Union (EU: 27) = EU27_2020, 900 World); static, so Gold does
-- not read WPP Silver here. Other groups have no Gold area (_X); the build stage builds only poc_un_wpp_blocks areas.
un_wpp_areas as ({{ poc_un_wpp_areas() }}),

un_wpp as (
    select
        'un_wpp' as source_system,
        loc_id as source_code,
        candidate_key as geography_key
    from un_wpp_areas
    union all
    select
        'un_wpp' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
)
{% endmacro %}
