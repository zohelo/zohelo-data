{# UNESCO UIS geography codes (D34 registration, docs/source-registration.md). Reads the source's own Silver
   data through poc_optional_ref, so models/gold/poc_map_geography.sql lists those models in its config meta
   `optional_parents` (ops/repo_check.py pairs the two). #}
{% macro poc_geography__unesco_uis() %}
-- UNESCO UIS: countries by ISO 3166-1 alpha-3 (dim_geography.iso3); regional units ('SDG: ...', 'WB: ...') have no
-- Gold key (_X), and the build stage builds only the EU27 members.
unesco_uis as (
    select
        'unesco_uis' as source_system,
        a.geo_unit as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct
            geo_unit,
            is_country_code
        from {{ poc_optional_ref('unesco_uis_observations', 'geo_unit:varchar, is_country_code:boolean') }}
    ) as a
    left join geo as g on a.is_country_code and g.level_name = 'country' and a.geo_unit = g.iso3
)
{% endmacro %}
