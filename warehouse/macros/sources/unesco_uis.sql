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

{# UNESCO UIS blocks (gold.build_unesco_uis): one per Data API selection; areas: EU27 members (ISO alpha-3 through
   dim_geography.iso3). #}
{% macro poc_unesco_uis_blocks() %}
    select * from (
        values
        ('gerd_gdp', 'EXPGDP.TOT', 'GERD as a percentage of GDP (UIS)', 'pct_gdp'),
        ('tertiary_attainment', 'EA.5T8.AG25T99', 'Completed short-cycle tertiary or higher, population 25+ (UIS)', 'pct')
    ) as t (entity, indicator_id, block_name, unit_code)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__unesco_uis() %}
    {{ return({
        'block_model': 'poc_build_unesco_uis',
        'activated': "select 'unesco_uis', entity, block_name from (" ~ poc_unesco_uis_blocks() ~ ")",
    }) }}
{% endmacro %}
