{# gddkia_mop (register PL-ENV-023): Gold registration, decision D34 (docs/source-registration.md). Block 'rest_areas'
   (models/gold/poc_build_gddkia_mop.sql): rest areas of the monthly MOP table by voivodeship and current category.
   Areas: the voivodeship name as printed (lower case, silver.gddkia_mop_rest_areas voivodeship_name) matched to the
   TERYT voivodeship names of dim_geography, 'PL' = Poland. A static list from geo, so the map never waits for this
   source's Silver table (no poc_optional_ref needed). #}
{% macro poc_source__gddkia_mop() %}
    {{ return({'block_model': 'poc_build_gddkia_mop', 'blocks': [['rest_areas', 'GDDKiA rest areas (MOP) on national roads']]}) }}
{% endmacro %}

{% macro poc_geography__gddkia_mop() %}
gddkia_mop as (
    select
        'gddkia_mop' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gddkia_mop' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
