{# mz_health_bulletin (register PLS-017, #2957): Gold registration, decision D34 (docs/source-registration.md). Block
   'health_bulletin' (models/gold/poc_build_mz_health_bulletin.sql); areas by name as printed (lower case), matched to
   the TERYT voivodeship names of dim_geography, 'polska' = Poland. A static list: the map reads no Silver table of
   this source, so it never waits for the source's first load (no poc_optional_ref needed). #}
{% macro poc_source__mz_health_bulletin() %}
    {{ return({'block_model': 'poc_build_mz_health_bulletin', 'blocks': [['health_bulletin', 'MZ statistical bulletin: health workforce and general hospitals (annual)']]}) }}
{% endmacro %}

{% macro poc_geography__mz_health_bulletin() %}
mz_health_bulletin as (
    select
        'mz_health_bulletin' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'mz_health_bulletin' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
