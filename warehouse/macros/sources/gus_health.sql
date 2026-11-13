{# gus_health (register PLS-004, health family): Gold registration, decision D34 (docs/source-registration.md). Block
   'health_care_resources' (models/gold/poc_build_gus_health.sql); areas by name as printed (lower case), matched to the
   TERYT voivodeship names of dim_geography, 'polska' = Poland. #}
{% macro poc_source__gus_health() %}
    {{ return({'block_model': 'poc_build_gus_health', 'blocks': [['health_care_resources', 'GUS health and health care tables (annual)']]}) }}
{% endmacro %}

{% macro poc_geography__gus_health() %}
gus_health as (
    select
        'gus_health' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_health' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
