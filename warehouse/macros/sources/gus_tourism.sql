{# gus_tourism (register PLS-035): Gold registration, decision D34 (docs/source-registration.md). Block 'accommodation'
   (models/gold/poc_build_gus_tourism.sql); areas by name as printed (lower case), matched to the TERYT voivodeship names
   of dim_geography, 'polska' = Poland. #}
{% macro poc_source__gus_tourism() %}
    {{ return({'block_model': 'poc_build_gus_tourism', 'blocks': [['accommodation', 'GUS tourism tables: tourist accommodation (annual)']]}) }}
{% endmacro %}

{% macro poc_geography__gus_tourism() %}
gus_tourism as (
    select
        'gus_tourism' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_tourism' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
