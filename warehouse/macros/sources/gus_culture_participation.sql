{# gus_culture_participation (register PLS-028): Gold registration, decision D34 (docs/source-registration.md). Block 'participation'
   (models/gold/poc_build_gus_culture_participation.sql); areas by name as printed (lower case), matched to the TERYT voivodeship names
   of dim_geography, 'polska' = Poland. #}
{% macro poc_source__gus_culture_participation() %}
    {{ return({'block_model': 'poc_build_gus_culture_participation', 'blocks': [['participation', 'GUS participation in culture survey (persons aged 15+)']]}) }}
{% endmacro %}

{% macro poc_geography__gus_culture_participation() %}
gus_culture_participation as (
    select
        'gus_culture_participation' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_culture_participation' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
