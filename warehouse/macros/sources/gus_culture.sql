{# gus_culture (register PLS-027): Gold registration, decision D34 (docs/source-registration.md). Block 'culture_institutions'
   (models/gold/poc_build_gus_culture.sql); areas by name as printed (lower case), matched to the TERYT voivodeship names
   of dim_geography, 'polska' = Poland. #}
{% macro poc_source__gus_culture() %}
    {{ return({'block_model': 'poc_build_gus_culture', 'blocks': [['culture_institutions', 'GUS culture and national heritage tables (annual)']]}) }}
{% endmacro %}

{% macro poc_geography__gus_culture() %}
gus_culture as (
    select
        'gus_culture' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_culture' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
