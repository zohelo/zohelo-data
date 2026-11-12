{# mrpips_social_assistance (register PLS-031): Gold registration, decision D34 (docs/source-registration.md). Block 'social_assistance'
   (models/gold/poc_build_mrpips_social_assistance.sql); areas by name as printed (lower case), matched to the TERYT voivodeship names
   of dim_geography, 'polska' = Poland. #}
{% macro poc_source__mrpips_social_assistance() %}
    {{ return({'block_model': 'poc_build_mrpips_social_assistance', 'blocks': [['social_assistance', 'MRPiPS-03 social assistance (benefits granted, annual)']]}) }}
{% endmacro %}

{% macro poc_geography__mrpips_social_assistance() %}
mrpips_social_assistance as (
    select
        'mrpips_social_assistance' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'mrpips_social_assistance' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
