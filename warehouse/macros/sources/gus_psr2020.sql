{# gus_psr2020 (register PLS-006): Gold registration, decision D34 (docs/source-registration.md). Block 'agricultural_census_2020'
   (models/gold/poc_build_gus_psr2020.sql); areas by name as printed (lower case), matched to the TERYT voivodeship names
   of dim_geography, 'polska' = Poland. #}
{% macro poc_source__gus_psr2020() %}
    {{ return({'block_model': 'poc_build_gus_psr2020', 'blocks': [['agricultural_census_2020', 'GUS PSR 2020 agricultural census (holdings by voivodeship)']]}) }}
{% endmacro %}

{% macro poc_geography__gus_psr2020() %}
gus_psr2020 as (
    select
        'gus_psr2020' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_psr2020' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
