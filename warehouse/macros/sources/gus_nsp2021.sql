{# gus_nsp2021 (register PLS-005): Gold registration, decision D34 (docs/source-registration.md). Block 'census_2021'
   (models/gold/poc_build_gus_nsp2021.sql); areas by name as printed (lower case), matched to the TERYT voivodeship names
   of dim_geography, 'polska' = Poland. #}
{% macro poc_source__gus_nsp2021() %}
    {{ return({'block_model': 'poc_build_gus_nsp2021', 'blocks': [['census_2021', 'GUS NSP 2021 census final results (reference date 31 March)']]}) }}
{% endmacro %}

{% macro poc_geography__gus_nsp2021() %}
gus_nsp2021 as (
    select
        'gus_nsp2021' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_nsp2021' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
