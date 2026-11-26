{# zus_yearbook (register PLS-010): Gold registration, decision D34 (docs/source-registration.md). Blocks 'payers_voiv'
   and 'insured_voiv' (models/gold/poc_build_zus_yearbook.sql); areas by name as printed (lower case), matched to the
   TERYT voivodeship names of dim_geography, 'polska' = Poland. #}
{% macro poc_source__zus_yearbook() %}
    {{ return({'block_model': 'poc_build_zus_yearbook', 'blocks': [
        ['payers_voiv', 'ZUS yearbook: contribution payers by voivodeship, 31 December'],
        ['insured_voiv', 'ZUS yearbook: insured persons by voivodeship, 31 December'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__zus_yearbook() %}
zus_yearbook as (
    select
        'zus_yearbook' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'zus_yearbook' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
