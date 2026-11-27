{# ZUS statistical portal geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__zus_psz() %}
-- ZUS statistical portal (PLS-009): '00' = Poland; voivodeships by their 2-digit TERYT code (the Silver model matched
-- ZUS's voivodeship names to these codes). Built from TERYT, so the dimensions never wait for the ZUS source.
zus_psz as (
    select
        'zus_psz' as source_system,
        terc_code as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    where unit_level = 'wojewodztwo'
    union all
    select
        'zus_psz' as source_system,
        '00' as source_code,
        'PL' as geography_key
)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__zus_psz() %}
    {{ return({
        'block_model': 'poc_build_zus_psz',
        'blocks': [
            ['pensioners_dec', 'ZUS old-age pensioners in December'],
            ['pension_amount_dec', 'ZUS average old-age pension in December'],
            ['insured_pension', 'ZUS insured persons, pension insurance'],
            ['insured_sickness_voiv', 'ZUS insured persons, sickness insurance, by voivodeship'],
            ['insured_health', 'ZUS insured persons, health insurance'],
            ['foreign_insured', 'ZUS foreign citizens in pension insurance'],
            ['payers_voiv', 'ZUS active contribution payers, by voivodeship'],
            ['absence_voiv', 'ZUS sickness absence, by voivodeship'],
        ],
    }) }}
{% endmacro %}
