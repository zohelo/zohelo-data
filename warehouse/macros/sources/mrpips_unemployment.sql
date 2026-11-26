{# MRPiPS registered unemployment geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__mrpips_unemployment() %}
-- MRPiPS registered unemployment: the rate workbook's WOJ. and POW. columns as one 4-digit code (WWPP; WW00 the
-- voivodeship, 0000 Poland), generated from TERYT like the BDL codes.
mrpips_unemployment as (
    select
        'mrpips_unemployment' as source_system,
        rpad(terc_code, 4, '0') as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    where unit_level in ('wojewodztwo', 'powiat')
    union all
    select
        'mrpips_unemployment' as source_system,
        '0000' as source_code,
        'PL' as geography_key
)
{% endmacro %}
