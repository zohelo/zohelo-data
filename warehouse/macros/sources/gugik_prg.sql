{# gugik_prg (registers PL-ENV-001 boundaries, PL-ENV-007 address points): Gold registration, decision D34
   (docs/source-registration.md). Moved here from the lists of poc_blocks.sql when the second block was added (#3106).
   Block 'jednostki_administracyjne': the area of every administrative unit (silver.prg_units); block
   'punkty_adresowe': the number of address points per gmina, powiat, voivodeship and Poland
   (models/gold/poc_prg_address_counts.sql). One build stage, models/gold/poc_build_prg.sql. #}
{% macro poc_source__gugik_prg() %}
    {{ return({'block_model': 'poc_build_prg', 'blocks': [
        ['jednostki_administracyjne', 'PRG administrative units'],
        ['punkty_adresowe', 'PRG address points'],
    ]}) }}
{% endmacro %}

{# GUGiK PRG geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__gugik_prg() %}
-- The PRG unit codes (JPT_KOD_JE: TERC, '0' the country) are TERC keys (#2739).
gugik_prg as (
    select distinct
        'gugik_prg' as source_system,
        teryt_code as source_code,
        case when unit_level = 'kraj' then 'PL' else 'TERC:' || teryt_code end as geography_key
    from {{ ref('prg_units') }}
    where unit_level in ('kraj', 'wojewodztwo', 'powiat', 'gmina')
)
{% endmacro %}
