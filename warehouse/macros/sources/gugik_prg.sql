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
