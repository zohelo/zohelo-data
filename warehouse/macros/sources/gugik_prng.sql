{# gugik_prng (register PL-ENV-002): Gold registration, decision D34 (docs/source-registration.md). Blocks 'miejscowosci'
   and 'obiekty_fizjograficzne' (models/gold/poc_build_gugik_prng.sql); areas by TERYT code as the register publishes
   them for gminas (7 digits) and their powiats (4) and voivodeships (2), matched to the TERC keys of dim_geography,
   'PL' = Poland. Static: read from geo only, so the map never waits for this source. #}
{% macro poc_source__gugik_prng() %}
    {{ return({'block_model': 'poc_build_gugik_prng', 'blocks': [
        ['miejscowosci', 'GUGiK PRNG localities (state register of geographical names)'],
        ['obiekty_fizjograficzne', 'GUGiK PRNG physiographic objects (state register of geographical names)'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__gugik_prng() %}
gugik_prng as (
    select
        'gugik_prng' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name in ('wojewodztwo', 'powiat', 'gmina') and g.terc_code is not null
    union all
    select
        'gugik_prng' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
