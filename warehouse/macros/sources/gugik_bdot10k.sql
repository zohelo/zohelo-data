{# gugik_bdot10k (register PL-ENV-003): Gold registration, decision D34 (docs/source-registration.md). Block
   'powiat_packages' (models/gold/poc_build_gugik_bdot10k.sql); areas by the first two digits of the powiat TERYT code
   the package index gives (voivodeship TERC), matched to the TERC keys of dim_geography, 'PL' = Poland. Static: read
   from geo only, so the map never waits for this source. #}
{% macro poc_source__gugik_bdot10k() %}
    {{ return({'block_model': 'poc_build_gugik_bdot10k', 'blocks': [
        ['powiat_packages', 'GUGiK BDOT10k powiat download packages'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__gugik_bdot10k() %}
gugik_bdot10k as (
    select
        'gugik_bdot10k' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo' and g.terc_code is not null
    union all
    select
        'gugik_bdot10k' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
