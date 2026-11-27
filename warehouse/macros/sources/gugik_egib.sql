{# gugik_egib (register PL-ENV-006): Gold registration, decision D34 (docs/source-registration.md). Blocks 'parcels'
   and 'buildings' (models/gold/poc_build_gugik_egib.sql): register counts of the KI EGiB aggregate WFS per powiat,
   voivodeship and Poland at each snapshot date. Source codes: 'PL', the voivodeship's 2-digit and the powiat's
   4-digit TERYT code (the filter of the request), mapped by code to dim_geography. #}
{% macro poc_source__gugik_egib() %}
    {{ return({'block_model': 'poc_build_gugik_egib', 'blocks': [
        ['parcels', 'GUGiK KI EGiB parcels (register count)'],
        ['buildings', 'GUGiK KI EGiB buildings (register count)'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__gugik_egib() %}
gugik_egib as (
    select
        'gugik_egib' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name in ('wojewodztwo', 'powiat')
    union all
    select
        'gugik_egib' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
