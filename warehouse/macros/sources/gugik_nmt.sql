{# gugik_nmt (register PL-ENV-005): Gold registration, decision D34 (docs/source-registration.md). Block 'sheets'
   (A, Poland and voivodeships) of models/gold/poc_build_gugik_nmt.sql, elevation sheet editions by service and data
   year. Area codes of the build stage: 'PL' for Poland and the 2-digit TERYT voivodeship code of the PRG boundary that
   contains the sheet's footprint centroid (silver.prg_units, teryt_code), mapped here to the TERYT voivodeships of
   dim_geography (static: reads only `geo`). #}
{% macro poc_source__gugik_nmt() %}
    {{ return({'block_model': 'poc_build_gugik_nmt', 'blocks': [
        ['sheets', 'GUGiK elevation sheet indexes (NMT, NMPT, LIDAR), editions by data year']
    ]}) }}
{% endmacro %}

{% macro poc_geography__gugik_nmt() %}
gugik_nmt as (
    select
        'gugik_nmt' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gugik_nmt' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
