{# gugik_orto (register PL-ENV-004): Gold registration, decision D34 (docs/source-registration.md). Block 'sheets'
   (A, Poland and voivodeships) of models/gold/poc_build_gugik_orto.sql, orthophoto sheet editions by data year. Area
   codes of the build stage: 'PL' for Poland and the 2-digit TERYT voivodeship code of the PRG boundary that contains
   the sheet's footprint centroid (silver.prg_units, teryt_code), mapped here to the TERYT voivodeships of
   dim_geography (static: reads only `geo`). #}
{% macro poc_source__gugik_orto() %}
    {{ return({'block_model': 'poc_build_gugik_orto', 'blocks': [
        ['sheets', 'GUGiK orthophoto sheet index, editions by data year']
    ]}) }}
{% endmacro %}

{% macro poc_geography__gugik_orto() %}
gugik_orto as (
    select
        'gugik_orto' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gugik_orto' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
