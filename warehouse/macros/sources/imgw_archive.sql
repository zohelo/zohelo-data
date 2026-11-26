{# imgw_archive (register PL-ENV-009): Gold registration, decision D34 (docs/source-registration.md). Blocks
   'synop_daily' (D, Poland) and 'synop_monthly' (M, Poland and voivodeships) of models/gold/poc_build_imgw_archive.sql,
   aggregates of the daily synoptic station data. Area codes of the build stage: 'PL' for Poland and the
   2-digit TERYT voivodeship code of the PRG boundary that contains the station (silver.prg_units, teryt_code),
   mapped here to the TERYT voivodeships of dim_geography (static: reads only `geo`). #}
{% macro poc_source__imgw_archive() %}
    {{ return({'block_model': 'poc_build_imgw_archive', 'blocks': [
        ['synop_daily', 'IMGW synoptic stations, daily (Poland)'],
        ['synop_monthly', 'IMGW synoptic stations, monthly (Poland, voivodeships)']
    ]}) }}
{% endmacro %}

{% macro poc_geography__imgw_archive() %}
imgw_archive as (
    select
        'imgw_archive' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'imgw_archive' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
