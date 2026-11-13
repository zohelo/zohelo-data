{# imgw_archive (register PL-ENV-009): Gold registration, decision D34 (docs/source-registration.md). Blocks
   'synop_daily' (D) and 'synop_monthly' (M) of models/gold/poc_build_imgw_archive.sql, Poland aggregates of the
   daily synoptic station data; geography is set to PL in the build stage (IMGW gives no voivodeship for its
   stations), so there is no geography map. #}
{% macro poc_source__imgw_archive() %}
    {{ return({'block_model': 'poc_build_imgw_archive', 'blocks': [
        ['synop_daily', 'IMGW synoptic stations, daily (Poland)'],
        ['synop_monthly', 'IMGW synoptic stations, monthly (Poland)']
    ]}) }}
{% endmacro %}
