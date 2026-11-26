{# imgw_api (register PL-ENV-008): Gold registration, decision D34 (docs/source-registration.md). Block 'synop_daily'
   (D) of models/gold/poc_build_imgw_api.sql: Poland daily air temperature from the provisional API snapshots, status
   'provisional'; geography PL in the build stage, no geography map. #}
{% macro poc_source__imgw_api() %}
    {{ return({'block_model': 'poc_build_imgw_api', 'blocks': [
        ['synop_daily', 'IMGW synoptic stations, provisional API snapshots, daily (Poland)']
    ]}) }}
{% endmacro %}
