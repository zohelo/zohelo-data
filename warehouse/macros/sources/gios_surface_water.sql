{# GIOŚ surface-water status assessment 2019-2024 (register PL-ENV-011, #3312): Gold registration, decision D34
   (docs/source-registration.md). Block 'status' (models/gold/poc_build_gios_surface_water.sql): surface water bodies by
   status class per water category and assessment, Poland. #}
{% macro poc_source__gios_surface_water() %}
    {{ return({'block_model': 'poc_build_gios_surface_water', 'blocks': [
        ['status', 'GIOŚ surface water bodies by status class, assessment 2019-2024']
    ]}) }}
{% endmacro %}

{# The status layers carry no area code (Poland only): one static row, never the source's own table. #}
{% macro poc_geography__gios_surface_water() %}
gios_surface_water as (
    select
        'gios_surface_water' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
