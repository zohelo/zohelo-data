{# GIOŚ surface-water status (#3312): share of the assessed surface water bodies in good overall status, a per cent of
   water bodies, 0 to 100 by definition (Dev trial TRIAL: SPOT). #}
{% macro poc_value_range__gios_surface_water() %}
    {{ return({
        'bounds': [
            ['surface_water_good_status_share', '%', '0', '100'],
        ],
        'models': ['poc_metrics_surface_water_good_status_share'],
    }) }}
{% endmacro %}
