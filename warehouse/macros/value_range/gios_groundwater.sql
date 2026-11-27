{# GIOŚ groundwater monitoring (#2870): share of monitoring points with a sample above 50 mg/l nitrate, a per cent of
   points, 0 to 100 by definition (Dev probe 45810873298: 36 to 67 of 716 to 1,404 nitrate results a year above 50
   mg/l in Poland). #}
{% macro poc_value_range__gios_groundwater() %}
    {{ return({
        'bounds': [
            ['groundwater_nitrate_above_50_share', '%', '0', '100'],
        ],
        'models': ['poc_metrics_groundwater_nitrate_above_50_share'],
    }) }}
{% endmacro %}
