{# GDDKiA traffic census (#2999): length-weighted SDRR of motor vehicles on national roads, vehicles per day;
   evidence in the PR's Dev trial. #}
{% macro poc_value_range__gddkia_gpr() %}
    {{ return({
        'bounds': [
            ['average_daily_traffic_national_roads', '%', '1000', '1e5'],
        ],
        'models': ['poc_metrics_average_daily_traffic_national_roads'],
    }) }}
{% endmacro %}
