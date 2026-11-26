{# GUS census tables (#2897): holdings PL 2010 = 1,509,148; ha per holding, highest 22.22 (lubuskie 2020) (Dev trial
   43288120956). #}
{% macro poc_value_range__gus_psr2020() %}
    {{ return({
        'bounds': [
            ['agricultural_holdings', '%', '0', '3e6'],
            ['average_farm_area', '%', '0', '100'],
        ],
        'models': ['poc_metrics_agricultural_holdings', 'poc_metrics_average_farm_area'],
    }) }}
{% endmacro %}
