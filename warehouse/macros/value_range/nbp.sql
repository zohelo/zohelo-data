{# NBP exchange rates: fx_rate (moved from tests/metrics_value_range.sql). #}
{% macro poc_value_range__nbp() %}
    {{ return({
        'bounds': [
            ['fx_rate', '%', '0', '100000'],
        ],
        'models': [
            'poc_metrics_fx_rate',
        ],
    }) }}
{% endmacro %}
