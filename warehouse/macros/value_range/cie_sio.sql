{# SIO teachers (#3130): persons, all grades, Poland 30 Sep 2006 = 649,745 (Dev probe 43800731510). #}
{% macro poc_value_range__cie_sio() %}
    {{ return({
        'bounds': [
            ['teachers', '%', '0', '2e6'],
        ],
        'models': ['poc_metrics_teachers'],
    }) }}
{% endmacro %}
