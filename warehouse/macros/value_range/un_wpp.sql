{# UN WPP: thousand persons as published (D33); largest area World, WPP 2024 medium-variant peak about 10.3 million
   thousand in the 2080s (10,180,160.751 in 2100; Dev trial 43098260601). Moved from tests/metrics_value_range.sql. #}
{% macro poc_value_range__un_wpp() %}
    {{ return({
        'bounds': [
            ['population_projection', '%', '0', '2e7'],
        ],
        'models': [
            'poc_metrics_population_projection',
        ],
    }) }}
{% endmacro %}
