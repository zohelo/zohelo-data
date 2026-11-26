{# GUS census tables (#2897): urban share in % (Dev trial 43288120956). #}
{% macro poc_value_range__gus_nsp2021() %}
    {{ return({
        'bounds': [
            ['urban_population_share', '%', '0', '100'],
        ],
        'models': ['poc_metrics_urban_population_share'],
    }) }}
{% endmacro %}
