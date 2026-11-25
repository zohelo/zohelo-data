{# TED notices of Polish buyers per publication month (#2976): Poland 7,513 to 12,578, largest voivodeship 3,448
   (Mazowieckie, Oct 2025) (Dev trial 43344674034); about 115,000 a year in 2025. #}
{% macro poc_value_range__ted_notices() %}
    {{ return({
        'bounds': [
            ['procurement_notices', '%', '0', '1e5'],
        ],
        'models': ['poc_metrics_procurement_notices'],
    }) }}
{% endmacro %}
