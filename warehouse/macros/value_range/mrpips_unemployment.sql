{# MRPiPS (#2778): persons, Poland and voivodeships, end of month (moved from
   tests/metrics_value_range.sql). #}
{% macro poc_value_range__mrpips_unemployment() %}
    {{ return({
        'bounds': [
            ['registered_unemployed', '%', '0', '5e6'],
        ],
        'models': [
            'poc_metrics_registered_unemployed',
        ],
    }) }}
{% endmacro %}
