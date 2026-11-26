{# PKW election results (PLG-029, #486): turnout in per cent as published, Poland, voivodeships, powiats and gminas;
   0 to 100 by definition (measured 19.81 to 90.88 on 8,670 rows, Dev trial of #3084, 26 Nov 2026). #}
{% macro poc_value_range__pkw() %}
    {{ return({
        'bounds': [
            ['voter_turnout', '%', '0', '100'],
        ],
        'models': ['poc_metrics_voter_turnout'],
    }) }}
{% endmacro %}
