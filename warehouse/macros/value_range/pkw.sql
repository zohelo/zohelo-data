{# PKW election results (PLG-029, #486): turnout in per cent as published, Poland, voivodeships, powiats and gminas;
   0 to 100 by definition (measured range: TRIAL_RANGE, Dev trial TRIAL_ID). #}
{% macro poc_value_range__pkw() %}
    {{ return({
        'bounds': [
            ['voter_turnout', '%', '0', '100'],
        ],
        'models': ['poc_metrics_voter_turnout'],
    }) }}
{% endmacro %}
