{# Sejm (#481): held sitting days per month (a month has at most 31 days), votings and prints per month, Poland;
   largest months MAXMONTH_TRIAL (Dev trial TRIAL_ID). #}
{% macro poc_value_range__sejm() %}
    {{ return({
        'bounds': [
            ['sejm_sitting_days', '%', '1', '31'],
            ['sejm_votings', '%', '1', '3000'],
            ['sejm_prints', '%', '1', '1000'],
        ],
        'models': ['poc_metrics_sejm_sitting_days', 'poc_metrics_sejm_votings', 'poc_metrics_sejm_prints'],
    }) }}
{% endmacro %}
