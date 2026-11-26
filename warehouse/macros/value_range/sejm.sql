{# Sejm (#481): held sitting days per month (a month has at most 31 days), votings and prints per month, Poland;
   largest months: 9 sitting days (2026-07), 709 votings (2020-02), 175 prints (2025-07) (Dev trial 45755790301). #}
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
