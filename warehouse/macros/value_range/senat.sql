{# Senate (#482): sitting days per month of finished sittings (a month has at most 31 days) and votes per month,
   Poland; largest month MAXVOTES_TRIAL (Dev trial TRIAL_ID). A month without a sitting has no row, so the
   lower bound is 1. #}
{% macro poc_value_range__senat() %}
    {{ return({
        'bounds': [
            ['senate_sitting_days', '%', '1', '31'],
            ['senate_votes', '%', '1', '2000'],
        ],
        'models': ['poc_metrics_senate_sitting_days', 'poc_metrics_senate_votes'],
    }) }}
{% endmacro %}
