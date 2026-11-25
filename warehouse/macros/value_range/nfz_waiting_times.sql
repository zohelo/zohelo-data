{# NFZ waiting times (#2956), stable case, Poland the largest area: persons awaiting 4,645,039 (October 2026),
   waiting lists 74,088 (November 2026 snapshot), Dev trial 43797091426. #}
{% macro poc_value_range__nfz_waiting_times() %}
    {{ return({
        'bounds': [
            ['waiting_list_persons', '%', '0', '2e7'],
            ['waiting_lists', '%', '1', '1e6'],
        ],
        'models': ['poc_metrics_waiting_list_persons', 'poc_metrics_waiting_lists'],
    }) }}
{% endmacro %}
