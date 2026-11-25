{# ISWS (#2968): common courts, all cases, Poland; largest 2019 = 17,728,993 received, 16,400,377 resolved; average
   duration in months, 2011-2025 between 4.05 and 7.15 (ISWS workbooks, Dev trial 43804846162). #}
{% macro poc_value_range__ms_isws() %}
    {{ return({
        'bounds': [
            ['court_cases_received', '%', '0', '5e7'],
            ['court_cases_resolved', '%', '0', '5e7'],
            ['court_case_duration', '%', '0', '60'],
        ],
        'models': [
            'poc_metrics_court_cases_received',
            'poc_metrics_court_cases_resolved',
            'poc_metrics_court_case_duration',
        ],
    }) }}
{% endmacro %}
