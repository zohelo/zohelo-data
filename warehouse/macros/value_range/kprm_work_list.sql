{# Work list of the Council of Ministers (#2972): drafts published per month, Poland, 13 (Dec 2023) to 105 (Dec 2025)
   in Dev trial 45757552453; about 600 a year. Only months with a published draft have a row, so the lower bound is 1. #}
{% macro poc_value_range__kprm_work_list() %}
    {{ return({
        'bounds': [
            ['government_drafts_published', '%', '1', '500'],
        ],
        'models': ['poc_metrics_government_drafts_published'],
    }) }}
{% endmacro %}
