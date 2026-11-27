{# NBP reference rate: percent per year; highest 24.0 in 1998 (silver.nbp_policy_rates, Dev trial 43093114523).
   Moved from tests/metrics_value_range.sql. #}
{% macro poc_value_range__nbp_interest_rates() %}
    {{ return({
        'bounds': [
            ['nbp_reference_rate', '%', '0', '30'],
        ],
        'models': [
            'poc_metrics_nbp_reference_rate',
        ],
    }) }}
{% endmacro %}
