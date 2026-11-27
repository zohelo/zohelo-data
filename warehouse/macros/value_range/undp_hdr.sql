{# UNDP HDR (#2861), Dev trial 43290214779 (moved from tests/metrics_value_range.sql). hdi: Human Development
   Index, 0 to 1 by definition: 0.608 to 0.962 (DK), Poland 2023 = 0.906. life_expectancy: 65.11 to 83.72 years,
   documentation until a metrics model of the KPI is in `models`. #}
{% macro poc_value_range__undp_hdr() %}
    {{ return({
        'bounds': [
            ['hdi', '%', '0', '1'],
            ['life_expectancy', '%', '20', '100'],
        ],
        'models': [
            'poc_metrics_hdi',
        ],
    }) }}
{% endmacro %}
