{# RPWDL register counts (#2958), Poland the largest area: active healthcare providers 29,845 ledgers and practices
   234,327 in the 16 November 2026 files (Dev trials 43796970764, 43813074556). #}
{% macro poc_value_range__rpwdl() %}
    {{ return({
        'bounds': [
            ['healthcare_providers', '%', '0', '2e5'],
            ['medical_practices', '%', '0', '1e6'],
        ],
        'models': ['poc_metrics_healthcare_providers', 'poc_metrics_medical_practices'],
    }) }}
{% endmacro %}
