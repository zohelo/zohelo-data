{# UNHCR (#2415): persons; highest refugees 4,404,995 (all asylum countries 1951-2025, Dev trial 43282453427).
   Documentation only (no metrics_refugees_hosted model yet). Moved from tests/metrics_value_range.sql. #}
{% macro poc_value_range__unhcr() %}
    {{ return({
        'bounds': [
            ['refugees_hosted', '%', '0', '2e7'],
        ],
        'models': [],
    }) }}
{% endmacro %}
