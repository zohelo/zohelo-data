{# Findex (#2931): published as a share 0-1, metrics in per cent of adults (D33, × 100); all areas 2011-2024:
   0.40 to 100.0 (Dev trial 43310863075). A share converted twice passes 100. Moved from tests/metrics_value_range.sql. #}
{% macro poc_value_range__world_bank_findex() %}
    {{ return({
        'bounds': [
            ['account_ownership', '%', '0', '100'],
        ],
        'models': [
            'poc_metrics_account_ownership',
        ],
    }) }}
{% endmacro %}
