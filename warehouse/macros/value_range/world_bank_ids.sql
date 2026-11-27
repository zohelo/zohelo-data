{# World Bank IDS (#2931): published in current US$, metrics in million US$ (D33, ÷ 1,000,000); highest is the
   low- and middle-income group in 2025: stocks 9,445,018, debt service 1,398,784 (Dev trial 43310863075). A value
   left in US$ passes the bound for every economy with more than 30 (10) million US$. Moved from
   tests/metrics_value_range.sql. #}
{% macro poc_value_range__world_bank_ids() %}
    {{ return({
        'bounds': [
            ['external_debt_stocks', '%', '0', '3e7'],
            ['external_debt_service', '%', '0', '1e7'],
        ],
        'models': [
            'poc_metrics_external_debt_stocks',
            'poc_metrics_external_debt_service',
        ],
    }) }}
{% endmacro %}
