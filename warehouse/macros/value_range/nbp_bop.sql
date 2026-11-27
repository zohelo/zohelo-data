{# NBP balance of payments (#2780): million EUR, Poland; current account 2026-Q2 -6,225, net IIP -283,248 (Dev trial
   43163012721). Documentation only until metrics_current_account_components and metrics_fdi_inflow are in `models`
   (draft concepts are outside the test's scope). Moved from tests/metrics_value_range.sql. #}
{% macro poc_value_range__nbp_bop() %}
    {{ return({
        'bounds': [
            ['current_account_components', '%', '-1e6', '1e6'],
            ['fdi_inflow', '%', '-1e6', '1e6'],
        ],
        'models': [],
    }) }}
{% endmacro %}
