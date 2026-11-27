{# World Bank GEM (#2373), Dev trial 43283254888 (moved from tests/metrics_value_range.sql). cpi_annual_rate: CPI %
   y-o-y, EU27 members 1987-2026: -6.5 to 3,453.3 (1990s hyperinflation); documentation only until
   poc_metrics_cpi_annual_rate (it exists) is in `models`. unemployment_rate_monthly_sa: %, highest 28.7;
   documentation only (no metrics model for the monthly KPI yet). #}
{% macro poc_value_range__world_bank_gem() %}
    {{ return({
        'bounds': [
            ['cpi_annual_rate', 'world_bank_gem:CPTOTSAXNZGY:pct', '-50', '5000'],
            ['unemployment_rate_monthly_sa', 'world_bank_gem:UNEMPSA_:pct', '0', '60'],
        ],
        'models': [],
    }) }}
{% endmacro %}
