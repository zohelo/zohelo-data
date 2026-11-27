{# UN Statistical Yearbook (#2861), EU27 members and World, selected years, Dev trial 43290214779 (moved from
   tests/metrics_value_range.sql): GDP in millions of US dollars as published (World 2023 = 105,943,460, Poland
   809,201), GDP per capita in US dollars (highest 128,936 LU, Poland 20,876), real GDP growth in per cent (-10.9 to 24.6
   IE, Poland 2023 = 0.1), the WDI bounds. Draft KPIs: internet_users, % of the population, 15.6 to 99.3 (LU), Poland
   2023 = 86.4; health_expenditure_gdp, current health expenditure, % of GDP, 4.9 to 12.9 (DE), Poland 2022 = 6.4.
   gerd_gdp: 0.4 to 3.7 % of GDP, documentation until a metrics model of the KPI is in `models`. #}
{% macro poc_value_range__undata() %}
    {{ return({
        'bounds': [
            ['gdp_current_prices', '%', '0', '3e8'],
            ['gdp_per_capita', '%', '0', '5e5'],
            ['gdp_volume_growth', '%', '-100', '200'],
            ['internet_users', '%', '0', '100'],
            ['health_expenditure_gdp', '%', '0', '30'],
            ['gerd_gdp', '%', '0', '10'],
        ],
        'models': [
            'poc_metrics_gdp_current_prices',
            'poc_metrics_gdp_per_capita',
            'poc_metrics_gdp_volume_growth',
            'poc_metrics_internet_users',
            'poc_metrics_health_expenditure_gdp',
        ],
    }) }}
{% endmacro %}
