{# BIS (#2861): draft KPIs, measured in Dev trial 43290214779 (moved from tests/metrics_value_range.sql).
   policy_rate: per cent per year, -0.75 to 355,085.64 (BR, hyperinflation, as the BIS publishes it), Poland 2026-10 =
   3.75; the upper bound only catches a broken file. house_price_growth and real_house_price_growth: residential
   property prices, year-on-year change in per cent, nominal -42.25 to 360.35 (IT, 1940s), real -45.84 to 59.64 (LT);
   Poland 2026-Q2 = 6.29 and 3.29. credit_to_gdp: credit to the private non-financial sector, % of GDP, 7.5 to 482.6
   (LU), Poland 2026-Q1 = 57.1. debt_service_ratio: private non-financial sector, per cent of income, 3.0 to 40.2 (BR),
   Poland 6.3. reer: index, 2020 = 100, 9.59 to 519.14 (AR); documentation until a metrics model of the KPI is in
   `models` (its other sources have no bound either). #}
{% macro poc_value_range__bis() %}
    {{ return({
        'bounds': [
            ['policy_rate', '%', '-5', '5e5'],
            ['house_price_growth', '%', '-100', '1000'],
            ['real_house_price_growth', '%', '-100', '300'],
            ['credit_to_gdp', '%', '0', '1000'],
            ['debt_service_ratio', '%', '0', '100'],
            ['reer', '%', '0', '2000'],
        ],
        'models': [
            'poc_metrics_policy_rate',
            'poc_metrics_house_price_growth',
            'poc_metrics_real_house_price_growth',
            'poc_metrics_credit_to_gdp',
            'poc_metrics_debt_service_ratio',
        ],
    }) }}
{% endmacro %}
