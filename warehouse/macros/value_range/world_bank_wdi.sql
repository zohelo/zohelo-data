{# World Bank WDI (moved from tests/metrics_value_range.sql). population: persons; largest area WLD 2025 =
   8,215,424,893 (API SP.POP.TOTL, #2788). gdp_current_prices: million US$ since D33 (metrics convert the published US$
   by scale), the former bound 3e14 US$ / 1e6; and current local currency units, largest IR 2025 = 2.693e17 IRR as
   published (NY.GDP.MKTP.CN, #2788). #}
{% macro poc_value_range__world_bank_wdi() %}
    {{ return({
        'bounds': [
            ['population', '%', '0', '1e10'],
            ['gdp_current_prices', 'world_bank_wdi:NY.GDP.MKTP.CD', '0', '3e8'],
            ['gdp_current_prices', 'world_bank_wdi:NY.GDP.MKTP.CN', '0', '3e18'],
            ['inflation', '%', '-100', '100000'],
            ['unemployment_rate', '%', '0', '60'],
            ['gdp_per_capita', '%', '0', '5e5'],
            ['gdp_volume_growth', '%', '-100', '200'],
        ],
        'models': [
            'poc_metrics_population',
            'poc_metrics_gdp_current_prices',
            'poc_metrics_inflation',
            'poc_metrics_unemployment_rate',
            'poc_metrics_gdp_per_capita',
            'poc_metrics_gdp_volume_growth',
        ],
    }) }}
{% endmacro %}
