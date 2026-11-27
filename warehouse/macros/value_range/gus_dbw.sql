{# GUS DBW (moved from tests/metrics_value_range.sql). gdp_current_prices: million PLN, Poland only; 2025 =
   3.915e6 (1038_16_105_4; Eurostat CP_MNAC PL is the same order). unemployment_rate: percent of the labour force
   (LFS/BAEL); 1514_1360_95_3 1.3-6.5 (#2788). gdp_volume_growth: an index, previous period or same quarter of the
   previous year = 100 (rate = value - 100); Poland published 92.6-111.2 (1038_16_389_3) and 98.0-106.9 (1038_16_92_4),
   #2788. industrial_production and construction_output: index, same month a year earlier = 100. median_wage: PLN.
   price_m2_secondary: PLN per m2; 2,202 the lowest (Dev trial 43093114523). The bounds from industrial_production on
   came with the slice needs-block rows (#2743). #}
{% macro poc_value_range__gus_dbw() %}
    {{ return({
        'bounds': [
            ['population', '%', '0', '5e7'],
            ['gdp_current_prices', '%', '0', '3e7'],
            ['unemployment_rate', '%', '0', '60'],
            ['gdp_volume_growth', '%', '50', '200'],
            ['industrial_production', '%', '1', '300'],
            ['construction_output', '%', '1', '300'],
            ['median_wage', '%', '500', '1e5'],
            ['price_m2_secondary', '%', '300', '1e5'],
            ['arope_rate', '%', '0', '100'],
            ['long_term_unemployment', '%', '0', '100'],
            ['registered_unemployed', '%', '0', '5e6'],
        ],
        'models': [
            'poc_metrics_population',
            'poc_metrics_gdp_current_prices',
            'poc_metrics_unemployment_rate',
            'poc_metrics_gdp_volume_growth',
            'poc_metrics_industrial_production',
            'poc_metrics_construction_output',
            'poc_metrics_median_wage',
            'poc_metrics_price_m2_secondary',
            'poc_metrics_arope_rate',
            'poc_metrics_long_term_unemployment',
            'poc_metrics_registered_unemployed',
        ],
    }) }}
{% endmacro %}
