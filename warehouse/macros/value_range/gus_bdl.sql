{# GUS BDL (moved from tests/metrics_value_range.sql). unemployment_rate: percent of the labour force, BDL
   P2361 up to 14.3 (#2788). industrial_production: index, previous year = 100. employment_enterprise: persons; Poland
   6,702,934 in 2026-08. median_wage: PLN; Poland 7,530.45 in 2026-03. price_m2_secondary: PLN per m2; 2,202 the
   lowest (Dev trial 43093114523). The bounds from adult_learning on came with the slice needs-block rows (#2743). #}
{% macro poc_value_range__gus_bdl() %}
    {{ return({
        'bounds': [
            ['population', '%', '0', '5e7'],
            ['unemployment_rate', '%', '0', '60'],
            ['adult_learning', '%', '0', '100'],
            ['extreme_poverty', '%', '0', '100'],
            ['industrial_production', '%', '1', '300'],
            ['employment_enterprise', '%', '1000', '2e7'],
            ['median_wage', '%', '500', '1e5'],
            ['price_m2_secondary', '%', '300', '1e5'],
            ['registered_unemployed', '%', '0', '5e6'],
        ],
        'models': [
            'poc_metrics_population',
            'poc_metrics_unemployment_rate',
            'poc_metrics_adult_learning',
            'poc_metrics_extreme_poverty',
            'poc_metrics_industrial_production',
            'poc_metrics_employment_enterprise',
            'poc_metrics_median_wage',
            'poc_metrics_price_m2_secondary',
            'poc_metrics_registered_unemployed',
        ],
    }) }}
{% endmacro %}
