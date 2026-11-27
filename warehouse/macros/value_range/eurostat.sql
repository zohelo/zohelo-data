{# Eurostat (moved from tests/metrics_value_range.sql). population: persons; largest area EEA31 2020 =
   519,826,027 (demo_pjan, #2788). gdp_current_prices: million EUR (CP_MEUR), and million national currency (CP_MNAC),
   largest HU 2025 = 87,307,647 million HUF (nama_10_gdp, #2788). core_inflation: 550.6 (Dev trial 43093114523: a
   candidate country in the 1990s). min_wage: national currencies; 322,800 (Dev trial 43093114523).
   industrial_production: % change on a year earlier. construction_output: % change on a year earlier; 261 (Dev trial
   43093114523). The bounds from core_inflation on came with the slice needs-block rows (#2743). #}
{% macro poc_value_range__eurostat() %}
    {{ return({
        'bounds': [
            ['population', '%', '0', '1e9'],
            ['gdp_current_prices', 'eurostat:NAMA_10_GDP:CP_MEUR:B1GQ', '0', '3e7'],
            ['gdp_current_prices', 'eurostat:NAMA_10_GDP:CP_MNAC:B1GQ', '0', '1e9'],
            ['inflation', '%', '-100', '3000'],
            ['unemployment_rate', '%', '0', '60'],
            ['gdp_per_capita', '%', '0', '2e5'],
            ['gdp_volume_growth', '%', '-50', '100'],
            ['core_inflation', '%', '-10', '600'],
            ['min_wage', '%', '1', '1e6'],
            ['adult_learning', '%', '0', '100'],
            ['industrial_production', '%', '-100', '300'],
            ['construction_output', '%', '-100', '400'],
            ['arope_rate', '%', '0', '100'],
            ['long_term_unemployment', '%', '0', '100'],
        ],
        'models': [
            'poc_metrics_population',
            'poc_metrics_gdp_current_prices',
            'poc_metrics_inflation',
            'poc_metrics_unemployment_rate',
            'poc_metrics_gdp_per_capita',
            'poc_metrics_gdp_volume_growth',
            'poc_metrics_core_inflation',
            'poc_metrics_min_wage',
            'poc_metrics_adult_learning',
            'poc_metrics_industrial_production',
            'poc_metrics_construction_output',
            'poc_metrics_arope_rate',
            'poc_metrics_long_term_unemployment',
        ],
    }) }}
{% endmacro %}
