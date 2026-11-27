{# UNECE Statistical Database (#2861, #2885), 56 member countries, EU-27 and euro area; Dev trial 43306220004
   (moved from tests/metrics_value_range.sql). Unemployment rate, annual, per cent: 0.0 (KZ 1991) to 38.8 (MK), Poland
   2025 = 3.1. Population: published in thousands of persons, metrics in persons (D33, x 1000): 29,000 to 451,935,000
   (EU-27), Poland 37,389,000. Industrial production, annual growth in per cent: -36.7 to 39.4 (SK), Poland 2025 = 2.6.
   Documentation until those KPIs' metrics models are in `models`: CPI growth -8.0 to 20,757.4 (AM, 1990s
   hyperinflation), PPI growth -46.4 to 7,687.6 (UA); monthly seasonally adjusted unemployment rate 0.5 to 36.7 (MK). #}
{% macro poc_value_range__unece() %}
    {{ return({
        'bounds': [
            ['unemployment_rate', '%', '0', '80'],
            ['population', '%', '0', '1e9'],
            ['industrial_production', '%', '-80', '300'],
            ['unemployment_rate_monthly_sa', '%', '0', '80'],
            ['cpi_annual_rate', '%', '-50', '1e5'],
            ['ppi_annual_rate', '%', '-80', '1e5'],
        ],
        'models': [
            'poc_metrics_population',
            'poc_metrics_unemployment_rate',
            'poc_metrics_industrial_production',
        ],
    }) }}
{% endmacro %}
