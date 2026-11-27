{# ILOSTAT (#2861), Dev trial 43290214779 (moved from tests/metrics_value_range.sql). unemployment_rate: per
   cent of the labour force, 15+, EU27 members, annual and quarterly national series; highest 28.153 (GR, quarterly),
   Poland 2025 = 3.018; the other sources' bound. Documentation until a metrics model of the KPI is in `models` (its
   other sources have no bound either): employment rate 43.857 to 83.293, activity rate 45.944 to 85.643, NEET 3.079 to
   77.747 (BG), youth unemployment 1.627 to 60.726 (GR). #}
{% macro poc_value_range__ilostat() %}
    {{ return({
        'bounds': [
            ['unemployment_rate', '%', '0', '60'],
            ['employment_rate', '%', '0', '100'],
            ['activity_rate', '%', '0', '100'],
            ['neet_rate', '%', '0', '100'],
            ['youth_unemployment', '%', '0', '100'],
        ],
        'models': [
            'poc_metrics_unemployment_rate',
        ],
    }) }}
{% endmacro %}
