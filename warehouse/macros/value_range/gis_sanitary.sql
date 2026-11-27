{# gis_sanitary (#442): per cent of food samples disqualified, a share of samples (0 to 100; printed 2,13 to 4,02 for
   all foodstuffs 2012-2025 in the Dev trial). No metrics model yet, so the bound waits. #}
{% macro poc_value_range__gis_sanitary() %}
    {{ return({
        'bounds': [
            ['food_samples_disqualified_share', 'gis_sanitary:food_samples:pct:%', '0', '100'],
        ],
        'models': [],
    }) }}
{% endmacro %}
