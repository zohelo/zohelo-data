{# Barometr Zawodów (PLS-030, #454): occupations an edition puts in deficit, per area and forecast year; a count
   between 0 and the number of occupations assessed (168 on the Poland poster for 2026, Dev probe 45896169831). #}
{% macro poc_value_range__barometr_zawodow() %}
    {{ return({
        'bounds': [
            ['occupations_deficit_forecast', 'barometr_zawodow:forecast:occupations:deficyt', '0', '400'],
        ],
        'models': ['poc_metrics_occupations_deficit_forecast'],
    }) }}
{% endmacro %}
