{# GUS publication tables (#2893): survey shares are % of persons 15+ (Dev trial 43298219762). #}
{% macro poc_value_range__gus_culture_participation() %}
    {{ return({
        'bounds': [
            ['museum_visiting_share', '%', '0', '100'],
            ['theatre_attendance_share', '%', '0', '100'],
        ],
        'models': ['poc_metrics_museum_visiting_share', 'poc_metrics_theatre_attendance_share'],
    }) }}
{% endmacro %}
