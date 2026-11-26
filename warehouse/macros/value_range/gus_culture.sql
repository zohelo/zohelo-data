{# GUS publication tables (#2893), counts per year; Poland is the largest area: museum visits 46,242,767 (2025), cinema
   admissions 61,949,490 (2019), library users 5,983,725 (2019) (Dev trial 43298219762). #}
{% macro poc_value_range__gus_culture() %}
    {{ return({
        'bounds': [
            ['museum_visitors', '%', '0', '2e8'],
            ['cinema_admissions', '%', '0', '2e8'],
            ['public_library_users', '%', '0', '3e7'],
        ],
        'models': ['poc_metrics_museum_visitors', 'poc_metrics_cinema_admissions', 'poc_metrics_public_library_users'],
    }) }}
{% endmacro %}
