{# GUS publication tables (#2893), counts per year; Poland is the largest area: tourists 42,738,329, overnight stays
   104,692,301, bed places 887,158 (2025) (Dev trial 43298219762). #}
{% macro poc_value_range__gus_tourism() %}
    {{ return({
        'bounds': [
            ['tourists_accommodated', '%', '0', '2e8'],
            ['tourist_overnight_stays', '%', '0', '5e8'],
            ['tourist_bed_places', '%', '0', '5e6'],
        ],
        'models': [
            'poc_metrics_tourists_accommodated',
            'poc_metrics_tourist_overnight_stays',
            'poc_metrics_tourist_bed_places',
        ],
    }) }}
{% endmacro %}
