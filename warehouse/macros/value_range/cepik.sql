{# CEPiK vehicle registrations (#2966): vehicles registered in a month; Poland is the largest area: 206,017 in
   March 2026 (CEPiK workbook of 22 Oct 2026, Dev probe 43340837086); a monthly count above 1 million is a scale
   error. #}
{% macro poc_value_range__cepik() %}
    {{ return({
        'bounds': [
            ['vehicle_registrations', '%', '0', '1e6'],
        ],
        'models': ['poc_metrics_vehicle_registrations'],
    }) }}
{% endmacro %}
