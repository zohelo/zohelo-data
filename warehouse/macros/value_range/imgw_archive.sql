{# IMGW synoptic stations, Poland and voivodeship monthly means (#2959). Air temperature °C: monthly means
   -15.28 to 23.8 (1966-2026); precipitation mm per month: Poland at most 182.54 (1997-07), a voivodeship at most
   345.97 (śląskie 1997-07, the Oder flood; a voivodeship mean can rest on one station) (Dev trial 43831905694). #}
{% macro poc_value_range__imgw_archive() %}
    {{ return({
        'bounds': [
            ['air_temperature', '%', '-30', '35'],
            ['precipitation', '%', '0', '1000'],
        ],
        'models': ['poc_metrics_air_temperature', 'poc_metrics_precipitation'],
    }) }}
{% endmacro %}
