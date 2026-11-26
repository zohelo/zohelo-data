{# GDDKiA rest areas (#2995), Dev trial 45829412455: Poland is the largest area, 424 rest areas in the September 2026
   edition (łódzkie the largest voivodeship, 58); a count above 2,000 is a reading error, not growth. No metrics model
   yet: the bound waits until the KPI gets one. #}
{% macro poc_value_range__gddkia_mop() %}
    {{ return({
        'bounds': [
            ['road_rest_areas', 'gddkia_mop:rest_areas:facilities', '0', '2e3'],
        ],
        'models': [],
    }) }}
{% endmacro %}
