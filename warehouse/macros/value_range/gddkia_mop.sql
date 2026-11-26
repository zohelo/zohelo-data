{# GDDKiA rest areas (#2995), Dev trial of the PR: Poland is the largest area (all rest areas of an edition, see
   ingestion/sources/gddkia_mop.yaml); a count above 5,000 is a reading error (the table lists rest areas on about
   18,000 km of national roads). No metrics model yet: the bound waits until the KPI gets one. #}
{% macro poc_value_range__gddkia_mop() %}
    {{ return({
        'bounds': [
            ['road_rest_areas', 'gddkia_mop:rest_areas:facilities', '0', '5e3'],
        ],
        'models': [],
    }) }}
{% endmacro %}
