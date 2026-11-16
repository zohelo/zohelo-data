{# gtfs_wroclaw (register PL-ENV-032): Gold registration (D34, docs/source-registration.md). The SQL of the build stage
   and of the Silver and entity models is shared by the city GTFS family (warehouse/macros/sources/gtfs_gdansk.sql).
   #}
{% macro poc_source__gtfs_wroclaw() %}
    {{ return({'block_model': 'poc_build_gtfs_wroclaw', 'blocks': [['scheduled_trips', 'Scheduled public transport trips per service day (GTFS, Wrocław public transport)']]}) }}
{% endmacro %}

{# The feed is the Wrocław public transport timetable; its lines reach neighbouring gminas, the area is the city that
   publishes it. #}
{% macro poc_geography__gtfs_wroclaw() %}
gtfs_wroclaw as (
    select
        'gtfs_wroclaw' as source_system,
        'Wrocław' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:0264011'
)
{% endmacro %}
