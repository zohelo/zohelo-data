{# gtfs_krakow (register PL-ENV-031): Gold registration (D34, docs/source-registration.md). The SQL of the build stage
   and of the Silver and entity models is shared by the city GTFS family (warehouse/macros/sources/gtfs_gdansk.sql).
   #}
{% macro poc_source__gtfs_krakow() %}
    {{ return({'block_model': 'poc_build_gtfs_krakow', 'blocks': [['scheduled_trips', 'Scheduled public transport trips per service day (GTFS, ZTP Kraków)']]}) }}
{% endmacro %}

{# The feed is the ZTP Kraków timetable; its lines reach neighbouring gminas, the area is the city that publishes it. #}
{% macro poc_geography__gtfs_krakow() %}
gtfs_krakow as (
    select
        'gtfs_krakow' as source_system,
        'Kraków' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:1261011'
)
{% endmacro %}
