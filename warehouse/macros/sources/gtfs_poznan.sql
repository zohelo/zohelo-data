{# gtfs_poznan (register PL-ENV-030): Gold registration (D34, docs/source-registration.md). The SQL of the build stage
   and of the Silver and entity models is shared by the city GTFS family (warehouse/macros/sources/gtfs_gdansk.sql).
   #}
{% macro poc_source__gtfs_poznan() %}
    {{ return({'block_model': 'poc_build_gtfs_poznan', 'blocks': [['scheduled_trips', 'Scheduled public transport trips per service day (GTFS, ZTM Poznań)']]}) }}
{% endmacro %}

{# The feed is the ZTM Poznań timetable; its lines reach neighbouring gminas, the area is the city that publishes it. #}
{% macro poc_geography__gtfs_poznan() %}
gtfs_poznan as (
    select
        'gtfs_poznan' as source_system,
        'Poznań' as source_code,
        g.geography_key
    from geo as g
    where g.geography_key = 'TERC:3064011'
)
{% endmacro %}
