{{ config(alias='build_gtfs_wroclaw', tags=['gold_block']) }}
{# Gold build stage: Wrocław public transport GTFS timetable (register PL-ENV-032), block 'scheduled_trips': scheduled
   trips per service day (frequency D) by GTFS route type and in total, from silver.gtfs_wroclaw_service_days; area =
   Wrocław (poc_geography__gtfs_wroclaw). SQL shared by the city GTFS family: poc_gtfs_build
   (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_build('gtfs_wroclaw', ref('gtfs_wroclaw_service_days'), 'Wrocław', 'Wrocław public transport') }}
