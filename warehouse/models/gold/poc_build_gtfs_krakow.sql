{{ config(alias='build_gtfs_krakow', tags=['gold_block']) }}
{# Gold build stage: ZTP Kraków GTFS timetable (register PL-ENV-031), block 'scheduled_trips': scheduled trips per
   service day (frequency D) by GTFS route type and in total, from silver.gtfs_krakow_service_days; area = Kraków
   (poc_geography__gtfs_krakow). SQL shared by the city GTFS family: poc_gtfs_build
   (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_build('gtfs_krakow', ref('gtfs_krakow_service_days'), 'Kraków', 'ZTP Kraków') }}
