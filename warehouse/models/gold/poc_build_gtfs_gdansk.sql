{{ config(alias='build_gtfs_gdansk', tags=['gold_block']) }}
{# Gold build stage: ZTM Gdańsk GTFS timetable (register PL-ENV-029), block 'scheduled_trips': scheduled trips per
   service day (frequency D) by GTFS route type and in total, from silver.gtfs_gdansk_service_days; area = Gdańsk
   (poc_geography__gtfs_gdansk). SQL shared by the city GTFS family: poc_gtfs_build
   (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_build('gtfs_gdansk', ref('gtfs_gdansk_service_days'), 'Gdańsk', 'ZTM Gdańsk') }}
