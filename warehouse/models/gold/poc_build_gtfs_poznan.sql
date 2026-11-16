{{ config(alias='build_gtfs_poznan', tags=['gold_block']) }}
{# Gold build stage: ZTM Poznań GTFS timetable (register PL-ENV-030), block 'scheduled_trips': scheduled trips per
   service day (frequency D) by GTFS route type and in total, from silver.gtfs_poznan_service_days; area = Poznań
   (poc_geography__gtfs_poznan). SQL shared by the city GTFS family: poc_gtfs_build
   (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_build('gtfs_poznan', ref('gtfs_poznan_service_days'), 'Poznań', 'ZTM Poznań') }}
