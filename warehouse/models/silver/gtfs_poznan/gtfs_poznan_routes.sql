{# Silver: routes of the ZTM Poznań GTFS timetable (register PL-ENV-030), from the newest landed window of each feed
   (bronze.gtfs_poznan_records). Grain: feed × route_id; route_type as the GTFS integer code. SQL shared by the city
   GTFS family: poc_gtfs_routes (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_routes(source('gtfs_poznan', 'gtfs_poznan_records')) }}
