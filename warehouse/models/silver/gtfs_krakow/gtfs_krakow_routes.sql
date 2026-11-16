{# Silver: routes of the ZTP Kraków GTFS timetable (register PL-ENV-031), from the newest landed window of each feed
   (bronze.gtfs_krakow_records). Grain: feed × route_id; route_type as the GTFS integer code. SQL shared by the city
   GTFS family: poc_gtfs_routes (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_routes(source('gtfs_krakow', 'gtfs_krakow_records')) }}
