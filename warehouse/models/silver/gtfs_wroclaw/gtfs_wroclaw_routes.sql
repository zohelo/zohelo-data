{# Silver: routes of the Wrocław public transport GTFS timetable (register PL-ENV-032), from the newest landed window of
   each feed (bronze.gtfs_wroclaw_records). Grain: feed × route_id; route_type as the GTFS integer code. SQL shared by
   the city GTFS family: poc_gtfs_routes (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_routes(source('gtfs_wroclaw', 'gtfs_wroclaw_records')) }}
