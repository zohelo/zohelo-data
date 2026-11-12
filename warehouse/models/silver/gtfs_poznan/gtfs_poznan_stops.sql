{# Silver: stops of the ZTM Poznań GTFS timetable (register PL-ENV-030), from the newest landed window of each feed
   (bronze.gtfs_poznan_records). Grain: feed × stop_id; coordinates as double, codes as published. SQL shared by the
   city GTFS family: poc_gtfs_stops (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_stops(source('gtfs_poznan', 'gtfs_poznan_records')) }}
