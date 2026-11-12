{# Silver: stops of the ZTP Kraków GTFS timetable (register PL-ENV-031), from the newest landed window of each feed
   (bronze.gtfs_krakow_records). Grain: feed × stop_id; coordinates as double, codes as published. SQL shared by the
   city GTFS family: poc_gtfs_stops (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_stops(source('gtfs_krakow', 'gtfs_krakow_records')) }}
