{# Silver: stops of the ZTM Gdańsk GTFS timetable (register PL-ENV-029), from the newest landed window of each feed
   (bronze.gtfs_gdansk_records). Grain: feed × stop_id; coordinates as double, codes as published. SQL shared by the
   city GTFS family: poc_gtfs_stops (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_stops(source('gtfs_gdansk', 'gtfs_gdansk_records')) }}
