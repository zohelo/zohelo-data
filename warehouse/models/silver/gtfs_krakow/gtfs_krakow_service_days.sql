{# Silver: scheduled service of the ZTP Kraków GTFS timetable (register PL-ENV-031) per service day, accumulated over
   every landed window (bronze.gtfs_krakow_records): trips, departures (stop_times rows) and routes by GTFS route
   type. Grain: feed × service_date × route_type; a day held by several windows takes the newest one downloaded on or before it. Service
   days from calendar.txt and calendar_dates.txt. SQL shared by the city GTFS family: poc_gtfs_service_days
   (warehouse/macros/sources/gtfs_gdansk.sql). #}
{{ poc_gtfs_service_days(source('gtfs_krakow', 'gtfs_krakow_records')) }}
