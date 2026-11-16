{{ config(alias='transit_stops_gtfs_krakow') }}
{# Gold entity table (reference source, playbook section 7): stops of the ZTP Kraków GTFS timetable (register PL-
   ENV-031), newest window of each feed, keyed by the source's own id 'gtfs_krakow:<feed>:<stop_id>'. SQL shared by
   the city GTFS family. #}
{{ poc_gtfs_stop_entities('gtfs_krakow', ref('gtfs_krakow_stops'), '1261011') }}
