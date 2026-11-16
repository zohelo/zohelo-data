{{ config(alias='transit_routes_gtfs_krakow') }}
{# Gold entity table (reference source, playbook section 7): routes of the ZTP Kraków GTFS timetable (register PL-
   ENV-031), newest window of each feed, keyed by the source's own id 'gtfs_krakow:<feed>:<route_id>'. SQL shared by
   the city GTFS family. #}
{{ poc_gtfs_route_entities('gtfs_krakow', ref('gtfs_krakow_routes'), '1261011') }}
