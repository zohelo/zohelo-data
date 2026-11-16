{{ config(alias='transit_routes_gtfs_gdansk') }}
{# Gold entity table (reference source, playbook section 7): routes of the ZTM Gdańsk GTFS timetable (register PL-
   ENV-029), newest window of each feed, keyed by the source's own id 'gtfs_gdansk:<feed>:<route_id>'. SQL shared by
   the city GTFS family. #}
{{ poc_gtfs_route_entities('gtfs_gdansk', ref('gtfs_gdansk_routes'), '2261011') }}
