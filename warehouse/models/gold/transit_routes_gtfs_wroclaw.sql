{{ config(alias='transit_routes_gtfs_wroclaw') }}
{# Gold entity table (reference source, playbook section 7): routes of the Wrocław public transport GTFS timetable
   (register PL-ENV-032), newest window of each feed, keyed by the source's own id 'gtfs_wroclaw:<feed>:<route_id>'.
   SQL shared by the city GTFS family. #}
{{ poc_gtfs_route_entities('gtfs_wroclaw', ref('gtfs_wroclaw_routes'), '0264011') }}
