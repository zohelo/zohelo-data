{{ config(alias='transit_stops_gtfs_wroclaw') }}
{# Gold entity table (reference source, playbook section 7): stops of the Wrocław public transport GTFS timetable
   (register PL-ENV-032), newest window of each feed, keyed by the source's own id 'gtfs_wroclaw:<feed>:<stop_id>'.
   SQL shared by the city GTFS family. #}
{{ poc_gtfs_stop_entities('gtfs_wroclaw', ref('gtfs_wroclaw_stops'), '0264011') }}
