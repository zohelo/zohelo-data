{{ config(alias='transit_stops_gtfs_gdansk') }}
{# Gold entity table (reference source, playbook section 7): stops of the ZTM Gdańsk GTFS timetable (register PL-
   ENV-029), newest window of each feed, keyed by the source's own id 'gtfs_gdansk:<feed>:<stop_id>'. SQL shared by
   the city GTFS family. #}
{{ poc_gtfs_stop_entities('gtfs_gdansk', ref('gtfs_gdansk_stops'), '2261011') }}
