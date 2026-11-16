{{ config(alias='transit_routes_gtfs_poznan') }}
{# Gold entity table (reference source, playbook section 7): routes of the ZTM Poznań GTFS timetable (register PL-
   ENV-030), newest window of each feed, keyed by the source's own id 'gtfs_poznan:<feed>:<route_id>'. SQL shared by
   the city GTFS family. #}
{{ poc_gtfs_route_entities('gtfs_poznan', ref('gtfs_poznan_routes'), '3064011') }}
