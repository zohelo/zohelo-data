{{ config(alias='transit_stops_gtfs_poznan') }}
{# Gold entity table (reference source, playbook section 7): stops of the ZTM Poznań GTFS timetable (register PL-
   ENV-030), newest window of each feed, keyed by the source's own id 'gtfs_poznan:<feed>:<stop_id>'. SQL shared by
   the city GTFS family. #}
{{ poc_gtfs_stop_entities('gtfs_poznan', ref('gtfs_poznan_stops'), '3064011') }}
