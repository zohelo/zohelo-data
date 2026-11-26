{# wipo_ip_stats (register GL-032): Gold registration, decision D34 (docs/source-registration.md). Four blocks, one per
   published count file (models/gold/poc_build_wipo_ip_stats.sql). #}
{% macro poc_source__wipo_ip_stats() %}
    {{ return({'block_model': 'poc_build_wipo_ip_stats', 'blocks': [
        ['patent_publications', 'WIPO patent publications by office, origin and technology field'],
        ['patent_grants', 'WIPO patent grants by office, origin and technology field'],
        ['design_applications', 'WIPO industrial designs in applications by office, origin and Locarno class'],
        ['design_registrations', 'WIPO industrial designs in registrations by office, origin and Locarno class'],
    ]}) }}
{% endmacro %}

{# Offices and origins by ISO 3166-1 alpha-2 as WIPO publishes them, from Gold's own country list (as eea), so the
   shared map reads no WIPO Silver table. IP offices that are not countries (EP European Patent Office, EM EUIPO, EA,
   AP, OA, GC, BX Benelux), historical codes (CS, YU, DD, AN) and '**' (not stated) are not listed: they map to _X with
   their code kept, never to a country. #}
{% macro poc_geography__wipo_ip_stats() %}
wipo_ip_stats as (
    select
        'wipo_ip_stats' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 is not null
)
{% endmacro %}
