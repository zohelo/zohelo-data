{# wto_stats (register GL-020): Gold registration, decision D34 (docs/source-registration.md). Block 'batis'
   (models/gold/poc_build_wto_stats.sql). #}
{% macro poc_source__wto_stats() %}
    {{ return({'block_model': 'poc_build_wto_stats', 'blocks': [['batis', 'OECD-WTO BaTIS: trade in services by partner and EBOPS 2010 main item']]}) }}
{% endmacro %}

{# Reporters by ISO 3166-1 alpha-2 as BaTIS publishes them (dim_geography iso2, as eea and oecd). Static, so the
   shared map reads no WTO Silver table. The build stage keeps Poland as reporter only; BaTIS' own aggregates (WL,
   EU, GEU, ROW) have no Gold key here and would map to _X. #}
{% macro poc_geography__wto_stats() %}
wto_stats as (
    select
        'wto_stats' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 is not null
)
{% endmacro %}
