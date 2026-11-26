{# cepik (register PL-ENV-024): Gold registration, decision D34 (docs/source-registration.md). Block
   'vehicle_registrations' (models/gold/poc_build_cepik.sql); areas by voivodeship name as printed (lower case),
   matched to the TERYT voivodeship names of dim_geography, 'polska' = Poland. A static list from geo, so the map
   never waits for the source's Silver table (no optional input needed). #}
{% macro poc_source__cepik() %}
    {{ return({'block_model': 'poc_build_cepik', 'blocks': [['vehicle_registrations', 'CEPiK vehicles registered per month']]}) }}
{% endmacro %}

{% macro poc_geography__cepik() %}
cepik as (
    select
        'cepik' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'cepik' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
