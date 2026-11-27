{# irena_stat (register GL-035): Gold registration, decision D34 (docs/source-registration.md). Block 'elecstat'
   (models/gold/poc_build_irena_stat.sql). #}
{% macro poc_source__irena_stat() %}
    {{ return({'block_model': 'poc_build_irena_stat', 'blocks': [['elecstat', 'IRENASTAT: electrical capacity and generation by technology']]}) }}
{% endmacro %}

{# Countries by ISO3 (dim_geography iso3, the World Bank economy code, as cckp_era5 and unhcr; IRENA codes Kosovo XKX
   like the World Bank). Static, so the shared map reads no IRENASTAT Silver table. An IRENA code without a Gold
   country (territories such as BES, GLP or TWN) maps to _X with its code kept and is counted by build_check. #}
{% macro poc_geography__irena_stat() %}
irena_stat as (
    select
        'irena_stat' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}
