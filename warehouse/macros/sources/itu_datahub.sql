{# itu_datahub (register GL-031): Gold registration, decision D34 (docs/source-registration.md). Block 'itu_dh'
   (models/gold/poc_build_itu_datahub.sql). #}
{% macro poc_source__itu_datahub() %}
    {{ return({'block_model': 'poc_build_itu_datahub', 'blocks': [['itu_dh', 'ITU DataHub: ICT indicators by economy (World Bank Data360 ITU_DH)']]}) }}
{% endmacro %}

{# Economies by ISO3 (dim_geography iso3, the World Bank economy code, as irena_stat and unhcr; Data360 codes Kosovo
   XKX like the World Bank). Static, so the shared map reads no ITU DataHub Silver table. A code without a Gold country
   (historic ANT and YUG, territories such as ASC) maps to _X with its code kept and is counted by build_check. #}
{% macro poc_geography__itu_datahub() %}
itu_datahub as (
    select
        'itu_datahub' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}
