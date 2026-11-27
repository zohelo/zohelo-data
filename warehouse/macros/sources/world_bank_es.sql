{# world_bank_es (register GL-010): Gold registration, decision D34 (docs/source-registration.md). Block 'wb_es'
   (models/gold/poc_build_world_bank_es.sql). #}
{% macro poc_source__world_bank_es() %}
    {{ return({'block_model': 'poc_build_world_bank_es', 'blocks': [['wb_es', 'World Bank Enterprise Surveys: all-firms indicators by economy (Data360 WB_ES)']]}) }}
{% endmacro %}

{# Economies by ISO3 (dim_geography iso3, the World Bank economy code, as itu_datahub). Static, so the shared map
   reads no Enterprise Surveys Silver table. A code without a Gold country maps to _X with its code kept and is
   counted by build_check. #}
{% macro poc_geography__world_bank_es() %}
world_bank_es as (
    select
        'world_bank_es' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}
