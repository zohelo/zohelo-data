{# un_sdg (register GL-018): Gold registration, decision D34 (docs/source-registration.md). Block 'sdg_observations'
   (models/gold/poc_build_un_sdg.sql). #}
{% macro poc_source__un_sdg() %}
    {{ return({'block_model': 'poc_build_un_sdg', 'blocks': [['sdg_observations', 'UN Global SDG Indicators Database']]}) }}
{% endmacro %}

{# Areas by UN M49 code as the SDG database publishes it (no leading zeros): the 27 EU countries, the only areas Silver
   holds (poc_m49_eu27). Static, so the shared map reads no SDG Silver table. #}
{% macro poc_geography__un_sdg() %}
un_sdg_m49 as ({{ poc_m49_eu27() }}),

un_sdg as (
    select
        'un_sdg' as source_system,
        cast(cast(m49 as integer) as varchar) as source_code,
        iso2 as geography_key
    from un_sdg_m49
)
{% endmacro %}
