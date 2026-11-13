{# esma_registers (register PLG-016, ESMA route): Gold registration, decision D34 (docs/source-registration.md). Block
   'upreg' (models/gold/poc_build_esma_registers.sql): counts of the ESMA register entries with home member state
   Poland. The only area is the home member state as ESMA spells it ('POLAND' = PL), a static list, so the map never
   waits for the source's Silver table. #}
{% macro poc_source__esma_registers() %}
    {{ return({'block_model': 'poc_build_esma_registers', 'blocks': [['upreg', 'ESMA register entries, home member state Poland (snapshot counts)']]}) }}
{% endmacro %}

{% macro poc_geography__esma_registers() %}
esma_registers as (
    select
        'esma_registers' as source_system,
        'POLAND' as source_code,
        'PL' as geography_key
)
{% endmacro %}
