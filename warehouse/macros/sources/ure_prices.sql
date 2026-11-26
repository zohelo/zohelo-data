{# ure_prices (register PL-ENV-020, #2870): Gold registration, decision D34 (docs/source-registration.md). Block
   'energy_prices' (models/gold/poc_build_ure_prices.sql). Poland only: a static list, the map reads no Silver
   table of this source, so it never waits for the source's first load. #}
{% macro poc_source__ure_prices() %}
    {{ return({'block_model': 'poc_build_ure_prices', 'blocks': [['energy_prices', 'URE energy price indicators']]}) }}
{% endmacro %}

{% macro poc_geography__ure_prices() %}
ure_prices as (
    select
        'ure_prices' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
