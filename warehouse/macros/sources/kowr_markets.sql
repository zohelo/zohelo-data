{# kowr_markets (register PLS-033): Gold registration, decision D34 (docs/source-registration.md). Block
   'fob_export_prices' (models/gold/poc_build_kowr_markets.sql). Areas of the export ports: 'US' and 'FR' as ISO codes;
   'BLACK_SEA' has no area and stays unmatched ('_X'). Static: the map never waits for this source. #}
{% macro poc_source__kowr_markets() %}
    {{ return({'block_model': 'poc_build_kowr_markets', 'blocks': [['fob_export_prices', 'KOWR grain export prices FOB (wheat, barley, maize)']]}) }}
{% endmacro %}

{% macro poc_geography__kowr_markets() %}
kowr_markets as (
    select
        'kowr_markets' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 in ('US', 'FR')
)
{% endmacro %}
