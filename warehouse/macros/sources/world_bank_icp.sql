{# World Bank ICP geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__world_bank_icp() %}
-- World Bank WGI, Findex and ICP (#2736): World Bank economy codes like WDI's, so each gets WDI's mapping (iso2 or
-- WB:<code>), built from wdi_countries only so that this shared model never waits for their Silver tables. A code
-- WDI does not know (a few territories, ICP benchmark groups) is not listed and the build stage maps it to _X.
world_bank_icp as ({{ poc_world_bank_economies('world_bank_icp') }})
{% endmacro %}

{# World Bank ICP 2021: GDP per capita in PPP and the GDP price level index (#2736). #}
{% macro poc_icp_blocks() %}
    select * from (
        values
        ('1000000', 'PCAP.PP', 'ICP GDP per capita, PPP-based (international $)'),
        ('1000000', 'PX.WL', 'ICP GDP price level index (world = 100)')
    ) as t (series_code, classification_code, block_name)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__world_bank_icp() %}
    {{ return({
        'block_model': 'poc_build_icp',
        'activated': "select 'world_bank_icp', series_code || ':' || classification_code, block_name from (" ~ poc_icp_blocks() ~ ")",
    }) }}
{% endmacro %}
