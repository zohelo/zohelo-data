{# World Bank Findex geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__world_bank_findex() %}
-- World Bank WGI, Findex and ICP (#2736): World Bank economy codes like WDI's, so each gets WDI's mapping (iso2 or
-- WB:<code>), built from wdi_countries only so that this shared model never waits for their Silver tables. A code
-- WDI does not know (a few territories, ICP benchmark groups) is not listed and the build stage maps it to _X.
world_bank_findex as ({{ poc_world_bank_economies('world_bank_findex') }})
{% endmacro %}

{# World Bank Global Findex: account ownership, age 15+ (#2736). #}
{% macro poc_findex_blocks() %}
    select * from (
        values
        ('account_t_d', 'Account ownership (% age 15+)')
    ) as t (indicator, block_name)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__world_bank_findex() %}
    {{ return({
        'block_model': 'poc_build_findex',
        'activated': "select 'world_bank_findex', indicator, block_name from (" ~ poc_findex_blocks() ~ ")",
    }) }}
{% endmacro %}
