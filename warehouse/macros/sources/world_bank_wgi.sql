{# World Bank WGI geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__world_bank_wgi() %}
-- World Bank WGI, Findex and ICP (#2736): World Bank economy codes like WDI's, so each gets WDI's mapping (iso2 or
-- WB:<code>), built from wdi_countries only so that this shared model never waits for their Silver tables. A code
-- WDI does not know (a few territories, ICP benchmark groups) is not listed and the build stage maps it to _X.
world_bank_wgi as ({{ poc_world_bank_economies('world_bank_wgi') }})
{% endmacro %}

{# World Bank WGI: the six governance estimates (#2736). #}
{% macro poc_wgi_blocks() %}
    select * from (
        values
        ('GOV_WGI_VA.EST'), ('GOV_WGI_PV.EST'), ('GOV_WGI_GE.EST'), ('GOV_WGI_RQ.EST'), ('GOV_WGI_RL.EST'),
        ('GOV_WGI_CC.EST')
    ) as t (indicator_code)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__world_bank_wgi() %}
    {{ return({
        'block_model': 'poc_build_wgi',
        'activated': "select 'world_bank_wgi', indicator_code, 'WGI ' || indicator_code from (" ~ poc_wgi_blocks() ~ ")",
    }) }}
{% endmacro %}
