{# World Bank WGI geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__world_bank_wgi() %}
-- World Bank WGI, Findex and ICP (#2736): World Bank economy codes like WDI's, so each gets WDI's mapping (iso2 or
-- WB:<code>), built from wdi_countries only so that this shared model never waits for their Silver tables. A code
-- WDI does not know (a few territories, ICP benchmark groups) is not listed and the build stage maps it to _X.
world_bank_wgi as ({{ poc_world_bank_economies('world_bank_wgi') }})
{% endmacro %}
