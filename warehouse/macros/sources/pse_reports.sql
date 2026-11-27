{# pse_reports (register PL-ENV-019): Gold registration, decision D34 (docs/source-registration.md). Blocks 'rce_pln'
   and 'his_wlk_cal' (models/gold/poc_build_pse_reports.sql); every value is national, source code 'PL' = Poland. A
   static list: it reads neither the source's Silver tables nor any CTE it does not define, so the map never waits for
   the source. #}
{% macro poc_source__pse_reports() %}
    {{ return({'block_model': 'poc_build_pse_reports', 'blocks': [
        ['rce_pln', 'PSE market energy price RCE, daily means'],
        ['his_wlk_cal', 'PSE power system demand and generation, daily means'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__pse_reports() %}
pse_reports as (
    select
        'pse_reports' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
