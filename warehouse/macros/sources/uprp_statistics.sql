{# uprp_statistics (register EX-001): Gold registration, decision D34 (docs/source-registration.md). Block
   'annual_reports' (models/gold/poc_build_uprp_statistics.sql). The reports give Poland only: 'polska' = PL, a
   static code, so the shared map reads no Silver table of this source. #}
{% macro poc_source__uprp_statistics() %}
    {{ return({'block_model': 'poc_build_uprp_statistics', 'blocks': [
        ['annual_reports', 'UPRP industrial-property statistics (annual reports)'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__uprp_statistics() %}
uprp_statistics as (
    select
        'uprp_statistics' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
