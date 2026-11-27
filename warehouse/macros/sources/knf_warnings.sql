{# KNF public warning list (register PLG-019): the Gold block of models/gold/poc_build_knf_warnings.sql. D34
   registration (docs/source-registration.md). #}
{% macro poc_source__knf_warnings() %}
    {{ return({
        'block_model': 'poc_build_knf_warnings',
        'blocks': [
            ['warning_list', 'KNF public warning list'],
        ],
    }) }}
{% endmacro %}

{% macro poc_geography__knf_warnings() %}
-- KNF public warning list: one national list, one area (Poland); a static list, so the map never waits for the
-- source's Silver table.
knf_warnings as (
    select
        'knf_warnings' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
