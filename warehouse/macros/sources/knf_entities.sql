{# KNF supervised entities, route 1: ESMA registers, home member state Poland (register PLG-016, #2994): Gold blocks of
   models/gold/poc_build_knf_entities.sql. D34 registration (docs/source-registration.md). #}
{% macro poc_source__knf_entities() %}
    {{ return({
        'block_model': 'poc_build_knf_entities',
        'blocks': [
            ['register_entries', 'Polish entries of the ESMA registers by status'],
            ['authorisations', 'Polish entries of the ESMA registers by year of authorisation'],
            ['withdrawals', 'Polish entries of the ESMA registers by year of withdrawal'],
        ],
    }) }}
{% endmacro %}

{% macro poc_geography__knf_entities() %}
-- ESMA registers, Polish entries: counted for Poland only (the register gives a free-text address, not a coded
-- location); a static list, so the map never waits for the source's Silver table.
knf_entities as (
    select
        'knf_entities' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
