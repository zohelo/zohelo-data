{# Decisions of the President of UOKiK (register PLG-020) and concentration decisions among them (PLG-021): Gold
   blocks of models/gold/poc_build_uokik_decisions.sql. D34 registration (docs/source-registration.md). #}
{% macro poc_source__uokik_decisions() %}
    {{ return({
        'block_model': 'poc_build_uokik_decisions',
        'blocks': [
            ['decisions', 'Decisions of the President of UOKiK (decisions database)'],
            ['concentration', 'Concentration decisions of the President of UOKiK (DKK)'],
        ],
    }) }}
{% endmacro %}

{% macro poc_geography__uokik_decisions() %}
-- UOKiK decisions: decisions of a national authority, one area (Poland); a static list, so the map never waits for
-- the source's Silver table.
uokik_decisions as (
    select
        'uokik_decisions' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
