{# Sejm API (register PLG-024, #481): Gold blocks sitting_days, votings and prints (models/gold/poc_build_sejm.sql).
   D34 registration (docs/source-registration.md). #}
{% macro poc_source__sejm() %}
    {{ return({
        'block_model': 'poc_build_sejm',
        'blocks': [['sitting_days', 'Sejm sitting days'], ['votings', 'Sejm votings'], ['prints', 'Sejm prints']],
    }) }}
{% endmacro %}

{% macro poc_geography__sejm() %}
-- Sejm: a national body, one area (Poland); a static list, so the map never waits for the source's Silver tables.
sejm as (
    select
        'sejm' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
