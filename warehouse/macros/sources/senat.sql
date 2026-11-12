{# Senate of the Republic of Poland (register PLG-025, #482, route D36): Gold blocks sitting_days and votes
   (models/gold/poc_build_senat.sql). D34 registration (docs/source-registration.md). #}
{% macro poc_source__senat() %}
    {{ return({
        'block_model': 'poc_build_senat',
        'blocks': [['sitting_days', 'Senate sitting days (term XI)'], ['votes', 'Senate votes (term XI)']],
    }) }}
{% endmacro %}

{% macro poc_geography__senat() %}
-- Senate: a national body, one area (Poland); a static list, so the map never waits for the source's Silver tables.
senat as (
    select
        'senat' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
