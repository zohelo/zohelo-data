{# ELI Poland (register PLG-026, #483): Gold block acts (models/gold/poc_build_eli_pl.sql). D34 registration
   (docs/source-registration.md). #}
{% macro poc_source__eli_pl() %}
    {{ return({
        'block_model': 'poc_build_eli_pl',
        'blocks': [['acts', 'Acts published in Dziennik Ustaw and Monitor Polski']],
    }) }}
{% endmacro %}

{% macro poc_geography__eli_pl() %}
-- ELI Poland: the national official journals, one area (Poland); a static list, so the map never waits for Silver.
eli_pl as (
    select
        'eli_pl' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
