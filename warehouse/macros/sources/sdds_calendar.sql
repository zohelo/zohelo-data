{# SDDS advance release calendar of Poland (registers TP-012 NBP, TP-013 Ministry of Finance): the Gold block of
   models/gold/poc_build_sdds_calendar.sql. D34 registration (docs/source-registration.md). #}
{% macro poc_source__sdds_calendar() %}
    {{ return({
        'block_model': 'poc_build_sdds_calendar',
        'blocks': [
            ['releases', 'SDDS advance release calendar: NBP and Ministry of Finance'],
        ],
    }) }}
{% endmacro %}

{% macro poc_geography__sdds_calendar() %}
-- SDDS advance release calendar: national releases, one area (Poland); a static list, so the map never waits for the
-- source's Silver table.
sdds_calendar as (
    select
        'sdds_calendar' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
