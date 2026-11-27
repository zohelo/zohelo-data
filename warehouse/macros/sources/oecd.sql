{# OECD geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__oecd() %}
-- OECD (#2738): countries by ISO 3166 alpha-3 (dim_geography iso3); the codes of macro poc_oecd_areas that equal a
-- Eurostat aggregate (EU27_2020, EA20, EA19; #2775) map to it, the others are OECD:<code> members. Built from Gold's
-- own lists, not from silver.oecd_cli, so the dimensions never wait for the OECD source; codes outside them are
-- unmatched (_X).
oecd_areas as ({{ poc_oecd_areas() }}),

oecd as (
    select
        'oecd' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'oecd' as source_system,
        ref_area as source_code,
        'EUROSTAT:' || eurostat_code as geography_key
    from oecd_areas
    where eurostat_code <> ''
    union all
    select
        'oecd' as source_system,
        replace(g.geography_key, 'OECD:', '') as source_code,
        g.geography_key
    from geo as g
    where g.geography_key like 'OECD:%'
)
{% endmacro %}

{# OECD composite leading and confidence indicators (silver.oecd_cli, #2738): the measures Gold builds.
   Amplitude adjusted (AA), index points, long-term average = 100. The CLI (LI) covers 22 areas, not
   Poland or the EU; business (BCICP) and consumer (CCICP) confidence cover Poland. #}
{% macro poc_oecd_measures() %}
    select * from (
        values
        ('BCICP', 'AA', 'IX', 'Business confidence indicator (BCI), amplitude adjusted, long-term average = 100'),
        ('CCICP', 'AA', 'IX', 'Consumer confidence indicator (CCI), amplitude adjusted, long-term average = 100'),
        ('LI', 'AA', 'IX', 'Composite leading indicator (CLI), amplitude adjusted, long-term average = 100')
    ) as t (measure, adjustment, transformation, measure_name)
{% endmacro %}

{# OECD area codes that are not ISO 3166 alpha-3 countries: their names, and the Eurostat aggregate they equal
   (EU27_2020, EA20, EA19; #2775). A code without one is an OECD:<code> member. #}
{% macro poc_oecd_areas() %}
    select * from (
        values
        ('EU27_2020', 'European Union (27 countries from 2020)', 'EU27_2020'),
        ('EA20', 'Euro area (20 countries)', 'EA20'),
        ('EA19', 'Euro area (19 countries)', 'EA19'),
        ('EU', 'European Union (OECD composite)', ''),
        ('OECDXEA', 'OECD excluding the euro area', ''),
        ('OECD', 'OECD total', ''),
        ('OECDE', 'OECD Europe', ''),
        ('G7', 'G7', ''),
        ('G20', 'G20', ''),
        ('A5M', 'Major five Asia economies', ''),
        ('G4E', 'Major four European economies', ''),
        ('NAFTA', 'NAFTA', '')
    ) as t (ref_area, name_en, eurostat_code)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__oecd() %}
    {{ return({
        'block_model': 'poc_build_oecd',
        'blocks': [
            ['cli', 'OECD composite leading indicators and confidence indicators'],
        ],
    }) }}
{% endmacro %}
