{# policja_statistics (register PLG-033): Gold registration, decision D34 (docs/source-registration.md). Blocks = the
   category workbooks (models/gold/poc_build_policja_statistics.sql). Areas by name as printed: 'Polska' = Poland,
   'woj. <name>' = the TERYT voivodeship of that name (lower case) in dim_geography. The two police areas inside
   Mazowieckie ('teren działania KSP Warszawa', 'teren działania KWP z/s w Radomiu') have no official area and are
   not built. Static (reads only geo). #}
{% macro poc_source__policja_statistics() %}
    {{ return({'block_model': 'poc_build_policja_statistics', 'blocks': [
        ['ogolem', 'Police statistics: all offences by voivodeship (annual)'],
        ['wybrane_7', 'Police statistics: seven selected offence categories by voivodeship (annual)'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__policja_statistics() %}
policja_statistics as (
    select
        'policja_statistics' as source_system,
        'woj. ' || lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'policja_statistics' as source_system,
        'Polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}
