{# dane.gov.pl open-data catalogue via data.europa.eu (register PLG-030, #3263): Gold block catalogue
   (models/gold/poc_build_dane_gov_pl.sql). D34 registration (docs/source-registration.md). #}
{% macro poc_source__dane_gov_pl() %}
    {{ return({
        'block_model': 'poc_build_dane_gov_pl',
        'blocks': [['catalogue', 'dane.gov.pl open-data catalogue (datasets and distributions)']],
    }) }}
{% endmacro %}

{% macro poc_geography__dane_gov_pl() %}
-- dane.gov.pl: the national open-data catalogue, one area (Poland); a static list, so the map never waits for the
-- source's Silver tables.
dane_gov_pl as (
    select
        'dane_gov_pl' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
