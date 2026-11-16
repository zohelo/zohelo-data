{# crossref (register CD-029): Gold registration, decision D34 (docs/source-registration.md). Block 'works_pl'
   (models/gold/poc_build_crossref.sql): works with affiliation text naming Poland or Polska (lower bound; can
   match foreign addresses); the area is Poland by the selection rule (source code 'PL'). The block name repeats
   the one in poc_build_crossref.sql. #}
{% macro poc_source__crossref() %}
    {%- set name = 'Crossref works with affiliation text naming Poland or Polska (lower bound; can match foreign'
        ~ ' addresses), annual; funder breakdowns hold at most the 1,000 most frequent funders per year' -%}
    {{ return({'block_model': 'poc_build_crossref', 'blocks': [['works_pl', name]]}) }}
{% endmacro %}

{% macro poc_geography__crossref() %}
crossref as (
    select
        'crossref' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
