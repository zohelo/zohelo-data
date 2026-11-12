{# crossref (register CD-029): Gold registration, decision D34 (docs/source-registration.md). Block 'works_pl'
   (models/gold/poc_build_crossref.sql): works with a Polish author affiliation; the area is Poland by the selection
   rule (source code 'PL'). #}
{% macro poc_source__crossref() %}
    {{ return({'block_model': 'poc_build_crossref', 'blocks': [['works_pl', 'Crossref works with a Polish author affiliation (annual)']]}) }}
{% endmacro %}

{% macro poc_geography__crossref() %}
crossref as (
    select
        'crossref' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
