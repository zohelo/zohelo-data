{# openalex (register CD-028, #2996): Gold registration, decision D34 (docs/source-registration.md). Blocks 'works_pl'
   and 'institutions_pl' of models/gold/poc_build_openalex.sql (the names repeat the ones there). The area is
   Poland by the selection rule (institutions.country_code:PL; source code 'PL'): a static row, so the shared map
   reads no Silver table of this source. #}
{% macro poc_source__openalex() %}
    {{ return({'block_model': 'poc_build_openalex', 'blocks': [
        ['works_pl', 'OpenAlex: works with an author affiliated with an institution in Poland, annual'],
        ['institutions_pl', 'OpenAlex: works and open-access works per institution in Poland, annual'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__openalex() %}
openalex as (
    select
        'openalex' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
