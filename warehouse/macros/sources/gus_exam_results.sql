{# gus_exam_results (register PLS-025, #449): Gold registration, decision D34 (docs/source-registration.md). Block
   'exam8' (models/gold/poc_build_gus_exam_results.sql); areas by name as printed (the build joins lower case),
   matched to the TERYT voivodeship names of dim_geography, 'ogółem' = Poland. #}
{% macro poc_source__gus_exam_results() %}
    {{ return({'block_model': 'poc_build_gus_exam_results', 'blocks': [['exam8', 'GUS: mean eighth-grade exam results (CKE/OKE)']]}) }}
{% endmacro %}

{% macro poc_geography__gus_exam_results() %}
gus_exam_results as (
    select
        'gus_exam_results' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gus_exam_results' as source_system,
        'ogółem' as source_code,
        'PL' as geography_key
)
{% endmacro %}
