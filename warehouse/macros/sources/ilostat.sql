{# ILOSTAT (gold.build_ilostat, #2861): D34 registration (docs/source-registration.md). #}
{# ILOSTAT indicator files built (gold.build_ilostat, #2861): block = landed entity, one frequency each (the
   annual and the quarterly unemployment file are separate blocks and measures). #}
{% macro poc_ilostat_blocks() %}
    select * from (
        values
        ('une_deap_sex_age_rt_a', 'ILOSTAT unemployment rate by sex and age, annual', 'Unemployment rate'),
        ('une_deap_sex_age_rt_q', 'ILOSTAT unemployment rate by sex and age, quarterly', 'Unemployment rate'),
        ('emp_dwap_sex_age_rt_a', 'ILOSTAT employment-to-population ratio by sex and age, annual', 'Employment-to-population ratio'),
        ('eap_dwap_sex_age_rt_a', 'ILOSTAT labour force participation rate by sex and age, annual', 'Labour force participation rate'),
        ('eip_neet_sex_rt_a', 'ILOSTAT share of youth not in employment, education or training, annual', 'Share of youth (15-24) not in employment, education or training')
    ) as t (entity, block_name, measure_label)
{% endmacro %}

{# ILOSTAT age bands built: the youth/adult classification (classif1), each band its own measure. The 5- and
   10-year bands and the AGE_AGGREGATE bands are not built. #}
{% macro poc_ilostat_ages() %}
    select * from (
        values
        ('AGE_YTHADULT_YGE15', '15+'), ('AGE_YTHADULT_Y15-24', '15-24'), ('AGE_YTHADULT_YGE25', '25+'),
        ('AGE_YTHADULT_Y15-64', '15-64')
    ) as t (classif1, age_label)
{% endmacro %}

{% macro poc_source__ilostat() %}
    {{ return({
        'block_model': 'poc_build_ilostat',
        'activated': "select 'ilostat', entity, block_name from (" ~ poc_ilostat_blocks() ~ ")",
    }) }}
{% endmacro %}

{# Countries by ISO 3166 alpha-3 (dim_geography.iso3), from Gold's own list so the shared dimensions never wait
   for ILOSTAT Silver. ILO aggregates (X01 …) are not listed; the build stage builds the EU27 members. #}
{% macro poc_geography__ilostat() %}
ilostat as (
    select
        'ilostat' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}
