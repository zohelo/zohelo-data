{# UNdata, UN Statistical Yearbook (gold.build_undata, #2861): D34 registration
   (docs/source-registration.md). #}
{# UN Statistical Yearbook series built (gold.build_undata, #2861): table (entity) × series text as published;
   measure_code is Gold's short code (the Yearbook has none); the unit is the one in the series name. Not built:
   the tables taken from UN World Population Prospects (syb_1, syb_246: the newest year is a projection) and
   from ILO estimates (syb_200, syb_329: the newest year is an estimate ahead of the data), and the other 20
   tables. #}
{% macro poc_undata_series() %}
    select * from (
        values
        ('syb_230', 'UN Statistical Yearbook: GDP and GDP per capita', 'GDP in current prices (millions of US dollars)', 'gdp_current', 'millions of US dollars'),
        ('syb_230', 'UN Statistical Yearbook: GDP and GDP per capita', 'GDP per capita (US dollars)', 'gdp_per_capita', 'US dollars'),
        ('syb_230', 'UN Statistical Yearbook: GDP and GDP per capita', 'GDP real rates of growth (percent)', 'gdp_growth', 'percent'),
        ('syb_285', 'UN Statistical Yearbook: research and development expenditure and researchers', 'Gross domestic expenditure on R&D: as a percentage of GDP', 'gerd_gdp', 'percentage of GDP'),
        ('syb_314', 'UN Statistical Yearbook: internet usage', 'Percentage of individuals using the internet', 'internet_users', 'percentage of individuals'),
        ('syb_325', 'UN Statistical Yearbook: expenditure on health', 'Current health expenditure (% of GDP)', 'health_expenditure', '% of GDP')
    ) as t (entity, block_name, series, measure_code, unit_code)
{% endmacro %}

{% macro poc_source__undata() %}
    {{ return({
        'block_model': 'poc_build_undata',
        'activated': "select distinct 'undata', entity, block_name from (" ~ poc_undata_series() ~ ")",
    }) }}
{% endmacro %}

{# Areas by UN M49 code as published (no leading zeros): the EU27 members (poc_m49_eu27) and 1 "Total, all
   countries or areas" = World. Other regions (150 Europe, 97 European Union …) are not listed and not built.
   Static. #}
{% macro poc_geography__undata() %}
undata_m49 as ({{ poc_m49_eu27() }}),

undata as (
    select
        'undata' as source_system,
        cast(cast(m49 as integer) as varchar) as source_code,
        iso2 as geography_key
    from undata_m49
    union all
    select
        'undata' as source_system,
        '1' as source_code,
        'WB:WLD' as geography_key
)
{% endmacro %}
