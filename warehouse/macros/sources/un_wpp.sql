{# UN WPP geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__un_wpp() %}
-- UN WPP: locations by ISO two-letter code (countries and areas, matched to dim_geography's country keys) and the
-- groups of poc_un_wpp_areas by LocID (1551 European Union (EU: 27) = EU27_2020, 900 World); static, so Gold does
-- not read WPP Silver here. Other groups have no Gold area (_X); the build stage builds only poc_un_wpp_blocks areas.
un_wpp_areas as ({{ poc_un_wpp_areas() }}),

un_wpp as (
    select
        'un_wpp' as source_system,
        loc_id as source_code,
        candidate_key as geography_key
    from un_wpp_areas
    union all
    select
        'un_wpp' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
)
{% endmacro %}

{# UN WPP blocks: one per indicator (gold.build_un_wpp); countries: the EU27 members built (ISO two-letter). #}
{% macro poc_un_wpp_blocks() %}
    select
        *,
        'AT,BE,BG,CY,CZ,DE,DK,EE,ES,FI,FR,GR,HR,HU,IE,IT,LT,LU,LV,MT,NL,PL,PT,RO,SE,SI,SK' as countries
    from (
        values
        ('TPopulation1July', 'Total population on 1 July (UN WPP)', 'thousand_persons', 'thousand persons'),
        ('TFR', 'Total fertility rate (UN WPP)', 'children_per_woman', 'live births per woman'),
        ('LEx', 'Life expectancy at birth, both sexes (UN WPP)', 'years', 'years'),
        ('MedianAgePop', 'Median age of the population (UN WPP)', 'years', 'years'),
        ('OldAgeDependency', 'Old-age dependency ratio, 65+ per 100 aged 15-64, computed from UN WPP 2024 five-year age groups', 'per_100', 'per 100 persons aged 15-64')
    ) as t (indicator, block_name, unit_code, unit_name)
{% endmacro %}

{# UN WPP groups → Gold area by LocID (poc_map_geography section un_wpp; static). Countries map by ISO code. #}
{% macro poc_un_wpp_areas() %}
    select * from (
        values
        ('1551', 'EUROSTAT:EU27_2020'), ('900', 'WB:WLD')
    ) as t (loc_id, candidate_key)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__un_wpp() %}
    {{ return({
        'block_model': 'poc_build_un_wpp',
        'activated': "select 'un_wpp', indicator, block_name from (" ~ poc_un_wpp_blocks() ~ ")",
    }) }}
{% endmacro %}
