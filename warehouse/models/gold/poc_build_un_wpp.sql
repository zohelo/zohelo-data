{{ config(alias='build_un_wpp', tags=['gold_block']) }}
{# Gold build stage: UN World Population Prospects 2024 (medium variant) for the areas of poc_un_wpp_blocks (EU27
   countries, the European Union (EU: 27) group → EUROSTAT:EU27_2020, World → WB:WLD), annual. Every indicator
   is two measures: un_wpp:<indicator>:estimate (1950-2023) and un_wpp:<indicator>:projection (2024-2100,
   observation status forecast), so a projection never becomes a KPI's latest value. Values, unit and scale as
   published (D33): population in thousands (unit thousand_persons); old-age dependency (65+ per 100 aged 15-64) from the broad age groups of
   silver.un_wpp_population_age_groups. Areas through poc_map_geography (section un_wpp). No breakdowns. #}
with blocks as ({{ poc_un_wpp_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'un_wpp'
),

areas as (
    select
        unnest(string_split(countries, ',')) as iso2,
        cast(null as integer) as loc_id
    from (select any_value(countries) as countries from blocks) as b
    union all
    select
        null as iso2,
        1551 as loc_id
    union all
    select
        null as iso2,
        900 as loc_id
),

indicators as (
    select
        d.loc_id,
        d.location,
        d.iso2,
        d.year,
        d.is_projection,
        d.raw_sha256,
        d.source_version,
        unnest(['TPopulation1July', 'TFR', 'LEx', 'MedianAgePop']) as indicator,
        unnest([d.tpopulation1july, d.tfr, d.lex, d.medianagepop]) as value
    from {{ ref('un_wpp_demographic_indicators') }} as d
    inner join areas as a on d.iso2 = a.iso2 or d.loc_id = a.loc_id
    union all
    select
        p.loc_id,
        p.location,
        p.iso2,
        p.year,
        p.is_projection,
        p.raw_sha256,
        p.source_version,
        'OldAgeDependency' as indicator,
        p.old_age_dependency_ratio as value
    from {{ ref('un_wpp_population_age_groups') }} as p
    inner join areas as a on p.iso2 = a.iso2 or p.loc_id = a.loc_id
),

shaped as (
    select
        i.*,
        b.block_name,
        b.unit_code,
        b.unit_name,
        case when i.is_projection then 'projection' else 'estimate' end as kind
    from indicators as i
    inner join blocks as b on i.indicator = b.indicator
    where i.value is not null
)

select
    'un_wpp' as source_system,
    s.indicator as block_id,
    s.block_name,
    'un_wpp:' || s.indicator || ':' || s.kind as measure_key,
    s.block_name || ' (' || case s.kind when 'projection' then 'projection, medium variant' else 'estimate' end || ')'
        as measure_name,
    s.kind as measure_codes,
    s.unit_code,
    s.unit_name,
    coalesce(s.iso2, cast(s.loc_id as varchar)) as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.year, 1, 1) as date_key,
    cast(s.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    case when s.is_projection then 'forecast' else 'ok' end as observation_status_key,
    s.source_version,
    s.raw_sha256
from shaped as s
left join geo_map as g on coalesce(s.iso2, cast(s.loc_id as varchar)) = g.source_code
