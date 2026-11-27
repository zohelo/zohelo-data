{{ config(alias='build_un_sdg', tags=['gold_block']) }}
{# Gold build stage: UN Global SDG Indicators Database (register GL-018), block 'sdg_observations', from
   silver.un_sdg_observations (Poland and the other 26 EU countries, every series of the newest release). One measure
   per SDG series code, annual: measure_key un_sdg:sdg_observations:<Units>:<SeriesCode>, e.g.
   un_sdg:sdg_observations:PER_1000_LIVE_BIRTHS:SH_DYN_MORT (a series has one unit in every release read). Every
   disaggregation dimension is the breakdown as published ({"Age": "<5Y", "Reporting Type": "G", "Sex": "FEMALE"});
   is_total when every dimension holds a total code (BOTHSEX, ALLAGE, ALLAREA, _T) or is the only code the series has
   for it; Sex gives sex_code. Values and units as published (D33). Areas by M49 code (poc_geography__un_sdg, the 27 EU
   countries). Not loaded, each counted in the trial: a row without a number (value_kind 'no_value' or 'below': NaN and
   censored values such as '<2.5' stay in Silver), a missing-value status (M, O, Q, L) or nature N (non-relevant), a
   forecast (status F), a period of several years (time_detail '2013-2017' under time_period 2017: not an annual
   observation), and a year after the release's year. Status: observation status B break, D definition_differs, E and I
   estimated, U low_reliability, P provisional, any other code but A flagged; else nature E, M and G (estimated,
   modelled, global monitoring data) estimated; C, CA and NA ok. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'un_sdg'
),

obs as (
    select *
    from {{ ref('un_sdg_observations') }}
    where
        value_kind = 'number'
        and isfinite(value)
        and coalesce(observation_status, '') not in ('M', 'O', 'Q', 'L', 'F')
        and nature <> 'N'
        and (time_detail is null or time_detail = cast(time_period as varchar))
        and time_period <= release_year
),

dimension_maps as (
    select
        observation_id,
        series_code,
        json_transform(dimensions, '"MAP(VARCHAR, VARCHAR)"') as dimension_map
    from obs
),

entries as (
    select
        observation_id,
        series_code,
        unnest(map_keys(dimension_map)) as dimension,
        unnest(map_values(dimension_map)) as code
    from dimension_maps
),

codes as (
    select
        series_code,
        dimension,
        count(distinct code) as n
    from entries
    group by all
),

not_total as (
    select distinct e.observation_id
    from entries as e
    inner join codes as c on e.series_code = c.series_code and e.dimension = c.dimension
    where c.n > 1 and e.code not in ('BOTHSEX', 'ALLAGE', 'ALLAREA', '_T')
)

select
    'un_sdg' as source_system,
    'sdg_observations' as block_id,
    'UN Global SDG Indicators Database' as block_name,
    'un_sdg:sdg_observations:' || o.unit_code || ':' || o.series_code as measure_key,
    o.series_description || ' (UN SDG)' as measure_name,
    o.series_code as measure_codes,
    o.unit_code,
    o.unit_code as unit_name,
    o.geo_area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.time_period, 1, 1) as date_key,
    cast(o.time_period as varchar) as period_label,
    o.dimensions as breakdown_json,
    o.dimensions as breakdown_label_json,
    case json_extract_string(o.dimensions, '$.Sex')
        when 'BOTHSEX' then 'T'
        when 'MALE' then 'M'
        when 'FEMALE' then 'F'
    end as sex_code,
    nt.observation_id is null as is_total,
    o.value,
    case
        when o.observation_status = 'B' then 'break'
        when o.observation_status = 'D' then 'definition_differs'
        when o.observation_status in ('E', 'I') then 'estimated'
        when o.observation_status = 'U' then 'low_reliability'
        when o.observation_status = 'P' then 'provisional'
        when o.observation_status is not null and o.observation_status <> 'A' then 'flagged'
        when o.nature in ('E', 'M', 'G') then 'estimated'
        else 'ok'
    end as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
left join not_total as nt on o.observation_id = nt.observation_id
left join geo as g on o.geo_area_code = g.source_code
