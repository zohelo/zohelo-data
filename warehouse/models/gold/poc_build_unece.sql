{{ config(alias='build_unece', tags=['gold_block']) }}
{# Gold build stage: the UNECE Statistical Database tables of poc_unece_blocks (macros/sources/unece.sql) from
   silver.unece_observations in the common shape. One measure per table and combination of its variable value
   codes (poc_unece_measures): measure_key unece:<table id>:<unit as the label prints it>[:<codes>], e.g.
   unece:40_en_MEUnRateY_r:percent (unemployment rate, annual), unece:0_en_MECCPriceInterestY_r:percent:c0000771
   (consumer price index, growth rate), unece:10_en_MELFOverviewY_r:Thousands of persons:ME.47.c0000803 (total
   population). Each table has one frequency (annual, quarterly or monthly), so each measure has one. No
   breakdowns: the tables built carry only the measure variables and the country. Values and units as published
   (D33: thousands of persons stay thousands, indices stay 2021 = 100). The JSON-stat files carry no status and
   no cell without a value reaches Silver; a 0 in a level series (poc_unece_measures.zero_is_missing) is a value
   below the printed precision or a cell without a value and is not loaded (148 cells, 1990-1997), a rate of 0 is
   kept. A period that starts after the build day cannot be an
   observation and is left out (none today). Areas through poc_map_geography (section unece, static): the 56
   countries by UN M49 code, E27 → EUROSTAT:EU27_2020, EAA → EUROSTAT:EA; the UNECE's own groupings (CIS, CWG,
   EAEU, ECE, NA, ST7) have no Gold area and are not built; any other code maps to _X and is counted by
   build_check. #}
with blocks as ({{ poc_unece_blocks() }}),

measures as ({{ poc_unece_measures() }}),

not_built_areas as (
    select area_code
    from ({{ poc_unece_areas() }})
    where iso2 = ''
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'unece'
),

obs as (
    select
        o.*,
        b.block_name,
        b.frequency,
        {{ poc_unece_codes() | trim }} as codes,
        case
            when b.frequency = 'A' and regexp_matches(o.time_period, '^[0-9]{4}$')
                then make_date(cast(o.time_period as integer), 1, 1)
            when b.frequency = 'Q' and regexp_matches(o.time_period, '^[0-9]{4}Q[1-4]$')
                then
                    make_date(
                        cast(left(o.time_period, 4) as integer), cast(right(o.time_period, 1) as integer) * 3 - 2, 1
                    )
            when b.frequency = 'M' and regexp_matches(o.time_period, '^[0-9]{4}M(0[1-9]|1[0-2])$')
                then make_date(cast(left(o.time_period, 4) as integer), cast(right(o.time_period, 2) as integer), 1)
        end as date_key
    from {{ ref('unece_observations') }} as o
    inner join blocks as b on o.table_id = b.table_id
    where
        o.obs_value is not null
        and o.area_code not in (select n.area_code from not_built_areas as n)
)

select
    'unece' as source_system,
    o.table_id as block_id,
    o.block_name,
    'unece:' || o.table_id || ':' || m.unit_code || case when m.codes = '' then '' else ':' || m.codes end
        as measure_key,
    m.measure_name,
    m.codes as measure_codes,
    m.unit_code,
    m.unit_name,
    o.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    o.frequency,
    o.date_key,
    o.time_period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.obs_value as value,
    case when o.obs_status is null then 'ok' else 'flagged' end as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
inner join measures as m on o.table_id = m.table_id and o.codes = m.codes
left join geo_map as g on o.area_code = g.source_code
where
    o.date_key is not null
    and o.date_key <= current_date
    and not (m.zero_is_missing and o.obs_value = 0)
