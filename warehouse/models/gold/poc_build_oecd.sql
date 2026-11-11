{{ config(alias='build_oecd') }}
{# Gold build stage: OECD composite leading indicators and confidence indicators (silver.oecd_cli,
   dataflow DSD_STES@DF_CLI) in the common shape, monthly. The measures are listed in
   poc_oecd_measures (macros/poc_blocks.sql): business and consumer confidence (Poland from 1997 / 2001)
   and the amplitude-adjusted CLI (22 areas, not Poland or the EU). measure_key
   oecd:cli:<unit>:<measure>:<adjustment>; no breakdowns, every series is a total. Areas through
   map_geography (ISO 3166 alpha-3, OECD aggregates). SDMX OBS_STATUS → Gold status. #}
with measures as ({{ poc_oecd_measures() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'oecd'
),

rows_ as (
    select
        o.*,
        m.measure_name,
        case
            when o.freq = 'M' and regexp_matches(o.time_period, '^[0-9]{4}-[0-9]{2}$')
                then cast(o.time_period || '-01' as date)
            when o.freq = 'Q' and regexp_matches(o.time_period, '^[0-9]{4}-Q[1-4]$')
                then make_date(
                    cast(substr(o.time_period, 1, 4) as integer),
                    cast(substr(o.time_period, 7, 1) as integer) * 3 - 2,
                    1
                )
        end as date_key
    from {{ ref('oecd_cli') }} as o
    inner join measures as m
        on o.measure = m.measure and o.adjustment = m.adjustment and o.transformation = m.transformation
    where o.obs_value is not null
)

select
    'oecd' as source_system,
    'cli' as block_id,
    'OECD composite leading indicators and confidence indicators' as block_name,
    'oecd:cli:' || r.unit_measure || ':' || r.measure || ':' || r.adjustment as measure_key,
    r.measure_name,
    r.measure || ':' || r.adjustment as measure_codes,
    r.unit_measure as unit_code,
    'index points, long-term average = 100' as unit_name,
    r.ref_area as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    r.freq as frequency,
    r.date_key,
    r.time_period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    r.obs_value as value,
    case r.obs_status
        when 'P' then 'provisional' when 'E' then 'estimated' when 'F' then 'forecast' when 'B' then 'break'
        else 'ok'
    end as observation_status_key,
    r.source_version,
    r.raw_sha256
from rows_ as r
left join geo_map as g on r.ref_area = g.source_code
where r.date_key is not null
