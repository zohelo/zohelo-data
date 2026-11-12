{{ config(alias='build_ecb', tags=['gold_block']) }}
{# Gold build stage: the activated ECB Data Portal selections (macros/poc_blocks.sql poc_ecb_blocks) in the common
   shape. measure_key ecb:<flow>:<codes that define the measure>: EXR ecb:EXR:<D|M>:<currency>:EUR (units of the
   currency per 1 EUR: daily reference rates and the ECB's own monthly averages; geography not applicable `_Z`;
   the PLN series carries the breakdown {"currency":"EUR"} as the NBP rates do: the zloty price of one euro), FM ecb:FM:<rate id> (key ECB rates at their change dates),
   EST ecb:EST:WT, HICP/ICP ecb:<flow>:<item>:ANR, IRS ecb:IRS:L40 (10-year convergence yield in the country's
   currency), BSI ecb:BSI:<item>:<data type>:<suffix>. Areas through poc_map_geography (section ecb, static codes
   of poc_ecb_areas): ISO countries, U2 → EUROSTAT:EA, I8 → NUTS:EA19, I9 → NUTS:EA20, B6 → EUROSTAT:EU27_2020,
   B0/D0 → EUROSTAT:EU; FM, EST and BSI are euro area (U2). IRS: an area with series in two currencies (BG: lev
   and euro) has as its total the series with the latest last period, then the longest, then the lowest
   series_key; the other keeps its currency as the breakdown. #}
with blocks as ({{ poc_ecb_blocks() }}),

obs as (
    select
        o.*,
        json_extract_string(o.dimensions, '$.PROVIDER_FM_ID') as fm_rate,
        json_extract_string(o.dimensions, '$.ICP_ITEM') as icp_item,
        json_extract_string(o.dimensions, '$.ICP_SUFFIX') as icp_suffix,
        json_extract_string(o.dimensions, '$.BS_ITEM') as bs_item,
        json_extract_string(o.dimensions, '$.DATA_TYPE') as bs_data_type,
        json_extract_string(o.dimensions, '$.BS_SUFFIX') as bs_suffix,
        json_extract_string(o.dimensions, '$.CURRENCY_TRANS') as irs_currency,
        count(*) over (partition by o.dataflow, o.series_key) as series_points,
        max(o.period_start) over (partition by o.dataflow, o.series_key) as series_last
    from {{ ref('ecb_observations') }} as o
    inner join blocks as b on o.entity = b.entity
    where
        o.obs_value is not null
        and o.period_start is not null
        and (o.dataflow <> 'EXR' or list_contains(string_split(b.currencies, ','), o.currency))
),

shaped as (
    select
        *,
        case dataflow
            when 'EXR' then 'ecb:EXR:' || freq || ':' || currency || ':EUR'
            when 'FM' then 'ecb:FM:' || fm_rate
            when 'EST' then 'ecb:EST:WT'
            when 'IRS' then 'ecb:IRS:L40'
            when 'BSI' then 'ecb:BSI:' || bs_item || ':' || bs_data_type || ':' || bs_suffix
            else 'ecb:' || dataflow || ':' || icp_item || ':' || icp_suffix
        end as measure_key,
        case dataflow
            when 'EXR' then freq || ':' || currency
            when 'FM' then fm_rate
            when 'EST' then 'WT'
            when 'IRS' then 'L40'
            when 'BSI' then bs_item || ':' || bs_data_type || ':' || bs_suffix
            else icp_item || ':' || icp_suffix
        end as measure_codes,
        case
            when dataflow = 'EXR' then currency || '/EUR'
            when unit = 'EUR' and unit_mult = 6 then 'MIO_EUR'
            when unit in ('PCPA', 'PC') then 'PC'
            when unit = 'PCCH' then 'PCCH'
            else coalesce(unit, 'as named')
        end as unit_code,
        case
            when dataflow = 'EXR' then currency || ' per 1 EUR'
            when unit = 'EUR' and unit_mult = 6 then 'million euro'
            when unit = 'PCPA' then 'percent per annum'
            when unit = 'PC' then 'percent'
            when unit = 'PCCH' then 'percentage change'
            else coalesce(unit, 'as named')
        end as unit_name,
        case
            when dataflow = 'EXR' then null
            when dataflow in ('FM', 'EST', 'BSI') then coalesce(ref_area, 'U2')
            else ref_area
        end as area_code,
        case when freq = 'B' then 'D' else freq end as frequency,
        dataflow <> 'IRS' or series_key = first_value(series_key) over (
            partition by dataflow, ref_area order by series_last desc, series_points desc, series_key asc
        ) as is_total_candidate
    from obs
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'ecb'
),

mapped as (
    select
        s.*,
        case when s.area_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key
    from shaped as s
    left join geo_map as g on s.area_code = g.source_code
)

select
    'ecb' as source_system,
    m.entity as block_id,
    b.block_name,
    m.measure_key,
    coalesce(m.title, m.measure_key) as measure_name,
    m.measure_codes,
    m.unit_code,
    m.unit_name,
    m.area_code as geography_source_code,
    m.geography_key,
    m.frequency,
    m.period_start as date_key,
    m.time_period as period_label,
    case
        when m.dataflow = 'EXR' and m.currency = 'PLN' then '{"currency":"EUR"}'
        when m.is_total_candidate then '{}'
        else '{"currency":"' || m.irs_currency || '"}'
    end as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    m.is_total_candidate as is_total,
    m.obs_value as value,
    case m.obs_status
        when 'P' then 'provisional' when 'E' then 'estimated' when 'F' then 'forecast' when 'B' then 'break'
        when 'D' then 'definition_differs' else 'ok'
    end as observation_status_key,
    m.source_version,
    m.raw_sha256
from mapped as m
inner join blocks as b on m.entity = b.entity
