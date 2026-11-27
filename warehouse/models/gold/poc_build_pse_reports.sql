{{ config(alias='build_pse_reports', tags=['gold_block']) }}
{# Gold build stage: PSE Reports API v2 (register PL-ENV-019), daily observations for Poland (frequency D only).
   - block 'rce_pln': the market energy price RCE, daily mean of the 15-minute values (PLN/MWh as published);
   - block 'his_wlk_cal': power demand, wind and photovoltaic generation, daily means of the 15-minute values (MW as
     published, mean power).
   A day enters only when it is complete for the measure: as many rows as the local day has periods
   (periods_expected: 96, or 92/100 on DST days), at distinct UTC times, each with a value. A day with fewer periods
   or a null value is left out, never partially averaged; no value becomes 0. Only actuals: his-wlk-cal is the report
   after the day, RCE the settled price; no plan or forecast report is read. Each measure has one frequency (the
   measure key says daily_mean). One file per business day: the day's source_version and raw_sha256 are grouped on,
   not picked; two copies of a day would give two rows and fail the grain tests. Means are rounded to 6 decimals so a
   rebuild gives identical values. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'pse_reports'
),

periods as (
    select
        'rce_pln' as block_id,
        'rce_pln' as code,
        business_date,
        dtime_utc,
        periods_expected,
        rce_pln as value,
        source_version,
        raw_sha256
    from {{ ref('pse_reports_rce_pln') }}
    union all
    select
        'his_wlk_cal' as block_id,
        unnest(['demand', 'wi', 'pv']) as code,
        business_date,
        dtime_utc,
        periods_expected,
        unnest([demand, wi, pv]) as value,
        source_version,
        raw_sha256
    from {{ ref('pse_reports_his_wlk_cal') }}
),

days as (
    select
        block_id,
        code,
        business_date,
        source_version,
        raw_sha256,
        min(periods_expected) as periods_expected,
        count(*) as periods,
        count(distinct dtime_utc) as distinct_periods,
        count(value) as values_found,
        round(avg(value), 6) as value
    from periods
    group by block_id, code, business_date, source_version, raw_sha256
),

complete as (
    select
        *,
        case code
            when 'rce_pln' then 'Market energy price RCE, daily mean of 15-minute values (PSE)'
            when 'demand' then 'Power demand of the national power system, daily mean of 15-minute values (PSE)'
            when 'wi' then 'Wind generation, daily mean of 15-minute values (PSE)'
            when 'pv' then 'Photovoltaic generation, daily mean of 15-minute values (PSE)'
        end as measure_name,
        case when block_id = 'rce_pln' then 'PLN/MWh' else 'MW' end as unit_code
    from days
    where periods = periods_expected and distinct_periods = periods_expected and values_found = periods_expected
)

select
    'pse_reports' as source_system,
    s.block_id,
    case
        when s.block_id = 'rce_pln' then 'PSE market energy price RCE, daily means'
        else 'PSE power system demand and generation, daily means'
    end as block_name,
    'pse_reports:' || s.block_id || ':' || s.unit_code || ':' || s.code || ':daily_mean' as measure_key,
    s.measure_name,
    s.code || ':daily_mean' as measure_codes,
    s.unit_code,
    case when s.unit_code = 'MW' then 'MW (mean power)' else 'PLN per MWh' end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    s.business_date as date_key,
    cast(s.business_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from complete as s
left join geo as g on g.source_code = 'PL'
