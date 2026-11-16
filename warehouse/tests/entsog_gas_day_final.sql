-- entsog (gold.build_entsog): a daily sum holds only gas days that have ended. GAZ-SYSTEM publishes running values
-- during the gas day (review of #3124: at 03:39 UTC on 16 Nov the trial held gas day 15 Nov, which ran to 05:00 UTC,
-- as complete), so every Silver value behind a daily row, in its scope (all points of the direction, or the group),
-- must have been updated at or after the end of its gas day, and no daily row may be a gas day that ends after now.
with groups as ({{ poc_entsog_point_groups() }}),

daily as (
    select
        geography_source_code as operator_key,
        measure_codes as direction_key,
        date_key as gas_day,
        json_extract_string(breakdown_json, '$.point_group') as scope_key,
        is_total
    from {{ ref('poc_build_entsog') }}
    where frequency = 'D'
),

running as (
    select
        f.operator_key,
        f.direction_key,
        f.gas_day,
        coalesce(g.group_key, 'unmapped') as group_key,
        f.point_key,
        f.last_update_utc,
        f.period_to_utc
    from {{ ref('entsog_physical_flow') }} as f
    left join groups as g on f.point_key = g.point_key
    where
        f.indicator = 'Physical Flow'
        and f.period_type = 'day'
        and f.value_kwh_d is not null
        and (f.last_update_utc is null or f.last_update_utc < f.period_to_utc or f.period_to_utc > now())
)

select
    d.operator_key,
    d.direction_key,
    d.gas_day,
    coalesce(d.scope_key, 'total') as scope,
    r.point_key,
    r.last_update_utc,
    r.period_to_utc
from daily as d
inner join running as r
    on
        d.operator_key = r.operator_key
        and d.direction_key = r.direction_key
        and d.gas_day = r.gas_day
        and (d.is_total or d.scope_key = r.group_key)
