{{ config(alias='build_entsog', tags=['gold_block']) }}
{# Gold build stage: ENTSOG TP physical gas flows of GAZ-SYSTEM (silver.entsog_physical_flow), Poland (operator
   PL-TSO-0002 → PL through poc_geography__entsog; the platform has no regional grain). Block physical_flow.
   Measures (one frequency each):
     entsog:physical_flow:kWh/d:<entry|exit>             daily, kWh/d as published; breakdown {} = every point of
                                                         the direction (is_total), {"point_group": <key>} per group
                                                         of poc_entsog_point_groups (unlisted points: 'unmapped');
     entsog:physical_flow_month_sum:kWh:<entry|exit>     monthly sum of the daily values (kWh), same breakdowns.
   Completeness rule: a point-direction "reports" in a gas month when it has at least one value in that month
   (virtual and trading points published only as isNA never report). It is expected on a gas day when it reports in
   that month and has a record that day (points start and stop mid-month; tests/entsog_physical_flow_complete.sql
   fails on a missing record inside a point's span). A daily sum is built only when every expected point-direction
   of its scope (all points of the direction, or the group) has a value that day: a day with an expected point
   published as isNA is left out, never published as if complete. A monthly sum is built only for an ended month
   whose every day has a complete daily sum. observation_status_key = 'provisional' when any value summed
   is Provisional (or any day of the month is), else 'ok'. No unit conversion (D33). #}
with groups as ({{ poc_entsog_point_groups() }}),

flows as (
    select
        f.operator_key,
        f.point_key,
        f.direction_key,
        f.gas_day,
        date_trunc('month', f.gas_day) as gas_month,
        f.value_kwh_d,
        f.flow_status,
        f.source_version,
        f.raw_sha256,
        bool_or(f.value_kwh_d is not null) over (
            partition by f.operator_key, f.point_key, f.direction_key, date_trunc('month', f.gas_day)
        ) as reports_in_month,
        coalesce(g.group_key, 'unmapped') as group_key,
        coalesce(g.group_label, 'Points not in the reviewed group list') as group_label
    from {{ ref('entsog_physical_flow') }} as f
    left join groups as g on f.point_key = g.point_key
    where f.indicator = 'Physical Flow' and f.period_type = 'day'
),

-- one row per scope: the total of the direction (group_key null) and each group
scoped as (
    select
        *,
        cast(null as varchar) as scope_key,
        cast(null as varchar) as scope_label
    from flows
    union all
    select
        *,
        group_key as scope_key,
        group_label as scope_label
    from flows
),

daily as (
    select
        s.operator_key,
        s.direction_key,
        s.scope_key,
        min(s.scope_label) as scope_label,  -- one label per group key
        s.gas_day,
        s.gas_month,
        sum(s.value_kwh_d) as value,
        count(distinct s.point_key) filter (where s.value_kwh_d is not null) as points_with_value,
        count(distinct s.point_key) filter (where s.reports_in_month) as points_expected,
        bool_or(s.value_kwh_d is not null and s.flow_status is distinct from 'Confirmed') as any_provisional,
        -- every record of one operator and gas day comes from the same month answer: one version, one file
        max(s.source_version) as source_version,
        max(s.raw_sha256) as raw_sha256
    from scoped as s
    group by s.operator_key, s.direction_key, s.scope_key, s.gas_day, s.gas_month
),

daily_complete as (
    select *
    from daily
    where points_with_value > 0 and points_with_value = points_expected
),

monthly as (
    select
        operator_key,
        direction_key,
        scope_key,
        min(scope_label) as scope_label,
        gas_month,
        sum(value) as value,
        count(*) as days,
        bool_or(any_provisional) as any_provisional,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from daily_complete
    where gas_month < date_trunc('month', current_date)
    group by all
    having count(*) = day(last_day(gas_month))
),

observations as (
    select
        'physical_flow' as measure_block,
        'kWh/d' as unit_code,
        'kWh/d' as unit_name,
        'D' as frequency,
        gas_day as date_key,
        cast(gas_day as varchar) as period_label,
        * exclude (gas_day, gas_month, points_with_value, points_expected)
    from daily_complete
    union all
    select
        'physical_flow_month_sum' as measure_block,
        'kWh' as unit_code,
        'kWh (sum of the daily kWh/d values of the month)' as unit_name,
        'M' as frequency,
        cast(gas_month as date) as date_key,
        strftime(gas_month, '%Y-%m') as period_label,
        * exclude (gas_month, days)
    from monthly
),

geo_map as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'entsog'
)

select
    'entsog' as source_system,
    'physical_flow' as block_id,
    'ENTSOG TP physical gas flows, GAZ-SYSTEM (daily; monthly sums)' as block_name,
    'entsog:' || o.measure_block || ':' || o.unit_code || ':' || o.direction_key as measure_key,
    'Natural gas physical flow, ' || o.direction_key || ' points, GAZ-SYSTEM transmission system ('
    || case o.measure_block when 'physical_flow' then 'daily' else 'monthly sum of complete days' end
    || ', ENTSOG TP)' as measure_name,
    o.direction_key as measure_codes,
    o.unit_code,
    o.unit_name,
    o.operator_key as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    o.frequency,
    o.date_key,
    o.period_label,
    case when o.scope_key is null then '{}' else cast(to_json(map(['point_group'], [o.scope_key])) as varchar) end
        as breakdown_json,
    case when o.scope_key is null then null else cast(to_json(map(['point_group'], [o.scope_label])) as varchar) end
        as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.scope_key is null as is_total,
    o.value,
    case when o.any_provisional then 'provisional' else 'ok' end as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
left join geo_map as g on o.operator_key = g.source_code
