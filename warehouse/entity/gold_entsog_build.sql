-- Gold build stage of the ENTSOG Transparency Platform physical gas flows, by rule (docs/gold-coverage.md): every daily
-- value of silver.entsog_physical_flow, per point and direction, in the common observation shape (the 21 columns of
-- poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one block. Relations:
--   silver      the data files of silver.entsog_physical_flow (operator × point × direction × gas day)
--   geo_map     gold.map_geography rows of ENTSOG (operator key -> area; PL-TSO-0002 = Poland)
--   measure_key = entsog:entsog_physical_flow:kWh/d:<entry|exit>@D: the indicator (physical flow) and the direction
--                 are the measure; the point is the breakdown. Points are never summed here: the curated stage
--                 builds the totals and the point groups, with its completeness rule.
--   value       = kWh/d as published (D33; nothing converted)
--   breakdown   = {"point": <pointKey>}, label the point's name in the newest record of the point and direction
--                 (one name per series, the same on every run); no total series (is_total false)
--   date_key    = the gas day (the date of periodFrom in WET, Silver's gas_day); period_label 2026-10-31
--   geography   = the operator's area through the map (PL); an operator the map does not know stays _X with its key
--   status      = Confirmed `ok`, Provisional `provisional`
-- Not loaded: a record without a value (isNA or empty; Silver keeps it with value null, nothing is 0-filled), a value
-- that is not a finite number, and a running value: a record last updated before its gas day ended is the operator's
-- intra-day figure, not the day's flow. It counts as final once the day ended more than 7 days before the newest
-- update in the table (the curated stage's rule, anchored on the data instead of the clock so that a rebuild of the
-- same files gives the same rows; Tieterowka 2023-06-21 and Wysokoje 2023-07-05 are the two such records).
-- Two records of one point, direction and gas day (two periodFrom on one date) would repeat series_key × date_key
-- and reject the block (tests/entsog_gas_days.sql keeps one start per day); nothing is picked.
with flows as (
    select
        s.*,
        max(s.last_update_utc) over () as newest_update,
        arg_max(s.point_label, s.period_from_utc) over (partition by s.point_key, s.direction_key) as newest_point_label
    from silver as s
    where s.indicator = 'Physical Flow' and s.period_type = 'day'
),

final as (
    select *
    from flows
    where
        value_kwh_d is not null
        and not isnan(value_kwh_d)
        and not isinf(value_kwh_d)
        and gas_day is not null
        and (last_update_utc >= period_to_utc or period_to_utc < newest_update - interval 7 day)
)

select
    'entsog' as source_system,
    'entsog_physical_flow' as block_id,
    'ENTSOG TP physical gas flows, every point and direction (daily)' as block_name,
    'entsog:entsog_physical_flow:kWh/d:' || f.direction_key || '@D' as measure_key,
    'Physical gas flow, ' || f.direction_key || ', per point (kWh/d)' as measure_name,
    f.indicator || ':' || f.direction_key as measure_codes,
    'kWh/d' as unit_code,
    'kWh/d' as unit_name,
    f.operator_key as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    f.gas_day as date_key,
    cast(f.gas_day as varchar) as period_label,
    cast(to_json(map(['point'], [f.point_key])) as varchar) as breakdown_json,
    cast(to_json(map(['point'], [coalesce(f.newest_point_label, f.point_key)])) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    false as is_total,
    cast(f.value_kwh_d as double) as value,
    case f.flow_status when 'Confirmed' then 'ok' else 'provisional' end as observation_status_key,
    f.source_version,
    f.raw_sha256
from final as f
left join geo_map as g on f.operator_key = g.source_code
