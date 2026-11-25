-- entsog (silver.entsog_physical_flow): no gas day is missing inside a series. Every gas day between an operator's
-- first and last day with a value has records, and every point-direction has a record on each day between its own
-- first and last record (points start and stop mid-month: Vyrava 2022-11-12, Zlate Hory 2023-09-13, Tieterowka
-- 2023-06-21, Wysokoje 2023-07-05; Dev trial 43323294484). A month answer that loses days or points fails here
-- instead of thinning the totals; Gold's daily totals rely on it (a point without a record is not expected).
with valued as (
    select
        operator_key,
        min(gas_day) as first_day,
        max(gas_day) as last_day
    from {{ ref('entsog_physical_flow') }}
    where value_kwh_d is not null
    group by operator_key
),

operator_days as (
    select
        v.operator_key,
        cast(c.gas_day as date) as gas_day
    from valued as v
    cross join lateral (select unnest(generate_series(v.first_day, v.last_day, interval 1 day)) as gas_day) as c
),

series as (
    select
        operator_key,
        point_key,
        direction_key,
        min(gas_day) as first_day,
        max(gas_day) as last_day,
        count(*) as days
    from {{ ref('entsog_physical_flow') }}
    group by operator_key, point_key, direction_key
)

select
    d.operator_key,
    d.gas_day,
    'no record on this day' as problem
from operator_days as d
where not exists (
    select 1
    from {{ ref('entsog_physical_flow') }} as f
    where f.operator_key = d.operator_key and f.gas_day = d.gas_day
)
union all
select
    operator_key,
    first_day as gas_day,
    'point ' || point_key || ' ' || direction_key || ': ' || days || ' records from ' || first_day || ' to '
    || last_day as problem
from series
where days <> last_day - first_day + 1
