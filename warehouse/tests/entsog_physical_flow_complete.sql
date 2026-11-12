-- entsog (silver.entsog_physical_flow): every gas day between an operator's first and last day is present, and
-- every point-direction published in a gas month has a record on each day of that month the operator published
-- (with a value or isNA). A month answer that loses days or points fails here instead of thinning the totals.
with days as (
    select
        operator_key,
        gas_day,
        date_trunc('month', gas_day) as gas_month
    from {{ ref('entsog_physical_flow') }}
    group by all
),

gaps as (
    select
        d.operator_key,
        cast(c.gas_day as date) as gas_day,
        'missing day' as problem
    from (
        select
            operator_key,
            min(gas_day) as first_day,
            max(gas_day) as last_day
        from days
        group by operator_key
    ) as d
    cross join lateral (select unnest(generate_series(d.first_day, d.last_day, interval 1 day)) as gas_day) as c
    where not exists (
        select 1 from days as x where x.operator_key = d.operator_key and x.gas_day = cast(c.gas_day as date)
    )
),

series_days as (
    select
        operator_key,
        point_key,
        direction_key,
        date_trunc('month', gas_day) as gas_month,
        count(*) as n
    from {{ ref('entsog_physical_flow') }}
    group by all
),

month_days as (
    select
        operator_key,
        gas_month,
        count(*) as n
    from days
    group by all
)

select
    operator_key,
    gas_day,
    problem
from gaps
union all
select
    s.operator_key,
    cast(s.gas_month as date) as gas_day,
    'point ' || s.point_key || ' ' || s.direction_key || ': ' || s.n || ' of ' || m.n || ' days' as problem
from series_days as s
inner join month_days as m on s.operator_key = m.operator_key and s.gas_month = m.gas_month
where s.n <> m.n
