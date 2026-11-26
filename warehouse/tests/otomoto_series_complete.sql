-- otomoto: the newest edition held has the month it is named after for active listings in all four segments, for
-- users in the three segments with a traffic chart and for buyers in two (Dev probe 45814610880, August 2026); every
-- series has every month between its first and last (an edition repeats the current and the previous year, so a
-- missed edition leaves no gap); the traffic indices are 100 in January 2025, their base. A chart that moved, a
-- segment tab renamed or a month lost fails here. One test per Silver model (it reads only this model).
with series as (
    select *
    from {{ ref('otomoto_series') }}
),

newest as (
    select cast(max(edition) || '-01' as date) as month
    from series
),

expected (measure_code, segments) as (
    values ('active_listings', 4), ('users_index', 3), ('buyers_index', 2)
),

found as (
    select
        s.measure_code,
        count(distinct s.segment_label) as segments
    from series as s
    inner join newest as n on s.period_month = n.month
    group by s.measure_code
)

select
    'segments with the newest edition month: ' || e.measure_code as problem,
    coalesce(f.segments, 0) as found
from expected as e
left join found as f on e.measure_code = f.measure_code
where coalesce(f.segments, 0) <> e.segments
union all
select
    'months missing in ' || measure_code || ' ' || segment_label as problem,
    date_diff('month', min(period_month), max(period_month)) + 1 - count(*) as found
from series
group by measure_code, segment_label
having date_diff('month', min(period_month), max(period_month)) + 1 <> count(*)
union all
select
    'index base not 100: ' || measure_code || ' ' || segment_label as problem,
    1 as found
from series
where measure_code in ('users_index', 'buyers_index') and period_month = date '2025-01-01' and value is distinct from 100
