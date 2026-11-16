-- ulc_airports_traffic: every quarter from the first to the last has the total row ('_T') of both measures, and
-- the airports of a quarter add up to its total (each table is checked against its "Suma" row; all rows of a
-- quarter come from the same newest file). A table that stops being read, an airport lost or read twice, or a value
-- read from the wrong column fails here instead of thinning or bending the series. One test per Silver model.
with t as (select * from {{ ref('ulc_airports_traffic') }}),

quarters as (
    select unnest(generate_series(min(period_date), max(period_date), interval 3 month)) as period_date
    from t
),

grid as (
    select
        q.period_date,
        m.measure_code
    from quarters as q
    cross join (values ('passengers'), ('passenger_operations')) as m (measure_code)
),

sums as (
    select
        period_date,
        measure_code,
        sum(value) filter (where airport_code <> '_T') as airports,
        max(value) filter (where airport_code = '_T') as total
    from t
    group by all
)

select
    g.period_date,
    g.measure_code,
    s.airports,
    s.total
from grid as g
left join sums as s on g.period_date = s.period_date and g.measure_code = s.measure_code
where s.total is null or s.airports is distinct from s.total
