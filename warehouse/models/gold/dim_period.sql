{# Conformed reference period (warehouse/dimensions/period.yaml): SDMX TIME_PERIOD values
   1900–2100 for frequencies A, S, Q, M, W (ISO weeks) and D, with start/end day, plus the
   special members _Z (not applicable) and _T (all periods). period_key = time_period. #}
with years as (select y::integer as y from range(1900, 2101) as t (y)),

annual as (
    select
        y::varchar as time_period,
        'A' as frequency_code,
        make_date(y, 1, 1) as start_date,
        make_date(y, 12, 31) as end_date,
        y as year,
        null::integer as half,
        null::integer as quarter,
        null::integer as month,
        null::integer as week
    from years
),

semester as (
    select
        years.y || '-S' || t.s as time_period,
        'S' as frequency_code,
        make_date(years.y, 6 * t.s - 5, 1) as start_date,
        (make_date(years.y, 6 * t.s - 5, 1) + interval 6 month - interval 1 day)::date as end_date,
        years.y as year,
        t.s as half,
        null as quarter,
        null as month,
        null as week
    from years, range(1, 3) as t (s)
),

quarterly as (
    select
        years.y || '-Q' || t.q as time_period,
        'Q' as frequency_code,
        make_date(years.y, 3 * t.q - 2, 1) as start_date,
        (make_date(years.y, 3 * t.q - 2, 1) + interval 3 month - interval 1 day)::date as end_date,
        years.y as year,
        (t.q + 1) // 2 as half,
        t.q as quarter,
        null as month,
        null as week
    from years, range(1, 5) as t (q)
),

monthly as (
    select
        years.y || '-' || lpad(t.m::varchar, 2, '0') as time_period,
        'M' as frequency_code,
        make_date(years.y, t.m, 1) as start_date,
        (make_date(years.y, t.m, 1) + interval 1 month - interval 1 day)::date as end_date,
        years.y as year,
        (t.m + 5) // 6 as half,
        (t.m + 2) // 3 as quarter,
        t.m as month,
        null as week
    from years, range(1, 13) as t (m)
),

weekly as (
    select
        isoyear(d) || '-W' || lpad(weekofyear(d)::varchar, 2, '0') as time_period,
        'W' as frequency_code,
        d::date as start_date,
        (d + interval 6 day)::date as end_date,
        isoyear(d) as year,
        null as half,
        null as quarter,
        null as month,
        weekofyear(d) as week
    from range(date '1900-01-01', date '2101-01-01', interval 7 day) as t (d)   -- 1900-01-01 is a Monday
),

daily as (
    select
        strftime(d, '%Y-%m-%d') as time_period,
        'D' as frequency_code,
        d::date as start_date,
        d::date as end_date,
        year(d) as year,
        (month(d) + 5) // 6 as half,
        quarter(d) as quarter,
        month(d) as month,
        null as week
    from range(date '1900-01-01', date '2101-01-01', interval 1 day) as t (d)
),

periods as (
    select * from annual
    union all
    select * from semester
    union all
    select * from quarterly
    union all
    select * from monthly
    union all
    select * from weekly
    union all
    select * from daily
)

select
    time_period as period_key,
    time_period,
    frequency_code,
    start_date,
    end_date,
    datediff('day', start_date, end_date) + 1 as days,
    year,
    half,
    quarter,
    month,
    week,
    'period' as member_type
from periods
union all
select
    '_Z' as period_key,
    '_Z' as time_period,
    '_Z' as frequency_code,
    null as start_date,
    null as end_date,
    null as days,
    null as year,
    null as half,
    null as quarter,
    null as month,
    null as week,
    'not_applicable' as member_type
union all
select
    '_T' as period_key,
    '_T' as time_period,
    '_T' as frequency_code,
    null as start_date,
    null as end_date,
    null as days,
    null as year,
    null as half,
    null as quarter,
    null as month,
    null as week,
    'all_periods' as member_type
