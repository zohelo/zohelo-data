{# Conformed reference period (warehouse/dimensions/period.yaml): SDMX TIME_PERIOD values
   1900–2100 for frequencies A, S, Q, M, W (ISO weeks) and D, with start/end day, plus the
   special members _Z (not applicable) and _T (all periods). period_key = time_period. #}
with years as (select y::integer as y from range(1900, 2101) as t (y)),

annual as (
    select
        cast(y as varchar) as time_period,
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
        y || '-S' || s,
        'S',
        make_date(y, 6 * s - 5, 1),
        (make_date(y, 6 * s - 5, 1) + interval 6 month - interval 1 day)::date,
        y,
        s,
        null,
        null,
        null
    from years, range(1, 3) as t (s)
),

quarterly as (
    select
        y || '-Q' || q,
        'Q',
        make_date(y, 3 * q - 2, 1),
        (make_date(y, 3 * q - 2, 1) + interval 3 month - interval 1 day)::date,
        y,
        (q + 1) // 2,
        q,
        null,
        null
    from years, range(1, 5) as t (q)
),

monthly as (
    select
        y || '-' || lpad(m::varchar, 2, '0'),
        'M',
        make_date(y, m, 1),
        (make_date(y, m, 1) + interval 1 month - interval 1 day)::date,
        y,
        (m + 5) // 6,
        (m + 2) // 3,
        m,
        null
    from years, range(1, 13) as t (m)
),

weekly as (
    select
        isoyear(d) || '-W' || lpad(weekofyear(d)::varchar, 2, '0'),
        'W',
        d::date,
        (d + interval 6 day)::date,
        isoyear(d),
        null,
        null,
        null,
        weekofyear(d)
    from range(date '1900-01-01', date '2101-01-01', interval 7 day) as t (d)   -- 1900-01-01 is a Monday
),

daily as (
    select
        strftime(d, '%Y-%m-%d'),
        'D',
        d::date,
        d::date,
        year(d),
        (month(d) + 5) // 6,
        quarter(d),
        month(d),
        null
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
    '_Z',
    '_Z',
    '_Z',
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    'not_applicable'
union all
select
    '_T',
    '_T',
    '_T',
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    'all_periods'
