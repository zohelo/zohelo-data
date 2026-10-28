{{ config(alias='dim_calendar') }}
{# Gold PoC: the conformed calendar at day grain (gold.dim_date plus period labels and
   period-start flags). date_key is the DATE itself; a fact's date is the first day of its
   period, so a monthly report filters is_month_start and gets exactly the monthly dates. #}
with days as (
    select cast(d as date) as calendar_date
    from range(date '1900-01-01', date '2101-01-01', interval 1 day) as t (d)
),

easter as (
    -- Anonymous Gregorian algorithm (Meeus/Jones/Butcher)
    select
        y as year,
        make_date(
            y, cast(floor((h + l - 7 * m + 114) / 31) as integer),
            cast(((h + l - 7 * m + 114) % 31) + 1 as integer)
        ) as easter_sunday
    from (
        select
            y,
            h,
            l,
            floor((a + 11 * h + 22 * l) / 451) as m
        from (
            select
                y,
                a,
                h,
                ((32 + 2 * e + 2 * i - h - k) % 7) as l
            from (
                select
                    y,
                    a,
                    e,
                    i,
                    k,
                    ((19 * a + b - d - g + 15) % 30) as h
                from (
                    select
                        y,
                        y % 19 as a,
                        floor(y / 100) as b,
                        y % 100 as c,
                        floor(floor(y / 100) / 4) as d,
                        floor(y / 100) % 4 as e,
                        floor((floor(y / 100) + 8) / 25) as f,
                        floor((floor(y / 100) - floor((floor(y / 100) + 8) / 25) + 1) / 3) as g,
                        floor((y % 100) / 4) as i,
                        (y % 100) % 4 as k
                    from range(1900, 2101) as t (y)
                ) as s4
            ) as s3
        ) as s2
    ) as s1
),

holidays as (
    select
        easter_sunday as holiday_date,
        'Wielkanoc' as holiday_name
    from easter
    union all
    select
        easter_sunday + 1 as holiday_date,
        'Poniedziałek Wielkanocny' as holiday_name
    from easter
    union all
    select
        easter_sunday + 49 as holiday_date,
        'Zielone Świątki' as holiday_name
    from easter
    union all
    select
        easter_sunday + 60 as holiday_date,
        'Boże Ciało' as holiday_name
    from easter
    union all
    select
        make_date(year, 1, 1) as holiday_date,
        'Nowy Rok' as holiday_name
    from easter
    union all
    select
        make_date(year, 1, 6) as holiday_date,
        'Święto Trzech Króli' as holiday_name
    from easter
    where year >= 2011
    union all
    select
        make_date(year, 5, 1) as holiday_date,
        'Święto Pracy' as holiday_name
    from easter
    union all
    select
        make_date(year, 5, 3) as holiday_date,
        'Święto Konstytucji 3 Maja' as holiday_name
    from easter
    union all
    select
        make_date(year, 8, 15) as holiday_date,
        'Wniebowzięcie NMP' as holiday_name
    from easter
    union all
    select
        make_date(year, 11, 1) as holiday_date,
        'Wszystkich Świętych' as holiday_name
    from easter
    union all
    select
        make_date(year, 11, 11) as holiday_date,
        'Narodowe Święto Niepodległości' as holiday_name
    from easter
    union all
    select
        make_date(year, 12, 24) as holiday_date,
        'Wigilia' as holiday_name
    from easter
    where year >= 2025
    union all
    select
        make_date(year, 12, 25) as holiday_date,
        'Boże Narodzenie (pierwszy dzień)' as holiday_name
    from easter
    union all
    select
        make_date(year, 12, 26) as holiday_date,
        'Boże Narodzenie (drugi dzień)' as holiday_name
    from easter
),

holiday_days as (
    select
        holiday_date,
        string_agg(holiday_name, '; ' order by holiday_name) as holiday_name
    from holidays
    group by holiday_date
),

dim_date as (
    select
        cast(strftime(d.calendar_date, '%Y%m%d') as integer) as date_key,
        d.calendar_date,
        year(d.calendar_date) as year,
        quarter(d.calendar_date) as quarter,
        month(d.calendar_date) as month,
        day(d.calendar_date) as day_of_month,
        isodow(d.calendar_date) as day_of_week_iso,
        dayname(d.calendar_date) as day_name,
        monthname(d.calendar_date) as month_name,
        isoyear(d.calendar_date) as iso_year,
        weekofyear(d.calendar_date) as iso_week,
        dayofyear(d.calendar_date) as day_of_year,
        isodow(d.calendar_date) >= 6 as is_weekend,
        case when year(d.calendar_date) >= 1990 then h.holiday_date is not null end as is_pl_public_holiday,
        case when year(d.calendar_date) >= 1990 then h.holiday_name end as pl_holiday_name,
        case when year(d.calendar_date) >= 1990 then isodow(d.calendar_date) < 6 and h.holiday_date is null end
            as is_pl_working_day,
        strftime(d.calendar_date, '%Y-%m') as year_month,
        year(d.calendar_date) || '-Q' || quarter(d.calendar_date) as year_quarter,
        case when month(d.calendar_date) >= 9 then year(d.calendar_date) else year(d.calendar_date) - 1 end
            as pl_school_year_start,
        'day' as member_type
    from days as d
    left join holiday_days as h on d.calendar_date = h.holiday_date
    union all
    select
        0 as date_key,
        null as calendar_date,
        null as year,
        null as quarter,
        null as month,
        null as day_of_month,
        null as day_of_week_iso,
        null as day_name,
        null as month_name,
        null as iso_year,
        null as iso_week,
        null as day_of_year,
        null as is_weekend,
        null as is_pl_public_holiday,
        null as pl_holiday_name,
        null as is_pl_working_day,
        null as year_month,
        null as year_quarter,
        null as pl_school_year_start,
        'not_applicable' as member_type
    union all
    select
        -1 as date_key,
        null as calendar_date,
        null as year,
        null as quarter,
        null as month,
        null as day_of_month,
        null as day_of_week_iso,
        null as day_name,
        null as month_name,
        null as iso_year,
        null as iso_week,
        null as day_of_year,
        null as is_weekend,
        null as is_pl_public_holiday,
        null as pl_holiday_name,
        null as is_pl_working_day,
        null as year_month,
        null as year_quarter,
        null as pl_school_year_start,
        'unknown' as member_type
)

select
    calendar_date as date_key,
    calendar_date,
    year,
    case when month <= 6 then 1 else 2 end as half,
    quarter,
    month,
    day_of_month as day,
    day_of_week_iso as day_of_week,
    day_name,
    month_name,
    iso_year,
    iso_week,
    day_of_year,
    is_weekend,
    is_pl_public_holiday,
    pl_holiday_name,
    is_pl_working_day,
    cast(year as varchar) as year_label,
    year || '-S' || (case when month <= 6 then 1 else 2 end) as half_label,
    year_quarter as quarter_label,
    year_month as month_label,
    iso_year || '-W' || lpad(cast(iso_week as varchar), 2, '0') as week_label,
    make_date(year, 1, 1) as year_start,
    make_date(year, case when month <= 6 then 1 else 7 end, 1) as half_start,
    make_date(year, (quarter - 1) * 3 + 1, 1) as quarter_start,
    make_date(year, month, 1) as month_start,
    cast(date_trunc('week', calendar_date) as date) as week_start,
    day_of_month = 1 and month = 1 as is_year_start,
    day_of_month = 1 and month in (1, 7) as is_half_start,
    day_of_month = 1 and month in (1, 4, 7, 10) as is_quarter_start,
    day_of_month = 1 as is_month_start,
    day_of_week_iso = 1 as is_week_start
from dim_date
where member_type = 'day'
