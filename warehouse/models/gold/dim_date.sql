{# Conformed calendar (warehouse/dimensions/calendar.yaml): one row per day,
   1900-01-01 … 2100-12-31, plus the special members 0 (_Z, not applicable) and
   -1 (_U, unknown). Polish statutory holidays are rule-based (Act of 18 January 1951 as
   amended: Epiphany from 2011, Christmas Eve from 2025; movable feasts from Easter).
   The rules are applied from 1990 (May 3 and November 11 restored); earlier years leave
   the holiday and working-day flags empty rather than guess past law. #}
with days as (
    select cast(d as date) as calendar_date
    from range(date '1900-01-01', date '2101-01-01', interval 1 day) as t(d)
),
easter as (
    -- Anonymous Gregorian algorithm (Meeus/Jones/Butcher)
    select y as year,
           make_date(y, cast(floor((h + l - 7 * m + 114) / 31) as integer),
                     cast(((h + l - 7 * m + 114) % 31) + 1 as integer)) as easter_sunday
    from (
        select y, h, l, floor((a + 11 * h + 22 * l) / 451) as m
        from (
            select y, a, h, ((32 + 2 * e + 2 * i - h - k) % 7) as l
            from (
                select y, a, e, i, k,
                       ((19 * a + b - d - g + 15) % 30) as h
                from (
                    select y, y % 19 as a, floor(y / 100) as b, y % 100 as c,
                           floor(floor(y / 100) / 4) as d, floor(y / 100) % 4 as e,
                           floor((floor(y / 100) + 8) / 25) as f,
                           floor((floor(y / 100) - floor((floor(y / 100) + 8) / 25) + 1) / 3) as g,
                           floor((y % 100) / 4) as i, (y % 100) % 4 as k
                    from range(1900, 2101) as t(y)
                )
            )
        )
    )
),
holidays as (
    select easter_sunday as holiday_date, 'Wielkanoc' as holiday_name from easter
    union all select easter_sunday + 1, 'Poniedziałek Wielkanocny' from easter
    union all select easter_sunday + 49, 'Zielone Świątki' from easter
    union all select easter_sunday + 60, 'Boże Ciało' from easter
    union all select make_date(year, 1, 1), 'Nowy Rok' from easter
    union all select make_date(year, 1, 6), 'Święto Trzech Króli' from easter where year >= 2011
    union all select make_date(year, 5, 1), 'Święto Pracy' from easter
    union all select make_date(year, 5, 3), 'Święto Konstytucji 3 Maja' from easter
    union all select make_date(year, 8, 15), 'Wniebowzięcie NMP' from easter
    union all select make_date(year, 11, 1), 'Wszystkich Świętych' from easter
    union all select make_date(year, 11, 11), 'Narodowe Święto Niepodległości' from easter
    union all select make_date(year, 12, 24), 'Wigilia' from easter where year >= 2025
    union all select make_date(year, 12, 25), 'Boże Narodzenie (pierwszy dzień)' from easter
    union all select make_date(year, 12, 26), 'Boże Narodzenie (drugi dzień)' from easter
),
holiday_days as (
    select holiday_date, string_agg(holiday_name, '; ' order by holiday_name) as holiday_name
    from holidays group by holiday_date
)
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
from days d
left join holiday_days h on h.holiday_date = d.calendar_date
union all
select 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
       null, null, null, 'not_applicable'
union all
select -1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
       null, null, null, 'unknown'
