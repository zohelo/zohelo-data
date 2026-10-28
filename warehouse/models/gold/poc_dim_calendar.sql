{{ config(alias='dim_calendar') }}
{# Gold PoC: the conformed calendar at day grain (gold.dim_date plus period labels and
   period-start flags). date_key is the DATE itself; a fact's date is the first day of its
   period, so a monthly report filters is_month_start and gets exactly the monthly dates. #}
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
from {{ ref('dim_date') }}
where member_type = 'day'
