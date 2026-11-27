-- pse_reports his-wlk-cal: every business day from 2024-06-14 (the API's first day) to the newest day in Silver is
-- there, from one file (entity his_wlk_cal_<YYYYMMDD> of that day), with every 15-minute period of the local
-- day (96; 92 or 100 on DST days) at distinct UTC times. The published local end of each period (dtime without
-- the 'a' of the repeated autumn hour) must equal dtime_local, or dtime_utc plus the offset in force after the
-- period: at a DST switch instant PSE writes either label (2025-03-30 '02:00', 2026-03-29 '03:00' for the same
-- end; Dev probe 45878048791). A Bronze file the model stops reading, a day Landing did not complete, a period lost
-- in a cast or a wrong local time shows up as a failing day here. Reads only its own model (a test reading
-- another table would fail the source's first runs). Returns the failing days.
with periods as (
    select
        *,
        try_cast(replace(dtime, 'a', '') as timestamp) as published_end,
        dtime_utc + to_hours(cast(
            (epoch(cast(timezone('Europe/Warsaw', timezone('UTC', dtime_utc)) as timestamp)) - epoch(dtime_utc))
            / 3600 as integer
        )) as local_end_after
    from {{ ref('pse_reports_his_wlk_cal') }}
),

days as (
    select
        business_date,
        count(*) as periods,
        count(distinct dtime_utc) as distinct_periods,
        min(periods_expected) as expected,
        count(distinct entity) as entities,
        min(entity) as entity,
        count(*) filter (
            where published_end is distinct from dtime_local and published_end is distinct from local_end_after
        ) as local_time_mismatches
    from periods
    group by all
),

calendar as (
    select cast(unnest(generate_series(date '2024-06-14', max(business_date), interval 1 day)) as date) as business_date
    from days
)

select
    c.business_date,
    coalesce(d.periods, 0) as periods_found,
    d.expected as periods_expected,
    d.entities,
    d.entity,
    d.local_time_mismatches
from calendar as c
left join days as d on c.business_date = d.business_date
where
    d.business_date is null
    or d.periods <> d.expected
    or d.distinct_periods <> d.expected
    or d.entities <> 1
    or d.local_time_mismatches > 0
    or d.entity <> 'his_wlk_cal_' || strftime(c.business_date, '%Y%m%d')
