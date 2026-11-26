-- pse_reports his-wlk-cal: every business day from 2024-06-14 (the API's first day) to the newest day in Silver is
-- there, from one file (entity his_wlk_cal_<YYYYMMDD> of that day), with every 15-minute period of the local day (96;
-- 92 or 100 on DST days) at distinct UTC times. A Bronze file the model stops reading, a day Landing did not complete
-- or a period lost in a cast shows up as a missing or short day here. Reads only its own model (a test reading another
-- table would fail the source's first runs). Returns the failing days.
with days as (
    select
        business_date,
        count(*) as periods,
        count(distinct dtime_utc) as distinct_periods,
        min(periods_expected) as expected,
        count(distinct entity) as entities,
        min(entity) as entity
    from {{ ref('pse_reports_his_wlk_cal') }}
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
    d.entity
from calendar as c
left join days as d on c.business_date = d.business_date
where
    d.business_date is null
    or d.periods <> d.expected
    or d.distinct_periods <> d.expected
    or d.entities <> 1
    or d.entity <> 'his_wlk_cal_' || strftime(c.business_date, '%Y%m%d')
