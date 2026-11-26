-- uzp_bzp: every month in gold.build_uzp_bzp has every one of its days landed for both notice types
-- (silver.uzp_bzp_days, also days without notices). A day that never landed, e.g. one whose notice/stats check kept
-- failing while later days landed, must keep its month out of Gold instead of showing a month with too few notices.
with build_months as (
    select distinct date_key as month_start
    from {{ ref('poc_build_uzp_bzp') }}
),

landed as (
    select
        cast(date_trunc('month', publication_date) as date) as month_start,
        count(distinct publication_date) filter (where notice_type = 'ContractNotice') as contract_days,
        count(distinct publication_date) filter (where notice_type = 'TenderResultNotice') as result_days
    from {{ ref('uzp_bzp_days') }}
    group by 1
)

select
    b.month_start,
    day(last_day(b.month_start)) as days_in_month,
    coalesce(l.contract_days, 0) as contract_days,
    coalesce(l.result_days, 0) as result_days
from build_months as b
left join landed as l on b.month_start = l.month_start
where
    coalesce(l.contract_days, 0) <> day(last_day(b.month_start))
    or coalesce(l.result_days, 0) <> day(last_day(b.month_start))
