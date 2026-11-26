-- KOWR commodity exchange offers (silver.kowr_markets_exchange_offers): KOWR skipped at most two weeks in a row
-- between March 2023 and October 2026 (holidays; gaps of 14 and 21 days on the archive page, Dev probe 45895465874),
-- so a gap over 21 days between report weeks means reports were lost (rejected or not read). Also returns a row whose
-- average lies outside its minimum and maximum.
with weeks as (
    select
        report_week,
        lead(report_week) over (order by report_week) as next_week
    from (select distinct report_week from {{ ref('kowr_markets_exchange_offers') }})
)

select
    report_week,
    next_week,
    'gap' as problem
from weeks
where next_week - report_week > 21
union all
select
    report_week,
    null as next_week,
    'average outside minimum and maximum: ' || commodity_name as problem
from {{ ref('kowr_markets_exchange_offers') }}
where price_avg < price_min or price_avg > price_max
