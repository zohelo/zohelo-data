-- KOWR commodity exchange offers (silver.kowr_markets_exchange_offers): the periods follow each other without a gap
-- over five weeks. KOWR skipped up to two report weeks in a row (holidays, archive page of 26 Nov 2026, Dev probe
-- 45895465874) and a table may show the week before its report's, so a longer gap means reports were lost (rejected
-- or not read). Also returns a row whose average lies outside its minimum and maximum without a flag.
with periods as (
    select
        period_start,
        lead(period_start) over (order by period_start) as next_start
    from (select distinct period_start from {{ ref('kowr_markets_exchange_offers') }})
)

select
    period_start,
    next_start,
    'gap' as problem
from periods
where next_start - period_start > 35
union all
select
    period_start,
    null as next_start,
    'average outside minimum and maximum: ' || commodity_name as problem
from {{ ref('kowr_markets_exchange_offers') }}
where price_avg < price_min or price_avg > price_max
