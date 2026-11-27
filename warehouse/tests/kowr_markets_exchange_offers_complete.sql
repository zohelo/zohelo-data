-- KOWR commodity exchange offers (silver.kowr_markets_exchange_offers): every period has at least one commodity with
-- an average offer or a flag, as Bronze accepts a table only with at least one commodity row; a column the Silver
-- model stops reading leaves a period without prices and fails here. Also returns a row whose average lies outside
-- its minimum and maximum without the flag. (Weeks lost to rejected files are counted by the rejections table, not
-- here: KOWR printed a few tables as images or with missing cells, Dev trial 45914640518.)
select
    period_start,
    'no average offer in the period' as problem
from {{ ref('kowr_markets_exchange_offers') }}
group by period_start
having count(price_avg) = 0 and count(value_flag) = 0
union all
select
    period_start,
    'average outside minimum and maximum: ' || commodity_name as problem
from {{ ref('kowr_markets_exchange_offers') }}
where price_avg < price_min or price_avg > price_max
